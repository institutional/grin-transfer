"""
Async GRIN API client using aiohttp
"""

import asyncio
import logging
import time
from collections.abc import AsyncGenerator, Callable
from datetime import datetime
from typing import Any

from grin_to_s3.auth import GRINAuth
from grin_to_s3.common import create_http_session

logger = logging.getLogger(__name__)

ALL_BOOKS_ENDPOINT = "_all_books"

class GRINClient:
    """Async client for GRIN API operations."""

    def __init__(
        self,
        base_url: str = "https://books.google.com/libraries/",
        auth: GRINAuth | None = None,
        secrets_dir: str | None = None,
        timeout: int = 60,
    ):
        self.base_url = base_url.rstrip("/")
        self.auth = auth or GRINAuth(secrets_dir=secrets_dir)
        self.timeout = timeout

    async def get_bearer_token(self) -> str:
        """Get current bearer token for manual use."""
        return await self.auth.get_bearer_token()

    async def fetch_resource(self, directory: str, resource: str = "?format=text", method: str = "GET") -> str:
        """
        Fetch a resource from GRIN directory.

        Args:
            directory: GRIN directory name (e.g., 'Harvard')
            resource: Resource path (e.g., '_all_books?format=text')
            method: HTTP method

        Returns:
            str: Response text
        """
        url = f"{self.base_url}/{directory}/{resource}"

        async with create_http_session(self.timeout) as session:
            response = await self.auth.make_authenticated_request(session, url, method=method)
            return await response.text()



    async def stream_book_list_html_prefetch(
        self,
        directory: str,
        list_type: str = ALL_BOOKS_ENDPOINT,
        page_size: int = 5000,
        max_pages: int = 1000,
        start_page: int = 1,
        start_url: str | None = None,
        pagination_callback: Callable | None = None,
        sqlite_tracker: Any = None,
    ) -> AsyncGenerator[tuple[str, set[str]], None]:
        """
        Stream book list from GRIN with prefetching and SQLite batch optimization.

        Prefetches the next page while processing the current page's data.
        Returns tuples of (book_line, known_barcodes_set) for batch SQLite optimization.
        """
        page_count = start_page - 1
        current_url: str | None = start_url or f"{self.base_url}/{directory}/{list_type}?result_count={page_size}"
        prefetch_task = None

        while page_count < max_pages and current_url:
            page_count += 1
            logger.debug(f"Fetching page {page_count} (page_size={page_size}) with prefetch")

            # Wait for current page (or prefetch result)
            if prefetch_task:
                wait_start = time.time()
                wait_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.debug(f"Page {page_count}: Waiting for prefetched data at {wait_time}...")
                html, response_url = await prefetch_task
                wait_elapsed = time.time() - wait_start
                use_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.debug(
                    f"Page {page_count}: Using prefetched data after {wait_elapsed:.2f}s wait at {use_time}"
                )

            else:
                # First page - fetch normally
                logger.debug(f"Page {page_count}: Normal fetch (no prefetch available)")
                async with create_http_session(300) as session:
                    response = await self.auth.make_authenticated_request(session, current_url)
                    html = await response.text()

            if "Your request is unavailable" in html:
                logger.warning(f"Page {page_count}: Request unavailable, stopping")
                break

            # Parse books and extract next URL with timing
            parse_start = time.time()
            logger.debug(f"Page {page_count}: Starting HTML parsing at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            books = self._parse_books_from_html(html)
            parse_elapsed = time.time() - parse_start
            book_count = len(books)
            logger.debug(f"Page {page_count}: HTML parsing completed in {parse_elapsed:.2f}s, found {book_count} books")

            # Log slow parsing
            if parse_elapsed > 10.0:
                logger.warning(f"Slow HTML parsing: {parse_elapsed:.1f}s on page {page_count} ({book_count} books)")

            next_url = self._extract_next_button_url(html, directory)

            if book_count == 0:
                logger.debug("No books found on page - stopping")
                break

            # Extract all barcodes from this page for batch SQLite query
            page_barcodes = set()
            for book_line in books:
                barcode = book_line.split("\t")[0] if book_line else ""
                if barcode:
                    page_barcodes.add(barcode)

            # Batch query SQLite for all barcodes on this page
            known_barcodes_on_page: set[str] = set()
            if sqlite_tracker and page_barcodes:
                known_barcodes_on_page = await sqlite_tracker.load_known_barcodes_batch(page_barcodes)
                logger.debug(
                    f"Page {page_count}: Batch SQLite query - "
                    f"{len(known_barcodes_on_page)}/{len(page_barcodes)} barcodes already known"
                )

            # Start prefetching next page in background
            if next_url and page_count < max_pages:
                prefetch_task = asyncio.create_task(self._prefetch_page(next_url))
                logger.debug(f"Started prefetch for page {page_count + 1}")
            else:
                prefetch_task = None

            # Clear current HTML to save memory
            del html

            # Yield books with their known barcode set for efficient checking
            yield_start_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            logger.debug(f"Page {page_count}: Starting to yield {len(books)} books at {yield_start_time}")
            for i, book_line in enumerate(books):
                yield book_line, known_barcodes_on_page
                # Add small yield every 1000 books to let prefetch progress
                if i % 1000 == 0:
                    await asyncio.sleep(0.001)  # 1ms yield to event loop
                    yield_progress_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.debug(f"Page {page_count}: Yielded {i + 1}/({len(books)}) books at {yield_progress_time}")

            yield_end_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            logger.debug(f"Page {page_count}: Finished yielding all {len(books)} books at {yield_end_time}")

            # Clean up books list
            del books

            # Save pagination state if callback provided
            if pagination_callback:
                callback_start = time.time()
                pagination_state = {"current_page": page_count + 1, "next_url": next_url, "page_size": page_size}
                save_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.debug(f"Page {page_count}: Saving pagination state at {save_time}")
                await pagination_callback(pagination_state)
                callback_elapsed = time.time() - callback_start
                logger.debug(f"Page {page_count}: Pagination callback completed in {callback_elapsed:.2f}s")

                # Log slow pagination saves
                if callback_elapsed > 1.0:
                    logger.warning(f"Slow pagination save: {callback_elapsed:.2f}s on page {page_count}")

            # Move to next page
            current_url = next_url
            if not current_url:
                break
            logger.debug(f"Page {page_count}: Moving to next page at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")

            # Force garbage collection periodically
            if page_count % 50 == 0:
                gc_start = time.time()
                import gc

                gc.collect()
                gc_elapsed = time.time() - gc_start
                logger.debug(f"Garbage collection took {gc_elapsed:.2f}s on page {page_count}")
                if gc_elapsed > 5.0:
                    logger.warning(f"Long garbage collection: {gc_elapsed:.2f}s on page {page_count}")

        # Clean up any remaining prefetch task
        if prefetch_task and not prefetch_task.done():
            prefetch_task.cancel()

    async def _prefetch_page(self, url: str) -> tuple[str, str]:
        """
        Prefetch a page's HTML content in the background.

        Returns:
            tuple: (html_content, url)
        """
        start_time = time.time()
        logger.debug(f"Prefetch started at {datetime.now().strftime('%H:%M:%S.%f')[:-3]} for URL: {url}")

        # Time the authentication/request phase
        auth_start = time.time()
        async with create_http_session(300) as session:
            auth_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            logger.debug(f"Session created, making authenticated request at {auth_time}")
            response = await self.auth.make_authenticated_request(session, url)
            auth_elapsed = time.time() - auth_start
            auth_complete_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            logger.debug(f"Authenticated request completed in {auth_elapsed:.2f}s at {auth_complete_time}")

            # Time the HTML download phase
            download_start = time.time()
            html = await response.text()
            download_elapsed = time.time() - download_start
            logger.debug(f"HTML download completed in {download_elapsed:.2f}s")

        total_elapsed = time.time() - start_time
        logger.debug(
            f"Prefetch completed in {total_elapsed:.2f}s total "
            f"(auth: {auth_elapsed:.2f}s, download: {download_elapsed:.2f}s)"
        )
        return html, url

    def _parse_books_from_html(self, html_content: str) -> list[str]:
        """
        Parse book data from GRIN HTML using fast selectolax parser.
        """
        from selectolax.lexbor import LexborHTMLParser

        try:
            # Parse with selectolax using the fastest parser
            tree = LexborHTMLParser(html_content)
            books: list[str] = []

            # Find all table rows with barcode inputs
            rows = tree.css("tr")

            # Log table headers if present for debugging
            header_row = tree.css_first("thead tr") or tree.css_first("tr")
            if header_row and not header_row.css_first('input[name="barcodes"]'):
                headers = []
                for th in header_row.css("th, td"):
                    header_text = th.text(strip=True) if th.text() else ""
                    headers.append(header_text)
                if headers:
                    logger.debug(f"HTML table headers found: {headers}")

            for row in rows:
                # Look for checkbox input with barcode (for _all_books format)
                barcode_input = row.css_first('input[name="barcodes"]')
                if barcode_input:
                    barcode = barcode_input.attributes.get("value")
                    if barcode:
                        # Extract all cell text from this row
                        cells = row.css("td")
                        cell_texts = []

                        for cell in cells:
                            # Check if cell contains a link and extract the URL
                            link = cell.css_first("a[href]")
                            if link and link.attributes.get("href"):
                                # Use the href attribute as the cell content
                                text = link.attributes["href"]
                            else:
                                # Get clean text content
                                text = cell.text(strip=True) if cell.text() else ""
                                # Clean up extra whitespace
                                text = " ".join(text.split())
                            cell_texts.append(text)

                        # Log the complete cell structure for first few books for debugging
                        if len(books) < 3:
                            logger.debug(f"Book {barcode} has {len(cell_texts)} cells: {cell_texts}")

                        # Create tab-separated line: barcode + cells (skip first checkbox cell)
                        if len(cell_texts) > 1:
                            # Ensure all cell values are strings (convert None to empty string)
                            cell_strings = [cell or "" for cell in cell_texts[1:]]
                            book_line = barcode + "\t" + "\t".join(cell_strings)
                            books.append(book_line)
                else:
                    # Handle _converted format (no checkboxes, filename in first cell)
                    cells = row.css("td")
                    if len(cells) >= 5:  # Data rows have multiple cells
                        filename = cells[0].text(strip=True) if cells[0].text() else ""
                        if filename.endswith(".tar.gz.gpg"):
                            # Extract barcode from filename
                            barcode = filename.replace(".tar.gz.gpg", "")
                            # Extract all cell text from this row
                            cell_texts = []
                            for cell in cells:
                                # Check if cell contains a link and extract the URL
                                link = cell.css_first("a[href]")
                                if link and link.attributes.get("href"):
                                    text = link.attributes["href"]
                                else:
                                    # Get clean text content
                                    text = cell.text(strip=True) if cell.text() else ""
                                    # Clean up extra whitespace
                                    text = " ".join(text.split())
                                cell_texts.append(text)

                            # Log the complete cell structure for first few books for debugging
                            if len(books) < 3:
                                logger.debug(f"Book {barcode} has {len(cell_texts)} cells: {cell_texts}")

                            # Create tab-separated line: barcode + cells (skip filename cell)
                            if len(cell_texts) > 1:
                                # Ensure all cell values are strings (convert None to empty string)
                                cell_strings = [cell or "" for cell in cell_texts[1:]]
                                book_line = barcode + "\t" + "\t".join(cell_strings)
                                books.append(book_line)

            return books

        except Exception as e:
            logger.error(f"selectolax parsing failed: {e}")
            raise

    def _extract_next_button_url(self, html_content: str, directory: str) -> str | None:
        """
        Extract Next button URL from GRIN HTML response using selectolax.
        """
        from selectolax.lexbor import LexborHTMLParser

        try:
            tree = LexborHTMLParser(html_content)

            # Look for Next button link
            # Try different patterns for Next button
            next_links = tree.css('a[href*="first="], a[href*="ctoken="]')

            for link in next_links:
                href = link.attributes.get("href", "")
                link_text = link.text(strip=True).lower() if link.text() else ""

                # Check if this is a Next button by text content
                if "next" in link_text or ">" in link_text or "&gt;" in (link.html or ""):
                    if href:
                        # Convert relative path to full URL
                        if href.startswith("/"):
                            return f"https://books.google.com{href}"
                        elif href.startswith("http"):
                            return href
                        else:
                            return f"{self.base_url}/{directory}/{href}"

            return None

        except Exception as e:
            logger.warning(f"selectolax next button parsing failed: {e}")
            return None


