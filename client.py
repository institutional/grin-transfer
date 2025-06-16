"""
Async GRIN API client using aiohttp
"""

import asyncio
import logging
import time
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp

from auth import GRINAuth, GRINPermissionError
from common import create_http_session

logger = logging.getLogger(__name__)


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

    async def get_book_list(
        self, directory: str, list_type: str = "_all_books", result_count: int | None = None, mode_all: bool = False
    ) -> list[str]:
        """
        Get list of book barcodes from GRIN.

        Args:
            directory: GRIN directory name
            list_type: Type of list (_all_books, _converted, _failed)
            result_count: Limit number of results
            mode_all: Include all books regardless of status

        Returns:
            list[str]: Book barcodes
        """
        # Build query parameters
        params = ["format=text"]
        if mode_all:
            params.append("mode=all")
        if result_count and result_count > 0:
            params.append(f"result_count={result_count}")

        resource = f"{list_type}?{'&'.join(params)}"

        # Get the response text
        text = await self.fetch_resource(directory, resource)

        # Parse barcodes from response
        return [line.strip() for line in text.strip().split("\n") if line.strip()]

    async def stream_book_list_html(
        self,
        directory: str,
        list_type: str = "_all_books",
        page_size: int = 1000,
        max_pages: int = 100,
        start_page: int = 1,
        start_url: str | None = None,
        pagination_callback: Callable | None = None,
    ) -> AsyncIterator[str]:
        """
        Stream book barcodes using HTML view with Next button extraction.

        Uses large page sizes and follows actual Next button URLs.

        Args:
            directory: GRIN directory name
            list_type: Type of list (_all_books, _converted, _failed)
            page_size: Number of books per page request (use large values like 1000)
            max_pages: Maximum number of pages to fetch
            start_page: Page number to start from (for resume functionality)
            start_url: URL to start from (for resume functionality)
            pagination_callback: Callback function to save pagination state

        Yields:
            str: Individual book barcode lines (tab-separated with full metadata)
        """
        page_count = start_page - 1  # Adjust for starting page
        current_url: str | None = start_url or f"{self.base_url}/{directory}/{list_type}?result_count={page_size}"

        while page_count < max_pages and current_url:
            page_count += 1
            print(f"Fetching page {page_count} (page_size={page_size})")
            print(f"URL: {current_url}")

            async with create_http_session(300) as session:
                try:
                    response = await self.auth.make_authenticated_request(session, current_url)
                    html = await response.text()

                    if "Your request is unavailable" in html:
                        print(f"Page {page_count}: Request unavailable, stopping")
                        break

                    # Parse books using robust HTML parsing
                    books = self._parse_books_from_html_robust(html)
                    book_count = len(books)

                    print(f"Page {page_count}: Found {book_count} books")

                    if book_count == 0:
                        print("No books found on page - stopping")
                        break

                    # Yield all books from this page
                    for book_line in books:
                        yield book_line

                    # Extract Next button URL from HTML before clearing
                    next_url = self._extract_next_button_url(html, directory)

                    # Clear HTML content and parser state to prevent memory accumulation
                    del html, books

                    # Force garbage collection every 50 pages (approximately every 250K records)
                    if page_count % 50 == 0:
                        import gc

                        gc.collect()
                    if next_url:
                        current_url = next_url
                        print(f"Found Next button URL for page {page_count + 1}")

                        # Save pagination state if callback provided
                        if pagination_callback:
                            pagination_state = {
                                "current_page": page_count + 1,
                                "next_url": next_url,
                                "page_size": page_size,
                            }
                            await pagination_callback(pagination_state)
                    else:
                        print("No Next button found - end of collection")
                        break

                except Exception as e:
                    print(f"Error on page {page_count}: {e}")
                    break

    async def stream_book_list_html_prefetch(
        self,
        directory: str,
        list_type: str = "_all_books",
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
                try:
                    wait_start = time.time()
                    wait_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.debug(f"Page {page_count}: Waiting for prefetched data at {wait_time}...")
                    html, response_url = await prefetch_task
                    wait_elapsed = time.time() - wait_start
                    use_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.debug(
                        f"Page {page_count}: Using prefetched data after {wait_elapsed:.2f}s wait at {use_time}"
                    )
                except Exception as e:
                    wait_elapsed = time.time() - wait_start
                    logger.warning(f"Page {page_count}: Prefetch failed after {wait_elapsed:.2f}s: {e}")
                    # Fall back to normal fetch
                    logger.debug(f"Page {page_count}: Falling back to normal fetch...")
                    async with create_http_session(300) as session:
                        response = await self.auth.make_authenticated_request(session, current_url)
                        html = await response.text()
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
            books = self._parse_books_from_html_robust(html)
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

            # Start prefetching next page EARLY in background
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

    def _parse_books_from_html_robust(self, html_content: str) -> list[str]:
        """
        Parse book data from GRIN HTML using fast selectolax parser.
        """
        from selectolax.lexbor import LexborHTMLParser

        try:
            # Parse with selectolax using the fastest parser
            tree = LexborHTMLParser(html_content)
            books = []

            # Find all table rows with barcode inputs
            rows = tree.css("tr")

            for row in rows:
                # Look for checkbox input with barcode
                barcode_input = row.css_first('input[name="barcodes"]')
                if barcode_input:
                    barcode = barcode_input.attributes.get("value")
                    if barcode:
                        # Extract all cell text from this row
                        cells = row.css("td")
                        cell_texts = []

                        for cell in cells:
                            # Get clean text content
                            text = cell.text(strip=True) if cell.text() else ""
                            # Clean up extra whitespace
                            text = " ".join(text.split())
                            cell_texts.append(text)

                        # Create tab-separated line: barcode + cells (skip first checkbox cell)
                        if len(cell_texts) > 1:
                            book_line = barcode + "\t" + "\t".join(cell_texts[1:])
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

    async def download_file(
        self, directory: str, filename: str, output_path: Path, chunk_size: int = 1024 * 1024
    ) -> None:
        """
        Download a file from GRIN to local filesystem.

        Args:
            directory: GRIN directory name
            filename: Name of file to download
            output_path: Local path to save file
            chunk_size: Size of chunks to download
        """
        url = f"{self.base_url}/{directory}/{filename}"

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        async with create_http_session(self.timeout) as session:
            response = await self.auth.make_authenticated_request(session, url)

            async with aiofiles.open(output_path, "wb") as f:
                async for chunk in response.content.iter_chunked(chunk_size):
                    await f.write(chunk)

    async def check_file_exists(self, directory: str, filename: str) -> bool:
        """
        Check if a file exists in GRIN directory.

        Args:
            directory: GRIN directory name
            filename: Name of file to check

        Returns:
            bool: True if file exists
        """
        url = f"{self.base_url}/{directory}/{filename}"

        try:
            async with create_http_session(self.timeout) as session:
                response = await self.auth.make_authenticated_request(session, url, method="HEAD")
                return response.status == 200
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return False
            raise
        except GRINPermissionError:
            # Permission denied might mean file exists but we can't access it
            return False
