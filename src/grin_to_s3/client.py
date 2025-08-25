"""
Async GRIN API client using aiohttp
"""

import asyncio
import logging
import time
from collections.abc import AsyncGenerator, Callable
from datetime import datetime
from typing import Any

import aiohttp
from selectolax.lexbor import LexborHTMLParser
from tenacity import retry, stop_after_attempt, wait_fixed

from grin_to_s3.auth import GRINAuth
from grin_to_s3.common import (
    DEFAULT_CONNECTOR_LIMITS,
    DEFAULT_DOWNLOAD_RETRIES,
    DEFAULT_RETRY_WAIT_SECONDS,
)

logger = logging.getLogger(__name__)

ALL_BOOKS_ENDPOINT = "_all_books"


GRINRow = dict[str, str]  # Type alias for book data with dynamic keys


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

        # Always create and manage a persistent session
        connector = aiohttp.TCPConnector(
            limit=DEFAULT_CONNECTOR_LIMITS["limit"],
            limit_per_host=DEFAULT_CONNECTOR_LIMITS["limit_per_host"]
        )
        timeout_config = aiohttp.ClientTimeout(total=self.timeout, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout_config
        )

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
        response = await self.auth.make_authenticated_request(self.session, url, method=method)
        return await response.text()

    async def download_archive(self, url: str) -> aiohttp.ClientResponse:
        """Download a book archive - for use by download.py."""
        return await self.auth.make_authenticated_request(self.session, url)

    async def head_archive(self, url: str) -> aiohttp.ClientResponse:
        """HEAD request for archive metadata - for use by check.py."""
        return await self.auth.make_authenticated_request(self.session, url, method="HEAD")

    async def close(self):
        """Close the session. Must be called when done with client."""
        await self.session.close()

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
    ) -> AsyncGenerator[tuple[GRINRow, set[str]], None]:
        """
        Stream book list from GRIN with prefetching and SQLite batch optimization.

        Prefetches the next page while processing the current page's data.
        Returns tuples of (book_dict, known_barcodes_set) for batch SQLite optimization.
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
                logger.debug(f"Page {page_count}: Using prefetched data after {wait_elapsed:.2f}s wait at {use_time}")

            else:
                # First page - fetch normally
                logger.debug(f"Page {page_count}: Normal fetch (no prefetch available)")
                response = await self.auth.make_authenticated_request(self.session, current_url)
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
            for book_dict in books:
                if barcode := book_dict.get("barcode", ""):
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
            for i, book_dict in enumerate(books):
                yield book_dict, known_barcodes_on_page
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

    @retry(
        stop=stop_after_attempt(DEFAULT_DOWNLOAD_RETRIES + 1),
        wait=wait_fixed(DEFAULT_RETRY_WAIT_SECONDS),
        reraise=True,
    )
    async def _prefetch_page(self, url: str) -> tuple[str, str]:
        """
        Prefetch a page's HTML content in the background.

        Returns:
            tuple: (html_content, url)
        """
        response = await self.auth.make_authenticated_request(self.session, url)
        html = await response.text()
        return html, url

    def _extract_cell_texts(self, cells) -> list[str]:
        """Extract text content from table cells, handling links and cleaning whitespace."""
        cell_texts = []
        for cell in cells:
            link = cell.css_first("a[href]")
            if link and link.attributes.get("href"):
                text = link.attributes["href"]
            else:
                text = cell.text(strip=True) if cell.text() else ""
                text = " ".join(text.split())
            cell_texts.append(text)
        return cell_texts

    def _parse_books_from_html(self, html_content: str) -> list[GRINRow]:
        """Parse book data from GRIN HTML using CSS selectors to directly extract data."""
        try:
            tree = LexborHTMLParser(html_content)
            books: list[GRINRow] = []

            # Find the data table with class="heading" header row
            for table in tree.css("table"):
                # Look for header row with class="heading"
                header_row = table.css_first("tr.heading")
                if not header_row:
                    continue

                # Extract header names from spans with class="hd3"
                headers = []
                for cell in header_row.css("td"):
                    span = cell.css_first("span.hd3")
                    if span and span.text():
                        headers.append(span.text(strip=True))
                    else:
                        headers.append("")  # Empty header for checkbox/empty columns

                # Debug: Log the headers we found
                logger.debug(f"HTML table headers found: {headers}")

                # Process data rows
                for row in table.css("tbody tr"):
                    if book_record := self._extract_book_from_row(row, headers):
                        books.append(book_record)

                break  # Found the right table, stop looking

            return books

        except Exception as e:
            logger.error(f"selectolax parsing failed: {e}")
            raise

    def _extract_book_from_row(self, row, headers: list[str]) -> GRINRow | None:
        """Extract book data from a table row using CSS selectors and direct header mapping."""
        cells = row.css("td")
        if len(cells) < 2:
            return None

        # Create record by directly mapping cell values to headers
        record = {}

        for i, cell in enumerate(cells):
            if i < len(headers) and headers[i]:  # Skip empty headers
                header_key = headers[i].lower().replace(" ", "_").replace("-", "_").replace("'", "")
                cell_text = cell.text(strip=True) if cell.text() else ""

                # Special handling for barcode extraction
                match header_key:
                    case "filename" if cell_text.endswith(".tar.gz.gpg"):
                        # For converted books: filename contains barcode
                        record["barcode"] = cell_text.replace(".tar.gz.gpg", "")
                    case "barcode":
                        # For all_books: direct barcode field
                        record["barcode"] = cell_text
                    case _ if cell_text:
                        record[header_key] = cell_text

        # Must have a barcode to be valid
        if not record.get("barcode"):
            return None

        return record

    def _extract_next_button_url(self, html_content: str, directory: str) -> str | None:
        """
        Extract Next button URL from GRIN HTML response using selectolax.
        """

        try:
            tree = LexborHTMLParser(html_content)

            # Look for Next button link
            # Try different patterns for Next button
            next_links = tree.css('a[href*="first="], a[href*="ctoken="]')

            for link in next_links:
                if not (href := link.attributes.get("href", "")):
                    continue

                link_text = link.text(strip=True).lower() if link.text() else ""

                # Check if this is a Next button by text content
                if "next" in link_text or ">" in link_text or "&gt;" in (link.html or ""):
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
