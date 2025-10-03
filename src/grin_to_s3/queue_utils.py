"""
Utility functions for querying GRIN queues
"""

import logging
import time

from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from grin_to_s3.client import GRINClient

logger = logging.getLogger(__name__)

# Cache for in_process queue data
_in_process_cache: dict[str, tuple[set[str], float]] = {}

BarcodeSet = set[str]


async def get_previous_queue_books(db) -> set[str]:
    """Get the barcodes for books in the previous queue.

    Returns books that:
    - Have GRIN state = 'PREVIOUSLY_DOWNLOADED'
    - Have never been requested for processing (processing_request_timestamp IS NULL or empty)
    - Are not marked as unavailable or conversion_failed in conversion status history

    Args:
        db: Database tracker instance

    Returns:
        set: Set of previous queue book barcodes
    """
    query = """
        SELECT barcode FROM books
        WHERE grin_state = 'PREVIOUSLY_DOWNLOADED'
        AND NULLIF(processing_request_timestamp, '') IS NULL
        AND barcode NOT IN (
            SELECT DISTINCT barcode FROM book_status_history
            WHERE status_type = 'conversion' AND status_value IN ('unavailable', 'conversion_failed')
        )
    """

    cursor = await db._execute_query(query, ())
    rows = await cursor.fetchall()
    previous_queue_barcodes = {row[0] for row in rows}

    logger.debug(f"Found {len(previous_queue_barcodes)} previous queue books in database")
    return previous_queue_barcodes


async def get_unconverted_books(db) -> set[str]:
    """Get the barcodes for books which have never been converted for download.

    Returns books that:
    - Have never been requested for processing (processing_request_timestamp IS NULL or empty string)
    - Have never been converted by GRIN (converted_date IS NULL or empty string)
    - Are not checked in (grin_check_in_date IS NULL or empty string)
    - Have no GRIN state or are not in CHECKED_IN or NOT_AVAILABLE_FOR_DOWNLOAD states
    - Are not marked as unavailable or conversion_failed

    Args:
        db: Database tracker instance

    Returns:
        set: Set of unconverted book barcodes
    """

    query = """
        SELECT barcode FROM books
        WHERE NULLIF(processing_request_timestamp, '') IS NULL
        AND NULLIF(converted_date, '') IS NULL
        AND NULLIF(grin_check_in_date, '') IS NULL
        AND (NULLIF(grin_state, '') IS NULL OR grin_state NOT IN ('CHECKED_IN', 'NOT_AVAILABLE_FOR_DOWNLOAD'))
        AND barcode NOT IN (
            SELECT DISTINCT barcode FROM book_status_history
            WHERE status_type = 'conversion' AND status_value IN ('unavailable', 'conversion_failed')
        )
    """

    cursor = await db._execute_query(query, ())
    rows = await cursor.fetchall()
    unconverted_barcodes = {row[0] for row in rows}

    logger.debug(f"Found {len(unconverted_barcodes)} unconverted books in database")
    return unconverted_barcodes


# Retry queue requests with exponential backoff for transient failures
# Retry schedule: immediate, 2s, 4s, 8s (total ~14s across 4 attempts)
@retry(
    stop=stop_after_attempt(4),
    retry=lambda retry_state: bool(retry_state.outcome and retry_state.outcome.failed),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def get_converted_books(
    grin_client: GRINClient, library_directory: str, page_size: int = 50_000, limit: int | None = None
) -> set[str]:
    """Get set of books that are converted and ready for download.

    Args:
        grin_client: GRIN client instance
        library_directory: Library directory name
        page_size: Number of results per page
        limit: Maximum number of books to fetch (stops pagination early)

    Returns:
        set: Set of converted book barcodes
    """
    try:
        converted_barcodes = set()
        page_count = 0

        # Stream through all pages of the converted list
        async for book_row, _ in grin_client.stream_book_list_html_prefetch(
            directory=library_directory,
            list_type="_converted",
            page_size=page_size,
            start_page=1,
            sqlite_tracker=None,  # No need for SQLite tracking here
        ):
            if barcode := book_row.get("barcode", ""):
                converted_barcodes.add(barcode)

            # Stop early if we've hit the limit
            if limit and len(converted_barcodes) >= limit:
                logger.info(f"Reached limit of {limit:,} converted books, stopping pagination")
                break

            # Track progress for large lists
            if len(converted_barcodes) % 50_000 == 0 and len(converted_barcodes) > 0:
                page_count = len(converted_barcodes) // page_size + 1
                logger.info(f"Fetched {len(converted_barcodes):,} converted books (page ~{page_count})")

        logger.info(f"Loaded {len(converted_barcodes):,} converted books via HTML pagination")
        return converted_barcodes

    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e) if str(e) else "No error message"
        logger.warning(f"Failed to get converted books via HTML: {error_type}: {error_msg}")
        return set()


@retry(
    stop=stop_after_attempt(4),
    retry=lambda retry_state: bool(retry_state.outcome and retry_state.outcome.failed),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def get_in_process_set(grin_client: GRINClient, library_directory: str) -> BarcodeSet:
    """Get set of books currently in GRIN processing queue with caching.
    Args:
        grin_client: GRIN client instance
        library_directory: Library directory name
    Returns:
        Set of barcodes currently in processing queue
    """
    current_time = time.time()
    cache_key = library_directory
    # Check cache (1 hour TTL)
    if cache_key in _in_process_cache:
        books, cached_time = _in_process_cache[cache_key]
        if current_time - cached_time < 3600:
            logger.debug(f"Using cached in_process data for {library_directory}")
            return books

    # Use streaming approach to avoid materializing large responses
    in_process_barcodes = set()
    async for line in grin_client.stream_text_lines(library_directory, "_in_process?format=text"):
        clean_line = line.strip()
        if clean_line:
            if ".tar.gz.gpg" in clean_line:
                barcode = clean_line.removesuffix(".tar.gz.gpg")
                in_process_barcodes.add(barcode)
            else:
                # Handle bare barcode format
                in_process_barcodes.add(clean_line)

    # Update cache
    _in_process_cache[cache_key] = (in_process_barcodes, current_time)
    logger.debug(f"Fetched {len(in_process_barcodes)} in_process books for {library_directory}")
    return in_process_barcodes


@retry(
    stop=stop_after_attempt(2),
    retry=lambda retry_state: bool(retry_state.outcome and retry_state.outcome.failed),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def get_all_conversion_failed_books(grin_client: GRINClient, library_directory: str) -> dict[str, dict[str, str]]:
    """Get ALL books currently in GRIN's _failed endpoint.

    Used by the pipeline to identify known conversion failures during CHECK task.

    Args:
        grin_client: GRIN client instance
        library_directory: Library directory name

    Returns:
        Dict mapping barcode to failure metadata dict with keys:
        - grin_convert_failed_date
        - grin_convert_failed_info
        - grin_detailed_convert_failed_info
    """
    # Fetch and parse HTML from _failed endpoint (result_count=-1 returns all results; this
    # page is typically not long enough to need pagination)
    html = await grin_client.fetch_resource(library_directory, "_failed?result_count=-1")
    failed_books = grin_client._parse_books_from_html(html)

    logger.debug(f"Parsed {len(failed_books)} books from _failed endpoint")

    # Convert to metadata dict
    all_failures = {}
    for book in failed_books:
        barcode = book.get("barcode")
        if not barcode:
            continue

        all_failures[barcode] = {
            "grin_convert_failed_date": book.get("convert_failed_date", ""),
            "grin_convert_failed_info": book.get("convert_failed_info", ""),
            "grin_detailed_convert_failed_info": book.get("detailed_convert_failed_info", ""),
        }

    logger.info(f"Loaded {len(all_failures)} conversion failures from GRIN")
    return all_failures
