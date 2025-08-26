"""
Pure function for parsing GRIN TSV responses.

Extracted from the enrichment pipeline to improve testability and separation of concerns.
"""

import logging

from grin_to_s3.collect_books.models import BookRecord

logger = logging.getLogger(__name__)


def parse_grin_tsv(tsv_text: str) -> dict[str, dict]:
    """Parse GRIN TSV response into barcode -> metadata dict.

    Args:
        tsv_text: TSV response from GRIN API

    Returns:
        Dict mapping barcode to metadata fields
    """
    lines = tsv_text.strip().split("\n")
    if len(lines) < 2:
        logger.debug("Insufficient TSV data - no header or data lines")
        return {}

    headers = lines[0].split("\t")
    mapping = BookRecord.get_grin_tsv_column_mapping()
    results = {}

    for i, line in enumerate(lines[1:], 1):
        values = line.split("\t")

        # Get barcode from first column
        if not values or not values[0]:
            logger.debug(f"Empty barcode in line {i}")
            continue

        barcode = values[0]

        # Pad values with empty strings if there are fewer values than headers
        if len(values) < len(headers):
            missing_count = len(headers) - len(values)
            logger.debug(
                f"Padding {barcode}: {len(headers)} headers, {len(values)} values - adding {missing_count} empty values"
            )
            values.extend([""] * missing_count)
        elif len(values) > len(headers):
            # This shouldn't happen, but handle it gracefully
            logger.warning(
                f"More values than headers for {barcode}: {len(headers)} headers, "
                f"{len(values)} values - truncating values"
            )
            values = values[: len(headers)]

        # Build metadata dict using column mapping
        metadata = {}
        for tsv_col, field_name in mapping.items():
            if tsv_col in headers:
                idx = headers.index(tsv_col)
                metadata[field_name] = values[idx] if idx < len(values) else ""
            else:
                metadata[field_name] = ""

        results[barcode] = metadata
        logger.debug(
            f"Parsed {barcode}: State={metadata.get('grin_state')}, Viewability={metadata.get('grin_viewability')}"
        )

    return results
