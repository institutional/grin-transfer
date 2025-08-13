#!/usr/bin/env python3
"""
MARC metadata extraction testing CLI.
"""

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from .marc_extraction import extract_marc_metadata


async def test_marc_extraction(tarballs: list[str], output_file: str = None) -> list[dict[str, Any]]:
    """Test MARC extraction on a list of tarball files."""
    results = []

    for tarball_path in tarballs:
        barcode = Path(tarball_path).stem.replace(".tar", "")
        print(f"Processing {barcode}...")

        try:
            # Extract MARC metadata directly from the archive
            marc_metadata = extract_marc_metadata(tarball_path)

            result = {"barcode": barcode, "marc_metadata": marc_metadata}
            results.append(result)
            print(f"  ✅ Extracted MARC metadata for {barcode}")

        except Exception as e:
            print(f"  ❌ Error processing {barcode}: {e}")
            continue

    # Save results if output file specified
    if output_file:
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to {output_file}")

    return results


def print_marc_summary(results: list[dict[str, Any]]):
    """Print a summary of extracted MARC metadata."""
    print(f"\n{'=' * 80}")
    print("MARC EXTRACTION SUMMARY")
    print("=" * 80)
    print(f"Successfully processed {len(results)} books\n")

    for result in results:
        marc = result["marc_metadata"]
        print(f"Barcode: {result['barcode']}")
        print(f"  Title: {marc.get('title', 'N/A')}")
        print(f"  Author (Personal): {marc.get('author_personal', 'N/A')}")
        print(f"  Author (Corporate): {marc.get('author_corporate', 'N/A')}")
        print(f"  Date 1: {marc.get('date_1', 'N/A')}")
        print(f"  Date 2: {marc.get('date_2', 'N/A')}")
        print(f"  Language: {marc.get('language', 'N/A')}")
        print(f"  OCLC Numbers: {marc.get('oclc_numbers', 'N/A')}")
        print(f"  ISBN: {marc.get('isbn', 'N/A')}")
        print(f"  LCCN: {marc.get('lccn', 'N/A')}")
        print(f"  LC Call Number: {marc.get('lc_call_number', 'N/A')}")
        print(f"  Subjects: {marc.get('subjects', 'N/A')}")
        print(f"  Genres: {marc.get('genres', 'N/A')}")
        print()


async def main():
    """Main entry point for metadata testing."""
    parser = argparse.ArgumentParser(
        prog="python -m grin_to_s3.metadata", description="Test MARC metadata extraction from Google Books archives"
    )

    parser.add_argument("tarballs", nargs="+", help="Path(s) to .tar.gz files to process")
    parser.add_argument("--output", "-o", help="Output JSON file to save results")
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress summary output")

    args = parser.parse_args()

    # Test MARC extraction
    results = await test_marc_extraction(args.tarballs, args.output)

    # Print summary unless quiet mode
    if not args.quiet:
        print_marc_summary(results)

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
