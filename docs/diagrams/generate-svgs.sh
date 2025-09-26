#!/bin/bash

# Generate SVG diagrams for pipeline documentation
# Usage: ./docs/generate-svgs.sh

set -e

# Check if mmdc is installed
if ! command -v mmdc &> /dev/null; then
    echo "Error: mermaid-cli (mmdc) is not installed"
    echo "Install with: npm install -g @mermaid-js/mermaid-cli"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p output

echo "Generating pipeline SVG diagrams..."

# Generate collect pipeline SVG
echo "  - collect-pipeline.svg"
mmdc \
    --input docs/diagrams/collect-pipeline.mmd \
    --output docs/diagrams/collect-pipeline.svg \
    --configFile docs/diagrams/mermaid-config.json \
    --cssFile docs/diagrams/mermaid-transparent.css \
    -b transparent \
    --width 900 \
    --height 550 \
    --scale 1.5

# Generate sync pipeline SVG
echo "  - sync-pipeline.svg"
mmdc \
    --input docs/diagrams/sync-pipeline.mmd \
    --output docs/diagrams/sync-pipeline.svg \
    --configFile docs/diagrams/mermaid-config.json \
    --cssFile docs/diagrams/mermaid-transparent.css \
    -b transparent \
    --width 900 \
    --height 550 \
    --scale 1.5

echo "SVG generation complete!"
echo "Files generated:"
echo "  - docs/diagrams/collect-pipeline.svg"
echo "  - docs/diagrams/sync-pipeline.svg"