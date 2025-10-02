#!/bin/bash

# Generate SVG and PNG diagrams for pipeline documentation
# Usage: ./docs/diagrams/generate-images.sh

set -euo pipefail

# Check if mmdc is installed
if ! command -v mmdc &> /dev/null; then
    echo "Error: mermaid-cli (mmdc) is not installed"
    echo "Install with: npm install -g @mermaid-js/mermaid-cli"
    exit 1
fi

# Create output directory if it doesn't exist (kept for backward compatibility)
mkdir -p output

diagrams=(
    collect-pipeline
    sync-pipeline
    lifecycle-summary
<<<<<<< HEAD
    check-conversion
    sync-detail
    upload-step
=======
>>>>>>> 53cd8e3 (Move helper)
)

render_diagram() {
    local name="$1"
    local input="docs/diagrams/${name}.mmd"
    local base_output="docs/diagrams/${name}"

    for extension in svg png; do
        local output="${base_output}.${extension}"
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ba8ed86 (Centralize fixtures)
        local scale="1"
        if [[ "${extension}" == "png" ]]; then
            # Produce 3x size PNGs for higher resolution embeddings
            scale="3"
        fi

        echo "  - ${name}.${extension} (scale ${scale})"
<<<<<<< HEAD
=======
        echo "  - ${name}.${extension}"
>>>>>>> 53cd8e3 (Move helper)
=======
>>>>>>> ba8ed86 (Centralize fixtures)
        mmdc \
            --input "${input}" \
            --output "${output}" \
            --configFile docs/diagrams/mermaid-config.json \
            --cssFile docs/diagrams/mermaid-transparent.css \
            -b transparent \
            --width 900 \
            --height 550 \
<<<<<<< HEAD
<<<<<<< HEAD
            --scale "${scale}"
    done
}

echo "Generating pipeline diagrams (SVG + 3x PNG)..."
=======
            --scale 1.5
    done
}

echo "Generating pipeline diagrams (SVG + PNG)..."
>>>>>>> 53cd8e3 (Move helper)
=======
            --scale "${scale}"
    done
}

echo "Generating pipeline diagrams (SVG + 3x PNG)..."
>>>>>>> ba8ed86 (Centralize fixtures)

for diagram in "${diagrams[@]}"; do
    render_diagram "${diagram}"
done

echo "Diagram generation complete!"
echo "Files generated:"
for diagram in "${diagrams[@]}"; do
    echo "  - docs/diagrams/${diagram}.svg"
<<<<<<< HEAD
<<<<<<< HEAD
    echo "  - docs/diagrams/${diagram}.png (3x scale)"
=======
    echo "  - docs/diagrams/${diagram}.png"
>>>>>>> 53cd8e3 (Move helper)
=======
    echo "  - docs/diagrams/${diagram}.png (3x scale)"
>>>>>>> ba8ed86 (Centralize fixtures)
done
