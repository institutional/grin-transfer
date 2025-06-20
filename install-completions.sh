#!/bin/bash
# Install zsh completions for GRIN-to-S3 pipeline scripts

set -e

COMPLETION_FILE="_grin_completions"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPLETION_PATH="$SCRIPT_DIR/$COMPLETION_FILE"

echo "Installing zsh completions for GRIN-to-S3 pipeline..."

# Check if zsh completion system is available
if ! command -v zsh >/dev/null 2>&1; then
    echo "Warning: zsh not found. Completions will only work if zsh is installed."
fi

# Method 1: Add to user's zsh completion directory
ZSH_COMPLETION_DIR="$HOME/.zsh/completions"
if [[ ! -d "$ZSH_COMPLETION_DIR" ]]; then
    echo "Creating user completion directory: $ZSH_COMPLETION_DIR"
    mkdir -p "$ZSH_COMPLETION_DIR"
fi

echo "Copying completion file to $ZSH_COMPLETION_DIR/$COMPLETION_FILE"
cp "$COMPLETION_PATH" "$ZSH_COMPLETION_DIR/$COMPLETION_FILE"

# Check if fpath includes the completion directory
ZSHRC="$HOME/.zshrc"
FPATH_LINE="fpath=(\$HOME/.zsh/completions \$fpath)"

if [[ -f "$ZSHRC" ]]; then
    if ! grep -q "\.zsh/completions" "$ZSHRC"; then
        echo "Adding completion directory to ~/.zshrc"
        echo "" >> "$ZSHRC"
        echo "# GRIN-to-S3 completions" >> "$ZSHRC"
        echo "$FPATH_LINE" >> "$ZSHRC"
        echo "autoload -U compinit && compinit" >> "$ZSHRC"
    else
        echo "Completion directory already in ~/.zshrc"
    fi
else
    echo "Creating ~/.zshrc with completion setup"
    echo "# GRIN-to-S3 completions" > "$ZSHRC"
    echo "$FPATH_LINE" >> "$ZSHRC"
    echo "autoload -U compinit && compinit" >> "$ZSHRC"
fi

echo ""
echo "✅ Installation complete!"
echo ""
echo "To activate completions:"
echo "  1. Restart your terminal, or"
echo "  2. Run: source ~/.zshrc"
echo ""
echo "To test completions:"
echo "  python grin.py <TAB>"
echo "  python grin.py sync-pipeline <TAB>"
echo "  python grin.py process <TAB>"
echo "  python grin.py storage <TAB>"
echo "  python grin.py collect --run-name <TAB>"
echo ""
echo "Note: Run names will be auto-completed from the 'output' directory"