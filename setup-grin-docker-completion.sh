#!/bin/bash
# Setup script for grin-docker completion

ZSHRC="$HOME/.zshrc"
PROJECT_DIR="$(pwd)"

echo "Setting up grin-docker completion..."

# Add to .zshrc
cat >> "$ZSHRC" << 'EOF'

# grin-docker completion
_grin_docker() {
    local context state line
    _arguments '*::command:->commands' && return 0
    
    case $state in
        commands)
            if [[ -x "./grin-docker" ]]; then
                local -a commands
                commands=(
                    'auth:Set up OAuth2 authentication'
                    'collect:Collect book metadata from GRIN'
                    'process:Request and monitor book processing'
                    'sync:Sync converted books from GRIN to storage'
                    'storage:Manage storage buckets and data'
                    'extract:Extract OCR text from decrypted archives'
                    'enrich:Enrich books with GRIN metadata'
                    'export:Export books to CSV'
                    'reports:View process summaries and reports'
                    'bash:Open interactive shell'
                )
                _describe 'grin-docker commands' commands
            fi
            ;;
    esac
}

compdef _grin_docker grin-docker
EOF

echo "Added grin-docker completion to $ZSHRC"
echo "Run 'source ~/.zshrc' or restart your terminal to activate."
echo ""
echo "Usage: grin-docker <TAB> to see available commands"