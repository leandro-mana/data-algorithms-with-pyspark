#!/usr/bin/env bash
#
# Ensures Poetry is installed and PATH is configured.
# - Detects CI environment (skips rc file modification)
# - Detects current shell (bash/zsh) for local installs
# - Installs Poetry if not found
# - Adds Poetry to PATH in shell rc file if not already present (local only)
#

set -e

POETRY_PATH='export PATH="$HOME/.local/bin:$PATH"'

# Check if running in CI environment
is_ci() {
    [[ -n "${CI:-}" || -n "${GITHUB_ACTIONS:-}" || -n "${GITLAB_CI:-}" || -n "${CIRCLECI:-}" ]]
}

# Detect shell and determine rc file
detect_shell_rc() {
    local current_shell
    current_shell=$(basename "$SHELL")

    case "$current_shell" in
        zsh)
            echo "$HOME/.zshrc"
            ;;
        bash)
            # macOS uses .bash_profile for login shells, Linux uses .bashrc
            if [[ "$OSTYPE" == "darwin"* ]]; then
                echo "$HOME/.bash_profile"
            else
                echo "$HOME/.bashrc"
            fi
            ;;
        *)
            # Default to .profile for other shells
            echo "$HOME/.profile"
            ;;
    esac
}

# Check if PATH config already exists in rc file
path_already_configured() {
    local rc_file="$1"

    if [[ ! -f "$rc_file" ]]; then
        return 1  # File doesn't exist, so not configured
    fi

    # Check for common Poetry PATH patterns
    if grep -q '\.local/bin' "$rc_file" 2>/dev/null; then
        return 0  # Already configured
    fi

    return 1  # Not configured
}

# Add Poetry PATH to rc file
add_path_to_rc() {
    local rc_file="$1"

    echo "" >> "$rc_file"
    echo "# Poetry" >> "$rc_file"
    echo "$POETRY_PATH" >> "$rc_file"

    echo "Added Poetry PATH to $rc_file"
}

# Add to GITHUB_PATH for CI
add_to_github_path() {
    if [[ -n "${GITHUB_PATH:-}" ]]; then
        echo "$HOME/.local/bin" >> "$GITHUB_PATH"
        echo "Added Poetry to GITHUB_PATH"
    fi
}

# Main logic
main() {
    if is_ci; then
        echo "CI environment detected"
    else
        local rc_file
        rc_file=$(detect_shell_rc)
        echo "Detected shell: $(basename "$SHELL")"
        echo "RC file: $rc_file"
    fi
    echo ""

    # Check if poetry is already available
    if command -v poetry &> /dev/null; then
        echo "Poetry is already installed: $(poetry --version)"

        if ! is_ci; then
            # Still check if PATH is configured in rc file
            if path_already_configured "$rc_file"; then
                echo "PATH already configured in $rc_file"
            else
                echo "Poetry works but PATH not in $rc_file (may be set elsewhere)"
            fi
        fi
        exit 0
    fi

    echo "Poetry not found. Installing..."
    echo ""

    # Install Poetry using the official installer
    curl -sSL https://install.python-poetry.org | python3 -

    # Add Poetry to PATH for current session
    export PATH="$HOME/.local/bin:$PATH"

    # Verify installation
    if ! command -v poetry &> /dev/null; then
        echo "ERROR: Poetry installation completed but 'poetry' command not found."
        echo "Please check the installation and try again."
        exit 1
    fi

    echo ""
    echo "Poetry installed successfully: $(poetry --version)"

    if is_ci; then
        # In CI, add to GITHUB_PATH so subsequent steps can use poetry
        add_to_github_path
    else
        # Configure PATH in rc file if not already present
        if path_already_configured "$rc_file"; then
            echo "PATH already configured in $rc_file - skipping"
        else
            add_path_to_rc "$rc_file"
            echo ""
            echo "To apply changes in current terminal, run:"
            echo "  source $rc_file"
        fi
    fi
}

main
