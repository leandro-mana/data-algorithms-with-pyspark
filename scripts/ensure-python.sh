#!/usr/bin/env bash
#
# Ensures Python 3.12+ is available (macOS).
# - Checks current Python version
# - Installs via pyenv if not found or version too old
# - Configures shell rc file
#

set -e

REQUIRED_MAJOR=3
REQUIRED_MINOR=12
REQUIRED_VERSION="${REQUIRED_MAJOR}.${REQUIRED_MINOR}"

# Detect shell rc file
detect_shell_rc() {
    local current_shell
    current_shell=$(basename "$SHELL")

    case "$current_shell" in
        zsh)  echo "$HOME/.zshrc" ;;
        bash) echo "$HOME/.bash_profile" ;;
        *)    echo "$HOME/.profile" ;;
    esac
}

# Get Python version as "major.minor"
get_python_version() {
    local python_cmd="$1"

    if ! command -v "$python_cmd" &> /dev/null; then
        return 1
    fi

    "$python_cmd" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
}

# Check if Python version meets requirements
check_python_version() {
    local python_cmd="$1"
    local version

    if ! version=$(get_python_version "$python_cmd" 2>/dev/null); then
        return 1
    fi

    local major minor
    major=$(echo "$version" | cut -d. -f1)
    minor=$(echo "$version" | cut -d. -f2)

    if [[ "$major" -gt "$REQUIRED_MAJOR" ]] || \
       [[ "$major" -eq "$REQUIRED_MAJOR" && "$minor" -ge "$REQUIRED_MINOR" ]]; then
        return 0
    fi

    return 1
}

# Find a working Python command
find_python() {
    for cmd in python3 python python3.12 python3.13; do
        if check_python_version "$cmd"; then
            echo "$cmd"
            return 0
        fi
    done
    return 1
}

# Check if pyenv is configured in rc file
pyenv_configured_in_rc() {
    local rc_file="$1"

    if [[ ! -f "$rc_file" ]]; then
        return 1
    fi

    grep -q 'pyenv init' "$rc_file" 2>/dev/null
}

# Add pyenv config to rc file
configure_pyenv_in_rc() {
    local rc_file="$1"

    echo "" >> "$rc_file"
    echo "# pyenv" >> "$rc_file"
    echo 'export PYENV_ROOT="$HOME/.pyenv"' >> "$rc_file"
    echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> "$rc_file"
    echo 'eval "$(pyenv init -)"' >> "$rc_file"

    echo "Added pyenv configuration to $rc_file"
}

# Install pyenv via Homebrew
install_pyenv() {
    echo "Installing pyenv via Homebrew..."

    if ! command -v brew &> /dev/null; then
        echo "ERROR: Homebrew not found. Please install Homebrew first:"
        echo '  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
        exit 1
    fi

    brew install pyenv
}

# Install Python version via pyenv
install_python_via_pyenv() {
    local version="$1"

    echo "Installing Python $version via pyenv..."
    pyenv install -s "$version"
    pyenv global "$version"

    echo "Python $version installed and set as global default"
}

main() {
    local rc_file
    rc_file=$(detect_shell_rc)

    echo "Checking Python version (requires ${REQUIRED_VERSION}+)..."
    echo "Shell: $(basename "$SHELL"), RC file: $rc_file"
    echo ""

    # Check if suitable Python already exists
    local python_cmd
    if python_cmd=$(find_python); then
        local full_version
        full_version=$("$python_cmd" --version 2>&1)
        echo "✓ $full_version (using: $python_cmd)"
        exit 0
    fi

    echo "Python ${REQUIRED_VERSION}+ not found. Installing..."
    echo ""

    # Ensure pyenv is installed
    if ! command -v pyenv &> /dev/null; then
        install_pyenv

        # Configure pyenv in rc file
        if ! pyenv_configured_in_rc "$rc_file"; then
            configure_pyenv_in_rc "$rc_file"
        fi

        # Initialize pyenv for current session
        export PYENV_ROOT="$HOME/.pyenv"
        export PATH="$PYENV_ROOT/bin:$PATH"
        eval "$(pyenv init -)"
    fi

    # Find latest 3.12.x version available
    local latest_version
    latest_version=$(pyenv install --list | grep -E "^\s*${REQUIRED_VERSION}\.[0-9]+$" | tail -1 | tr -d ' ')

    if [[ -z "$latest_version" ]]; then
        echo "ERROR: Could not find Python ${REQUIRED_VERSION}.x in pyenv"
        exit 1
    fi

    # Install Python
    install_python_via_pyenv "$latest_version"

    # Verify installation
    echo ""
    if python_cmd=$(find_python); then
        local full_version
        full_version=$("$python_cmd" --version 2>&1)
        echo "✓ $full_version installed successfully"

        if ! pyenv_configured_in_rc "$rc_file"; then
            echo ""
            echo "NOTE: pyenv not in $rc_file. Add manually or run:"
            echo "  echo 'eval \"\$(pyenv init -)\"' >> $rc_file"
        fi
    else
        echo "ERROR: Python installation completed but version check failed"
        echo "You may need to restart your terminal or run: source $rc_file"
        exit 1
    fi
}

main
