#!/usr/bin/env bash
#
# Ensures Java 17+ is available (macOS).
# - Checks JAVA_HOME and java command
# - Installs via Homebrew if not found or version too old
# - Configures JAVA_HOME in shell rc file
#

set -e

REQUIRED_VERSION=17

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

# Detect if running on Apple Silicon or Intel
is_apple_silicon() {
    [[ $(uname -m) == "arm64" ]]
}

# Get Homebrew prefix based on architecture
get_brew_prefix() {
    if is_apple_silicon; then
        echo "/opt/homebrew"
    else
        echo "/usr/local"
    fi
}

# Extract major version from java -version output
get_java_major_version() {
    local java_cmd="$1"

    if [[ ! -x "$java_cmd" ]] && ! command -v "$java_cmd" &> /dev/null; then
        return 1
    fi

    local version_output
    version_output=$("$java_cmd" -version 2>&1 | head -1)

    # Extract version: handles "17.0.1", "21.0.1", or old "1.8.0" format
    if [[ "$version_output" =~ \"([0-9]+)\.([0-9]+) ]]; then
        local major="${BASH_REMATCH[1]}"
        # Handle old 1.x naming (1.8 = Java 8)
        if [[ "$major" == "1" ]]; then
            echo "${BASH_REMATCH[2]}"
        else
            echo "$major"
        fi
        return 0
    fi

    return 1
}

# Check if Java meets version requirements
check_java_version() {
    local java_cmd="$1"
    local version

    if ! version=$(get_java_major_version "$java_cmd" 2>/dev/null); then
        return 1
    fi

    if [[ "$version" -ge "$REQUIRED_VERSION" ]]; then
        echo "$version"
        return 0
    fi

    return 1
}

# Check if JAVA_HOME is configured in rc file
java_home_configured_in_rc() {
    local rc_file="$1"

    if [[ ! -f "$rc_file" ]]; then
        return 1
    fi

    grep -q 'JAVA_HOME' "$rc_file" 2>/dev/null
}

# Add Java config to rc file
configure_java_in_rc() {
    local rc_file="$1"
    local brew_prefix
    brew_prefix=$(get_brew_prefix)

    echo "" >> "$rc_file"
    echo "# Java (OpenJDK ${REQUIRED_VERSION})" >> "$rc_file"
    echo "export PATH=\"${brew_prefix}/opt/openjdk@${REQUIRED_VERSION}/bin:\$PATH\"" >> "$rc_file"
    echo 'export JAVA_HOME="$(/usr/libexec/java_home -v'"${REQUIRED_VERSION}"')"' >> "$rc_file"

    echo "Added Java configuration to $rc_file"
}

# Install Java via Homebrew
install_java() {
    echo "Installing OpenJDK ${REQUIRED_VERSION} via Homebrew..."

    if ! command -v brew &> /dev/null; then
        echo "ERROR: Homebrew not found. Please install Homebrew first:"
        echo '  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
        exit 1
    fi

    brew install openjdk@${REQUIRED_VERSION}

    # Create symlink for system Java wrappers (requires sudo on some systems)
    local brew_prefix
    brew_prefix=$(get_brew_prefix)
    local jdk_path="${brew_prefix}/opt/openjdk@${REQUIRED_VERSION}/libexec/openjdk.jdk"

    if [[ -d "$jdk_path" ]]; then
        echo ""
        echo "To make Java ${REQUIRED_VERSION} available system-wide, run:"
        echo "  sudo ln -sfn ${jdk_path} /Library/Java/JavaVirtualMachines/openjdk-${REQUIRED_VERSION}.jdk"
    fi
}

main() {
    local rc_file
    rc_file=$(detect_shell_rc)
    local brew_prefix
    brew_prefix=$(get_brew_prefix)

    echo "Checking Java version (requires ${REQUIRED_VERSION}+)..."
    echo "Shell: $(basename "$SHELL"), RC file: $rc_file"
    echo "Architecture: $(uname -m)"
    echo ""

    local java_cmd=""
    local java_version=""

    # Check JAVA_HOME first
    if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]]; then
        if java_version=$(check_java_version "$JAVA_HOME/bin/java"); then
            java_cmd="$JAVA_HOME/bin/java"
        fi
    fi

    # Check Homebrew OpenJDK path
    if [[ -z "$java_cmd" ]]; then
        local brew_java="${brew_prefix}/opt/openjdk@${REQUIRED_VERSION}/bin/java"
        if [[ -x "$brew_java" ]] && java_version=$(check_java_version "$brew_java"); then
            java_cmd="$brew_java"
        fi
    fi

    # Fall back to PATH
    if [[ -z "$java_cmd" ]] && command -v java &> /dev/null; then
        if java_version=$(check_java_version "java"); then
            java_cmd="java"
        fi
    fi

    # If found, report and exit
    if [[ -n "$java_cmd" ]]; then
        local full_version
        full_version=$("$java_cmd" -version 2>&1 | head -1)
        echo "✓ Java $java_version ($full_version)"

        if [[ -n "$JAVA_HOME" ]]; then
            echo "  JAVA_HOME: $JAVA_HOME"
        else
            echo "  WARNING: JAVA_HOME not set"
            if ! java_home_configured_in_rc "$rc_file"; then
                echo "  Adding JAVA_HOME to $rc_file..."
                configure_java_in_rc "$rc_file"
                echo "  Run: source $rc_file"
            fi
        fi
        exit 0
    fi

    # Report what was found (if anything)
    if command -v java &> /dev/null; then
        local installed_version
        installed_version=$(get_java_major_version "java" || echo "unknown")
        echo "Java found but version $installed_version is below required ${REQUIRED_VERSION}"
    else
        echo "Java not found"
    fi

    echo ""
    echo "Installing Java ${REQUIRED_VERSION}..."
    echo ""

    # Install Java
    install_java

    # Configure in rc file
    if ! java_home_configured_in_rc "$rc_file"; then
        configure_java_in_rc "$rc_file"
    fi

    # Set up for current session
    export PATH="${brew_prefix}/opt/openjdk@${REQUIRED_VERSION}/bin:$PATH"
    export JAVA_HOME="$(/usr/libexec/java_home -v${REQUIRED_VERSION} 2>/dev/null || echo "")"

    # Verify installation
    echo ""
    if java_version=$(check_java_version "java" 2>/dev/null); then
        local full_version
        full_version=$(java -version 2>&1 | head -1)
        echo "✓ Java $java_version installed successfully"
        echo ""
        echo "To apply changes in current terminal, run:"
        echo "  source $rc_file"
    else
        echo "Java installed. Restart your terminal or run:"
        echo "  source $rc_file"
    fi
}

main
