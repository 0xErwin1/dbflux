#!/usr/bin/env bash
# validate_tools.sh
# Validates that MCP tools are synchronized across three locations:
# 1. Implementation (#[tool] attributes in src/tools/*.rs)
# 2. CANONICAL_V1_TOOLS in dbflux_mcp/src/tool_catalog.rs
# 3. Builtin policies in dbflux_mcp/src/built_ins.rs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TOOLS_DIR="$REPO_ROOT/crates/dbflux_mcp_server/src/tools"
CATALOG_FILE="$REPO_ROOT/crates/dbflux_mcp/src/tool_catalog.rs"
BUILTINS_FILE="$REPO_ROOT/crates/dbflux_mcp/src/built_ins.rs"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "MCP Tools Validation Report"
echo "============================"
echo ""

# Extract implemented tools from #[tool] attributes
# Strategy: Look for lines with #[tool], then find the next "async fn" (may be several lines later)
echo "1. Extracting implemented tools from src/tools/*.rs..."
IMPLEMENTED_TOOLS=$(for file in "$TOOLS_DIR"/*.rs; do
    awk '
        /^[[:space:]]*#\[tool/ { in_tool=1; next }
        in_tool && /async fn/ {
            match($0, /async fn ([a-z_]+)/, arr)
            if (arr[1]) print arr[1]
            in_tool=0
        }
    ' "$file"
done | sort | uniq)

IMPLEMENTED_COUNT=$(echo "$IMPLEMENTED_TOOLS" | wc -l)
echo "   Found $IMPLEMENTED_COUNT implemented tools"
echo ""

# Extract CANONICAL_V1_TOOLS
echo "2. Extracting CANONICAL_V1_TOOLS from tool_catalog.rs..."
CANONICAL_TOOLS=$(awk '/pub const CANONICAL_V1_TOOLS/,/\];/ { if (/"[a-z_]+"/) { match($0, /"([a-z_]+)"/, arr); print arr[1] } }' "$CATALOG_FILE" | sort)

CANONICAL_COUNT=$(echo "$CANONICAL_TOOLS" | wc -l)
echo "   Found $CANONICAL_COUNT canonical tools"
echo ""

# Extract builtin policy tools
# We extract tools from allowed_tools sections only (not allowed_classes)
echo "3. Extracting tools from builtin policies..."

READONLY_TOOLS=$(awk '
    /id: "builtin\/read-only"/ { in_readonly=1; next }
    in_readonly && /allowed_tools:/ { in_tools=1; next }
    in_readonly && in_tools && /allowed_classes:/ { exit }
    in_readonly && in_tools && /"[a-z_]+".to_string/ {
        match($0, /"([a-z_]+)"/, arr)
        print arr[1]
    }
' "$BUILTINS_FILE" | sort)

WRITE_TOOLS=$(awk '
    /id: "builtin\/write"/ { in_write=1; next }
    in_write && /allowed_tools:/ { in_tools=1; next }
    in_write && in_tools && /allowed_classes:/ { exit }
    in_write && in_tools && /"[a-z_]+".to_string/ {
        match($0, /"([a-z_]+)"/, arr)
        print arr[1]
    }
' "$BUILTINS_FILE" | sort)

ADMIN_TOOLS=$(awk '
    /id: "builtin\/admin"/ { in_admin=1; next }
    in_admin && /allowed_tools:/ { in_tools=1; next }
    in_admin && in_tools && /allowed_classes:/ { exit }
    in_admin && in_tools && /"[a-z_]+".to_string/ {
        match($0, /"([a-z_]+)"/, arr)
        print arr[1]
    }
' "$BUILTINS_FILE" | sort)

READONLY_COUNT=$(echo "$READONLY_TOOLS" | wc -l)
WRITE_COUNT=$(echo "$WRITE_TOOLS" | wc -l)
ADMIN_COUNT=$(echo "$ADMIN_TOOLS" | wc -l)

echo "   Read-only policy: $READONLY_COUNT tools"
echo "   Write policy: $WRITE_COUNT tools"
echo "   Admin policy: $ADMIN_COUNT tools"
echo ""

# Validate: All implemented tools must be in CANONICAL
echo "4. Checking implementation vs CANONICAL_V1_TOOLS..."
MISSING_FROM_CANONICAL=""
while IFS= read -r tool; do
    if [ -n "$tool" ] && ! echo "$CANONICAL_TOOLS" | grep -q "^${tool}$"; then
        MISSING_FROM_CANONICAL="${MISSING_FROM_CANONICAL}${tool}\n"
    fi
done <<< "$IMPLEMENTED_TOOLS"

if [ -z "$MISSING_FROM_CANONICAL" ]; then
    echo -e "   ${GREEN}✓${NC} All implemented tools are in CANONICAL_V1_TOOLS"
else
    echo -e "   ${RED}✗${NC} Tools missing from CANONICAL_V1_TOOLS:"
    echo -e "$MISSING_FROM_CANONICAL" | grep -v '^$' | sed 's/^/     - /'
fi
echo ""

# Validate: All CANONICAL tools should be implemented (or intentionally deferred)
echo "5. Checking CANONICAL_V1_TOOLS vs implementation..."
MISSING_IMPLEMENTATION=""
while IFS= read -r tool; do
    if [ -n "$tool" ] && ! echo "$IMPLEMENTED_TOOLS" | grep -q "^${tool}$"; then
        MISSING_IMPLEMENTATION="${MISSING_IMPLEMENTATION}${tool}\n"
    fi
done <<< "$CANONICAL_TOOLS"

if [ -z "$MISSING_IMPLEMENTATION" ]; then
    echo -e "   ${GREEN}✓${NC} All CANONICAL_V1_TOOLS are implemented"
else
    echo -e "   ${YELLOW}!${NC} CANONICAL_V1_TOOLS not yet implemented:"
    echo -e "$MISSING_IMPLEMENTATION" | grep -v '^$' | sed 's/^/     - /'
fi
echo ""

# Validate: All builtin policy tools should be in CANONICAL
echo "6. Checking builtin policies reference valid tools..."
INVALID_READONLY=""
INVALID_WRITE=""
INVALID_ADMIN=""

while IFS= read -r tool; do
    if [ -n "$tool" ] && ! echo "$CANONICAL_TOOLS" | grep -q "^${tool}$"; then
        INVALID_READONLY="${INVALID_READONLY}${tool}\n"
    fi
done <<< "$READONLY_TOOLS"

while IFS= read -r tool; do
    if [ -n "$tool" ] && ! echo "$CANONICAL_TOOLS" | grep -q "^${tool}$"; then
        INVALID_WRITE="${INVALID_WRITE}${tool}\n"
    fi
done <<< "$WRITE_TOOLS"

while IFS= read -r tool; do
    if [ -n "$tool" ] && ! echo "$CANONICAL_TOOLS" | grep -q "^${tool}$"; then
        INVALID_ADMIN="${INVALID_ADMIN}${tool}\n"
    fi
done <<< "$ADMIN_TOOLS"

if [ -z "$INVALID_READONLY" ] && [ -z "$INVALID_WRITE" ] && [ -z "$INVALID_ADMIN" ]; then
    echo -e "   ${GREEN}✓${NC} All builtin policy tools are valid"
else
    if [ -n "$INVALID_READONLY" ]; then
        echo -e "   ${RED}✗${NC} Invalid tools in read-only policy:"
        echo -e "$INVALID_READONLY" | grep -v '^$' | sed 's/^/     - /'
    fi
    if [ -n "$INVALID_WRITE" ]; then
        echo -e "   ${RED}✗${NC} Invalid tools in write policy:"
        echo -e "$INVALID_WRITE" | grep -v '^$' | sed 's/^/     - /'
    fi
    if [ -n "$INVALID_ADMIN" ]; then
        echo -e "   ${RED}✗${NC} Invalid tools in admin policy:"
        echo -e "$INVALID_ADMIN" | grep -v '^$' | sed 's/^/     - /'
    fi
fi
echo ""

# Validate: All CANONICAL tools should be in at least one policy
echo "7. Checking all tools are in at least one policy..."
ALL_POLICY_TOOLS=$(echo -e "${READONLY_TOOLS}\n${WRITE_TOOLS}\n${ADMIN_TOOLS}" | sort | uniq)
MISSING_FROM_POLICIES=""

while IFS= read -r tool; do
    if [ -n "$tool" ] && ! echo "$ALL_POLICY_TOOLS" | grep -q "^${tool}$"; then
        MISSING_FROM_POLICIES="${MISSING_FROM_POLICIES}${tool}\n"
    fi
done <<< "$CANONICAL_TOOLS"

if [ -z "$MISSING_FROM_POLICIES" ]; then
    echo -e "   ${GREEN}✓${NC} All CANONICAL tools are in at least one policy"
else
    echo -e "   ${RED}✗${NC} Tools missing from all policies:"
    echo -e "$MISSING_FROM_POLICIES" | grep -v '^$' | sed 's/^/     - /'
fi
echo ""

# Summary
echo "============================"
echo "Summary:"
echo "  - Implemented: $IMPLEMENTED_COUNT tools"
echo "  - Canonical: $CANONICAL_COUNT tools"
echo "  - Read-only: $READONLY_COUNT tools"
echo "  - Write: $WRITE_COUNT tools"
echo "  - Admin: $ADMIN_COUNT tools"
echo ""

# Exit code
if [ -n "$MISSING_FROM_CANONICAL" ] || \
   [ -n "$INVALID_READONLY" ] || \
   [ -n "$INVALID_WRITE" ] || \
   [ -n "$INVALID_ADMIN" ] || \
   [ -n "$MISSING_FROM_POLICIES" ]; then
    echo -e "${RED}Validation FAILED${NC} - Please fix the issues above"
    exit 1
else
    echo -e "${GREEN}Validation PASSED${NC} - All tools are synchronized"
    exit 0
fi
