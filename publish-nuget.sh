#!/usr/bin/env bash
set -euo pipefail

# Usage: ./publish-nuget.sh <NUGET_API_KEY> [version]
# Example: ./publish-nuget.sh my-api-key 1.0.0

if [ $# -lt 1 ]; then
    echo "Usage: $0 <NUGET_API_KEY> [version]"
    echo "  Get your API key from https://www.nuget.org/account/apikeys"
    exit 1
fi

API_KEY="$1"
VERSION="${2:-}"
PROJECT="src/StreamDB/StreamDB.csproj"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Override version if provided
VERSION_FLAG=""
if [ -n "$VERSION" ]; then
    VERSION_FLAG="-p:Version=$VERSION"
    echo "Publishing StreamDB v$VERSION"
else
    echo "Publishing StreamDB (version from csproj)"
fi

# Clean previous artifacts
rm -rf ./artifacts/*.nupkg
rm -rf src/StreamDB/bin/Release/*.nupkg

# Build and pack
echo "Building and packing..."
dotnet pack "$PROJECT" -c Release $VERSION_FLAG --output ./artifacts

# Find the .nupkg
NUPKG=$(ls ./artifacts/StreamDB.*.nupkg 2>/dev/null | head -1)
if [ -z "$NUPKG" ]; then
    echo "Error: No .nupkg found in ./artifacts/"
    exit 1
fi

echo "Package: $NUPKG"

# Push to NuGet
echo "Pushing to nuget.org..."
dotnet nuget push "$NUPKG" \
    --api-key "$API_KEY" \
    --source https://api.nuget.org/v3/index.json \
    --skip-duplicate

echo "Done! Package published to https://www.nuget.org/packages/StreamDB"
