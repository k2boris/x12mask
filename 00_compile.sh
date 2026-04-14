#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

JAVA_CP="${JAVA_CP:-src:jars/*}"

echo "[00_compile] Compiling Java sources"
javac -cp "${JAVA_CP}" $(find src -name '*.java')
echo "[00_compile] Compile completed"

