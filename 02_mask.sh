#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <transaction_id> [application.properties]"
  exit 1
fi

TXN_ID="$1"
CONFIG_FILE="${2:-application.properties}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

MAIN_CLASS="com.k2view.x12mask.Main"
JAVA_CP="${JAVA_CP:-src:jars/*}"

echo "[02_mask] Running Stage 2 (jdbc read -> reverse json -> masked edi) for ${TXN_ID}"
java -cp "${JAVA_CP}" "${MAIN_CLASS}" mask "${TXN_ID}" "${CONFIG_FILE}"

