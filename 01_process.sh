#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

MAIN_CLASS="com.k2view.x12mask.Main"
JAVA_CP="${JAVA_CP:-src:jars/*}"
CONFIG_FILE="${1:-application.properties}"
SQLITE_DB_PATH="${SQLITE_DB_PATH:-connector/x12-staging.db}"

read_prop() {
  local key="$1"
  local file="$2"
  local line
  line="$(grep -E "^${key}=" "${file}" | tail -n 1 || true)"
  echo "${line#*=}"
}

echo "[01_process] Running Stage 1 (extract -> trace -> sqlite staging)"
java -cp "${JAVA_CP}" "${MAIN_CLASS}" stage "${CONFIG_FILE}"

K2_CONTAINER="${K2_CONTAINER:-$(read_prop app.docker.k2.container "${CONFIG_FILE}")}"
K2_DB_DEST="${K2_DB_DEST:-$(read_prop app.docker.k2.db.dest "${CONFIG_FILE}")}"

if [[ -n "${K2_CONTAINER}" && -n "${K2_DB_DEST}" ]]; then
  echo "[01_process] Copying staging DB to container ${K2_CONTAINER}:${K2_DB_DEST}"
  docker cp "${SQLITE_DB_PATH}" "${K2_CONTAINER}:${K2_DB_DEST}"
else
  echo "[01_process] Stage complete."
  echo "[01_process] Optional docker copy:"
  echo "  docker cp ${SQLITE_DB_PATH} <k2_container>:<path/in/container>/x12-staging.db"
fi
