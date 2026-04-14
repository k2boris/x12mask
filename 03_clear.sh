#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE="${1:-application.properties}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

MAIN_CLASS="com.k2view.x12mask.Main"
JAVA_CP="${JAVA_CP:-src:jars/*}"
SQLITE_DB_PATH="${SQLITE_DB_PATH:-connector/x12-staging.db}"
TRACE_DIR="${TRACE_DIR:-data/trace}"
OUT_DIR="${OUT_DIR:-data/out}"

read_prop() {
  local key="$1"
  local file="$2"
  local line
  line="$(grep -E "^${key}=" "${file}" | tail -n 1 || true)"
  echo "${line#*=}"
}

if [[ -z "${SQLITE_DB_PATH}" ]]; then
  SQLITE_JDBC_URL="$(read_prop app.sqlite.jdbc.url "${CONFIG_FILE}")"
  SQLITE_DB_PATH="${SQLITE_JDBC_URL#jdbc:sqlite:}"
fi

echo "[03_clear] Full cleanup: reset SQLite staging DB"
rm -f "${SQLITE_DB_PATH}"

# Recreate empty SQLite DB + schema using existing Java clear path.
java -cp "${JAVA_CP}" "${MAIN_CLASS}" clear "__RESET__" "${CONFIG_FILE}"

echo "[03_clear] Full cleanup: removing local trace/output artifacts"
rm -rf "${TRACE_DIR}" "${OUT_DIR}"
mkdir -p "${TRACE_DIR}" "${OUT_DIR}"

K2_CONTAINER="${K2_CONTAINER:-$(read_prop app.docker.k2.container "${CONFIG_FILE}")}"
K2_DB_DEST="${K2_DB_DEST:-$(read_prop app.docker.k2.db.dest "${CONFIG_FILE}")}"

if [[ -n "${K2_CONTAINER}" && -n "${K2_DB_DEST}" ]]; then
  echo "[03_clear] Copying staging DB to container ${K2_CONTAINER}:${K2_DB_DEST}"
  docker cp "${SQLITE_DB_PATH}" "${K2_CONTAINER}:${K2_DB_DEST}"
else
  echo "[03_clear] Clear complete."
  echo "[03_clear] Optional docker copy:"
  echo "  docker cp ${SQLITE_DB_PATH} <k2_container>:<path/in/container>/x12-staging.db"
fi
