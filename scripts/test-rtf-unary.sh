#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET="${GRPC_HOST:-orders.muleaceacademy.com:443}"
ORDER_ID="${ORDER_ID:-ORD-1042}"
METHOD="orders.v1.OrderTrackingService/GetOrderStatus"

resolve_protoset() {
  local candidate=""

  if [[ -n "${PROTOSET:-}" ]]; then
    if [[ -f "${PROTOSET}" ]]; then
      printf '%s\n' "${PROTOSET}"
      return 0
    fi

    if [[ -f "${PROJECT_ROOT}/${PROTOSET}" ]]; then
      printf '%s\n' "${PROJECT_ROOT}/${PROTOSET}"
      return 0
    fi

    return 1
  fi

  for dir in \
    "${PROJECT_ROOT}/src/main/resources/grpc" \
    "${PROJECT_ROOT}/mule-app/src/main/resources/grpc"; do

    if [[ -d "${dir}" ]]; then
      candidate="$(find "${dir}" -type f -name "*.protobin" -print 2>/dev/null | head -n 1)"
      if [[ -n "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
      fi
    fi
  done

  return 1
}

command -v grpcurl >/dev/null 2>&1 || {
  echo "Error: grpcurl is not installed or not available in PATH." >&2
  exit 1
}

if ! PROTOSET_PATH="$(resolve_protoset)"; then
  echo "Error: No .protobin file found." >&2
  exit 1
fi

REQUEST_JSON="${REQUEST_JSON:-}"
if [[ -z "${REQUEST_JSON}" ]]; then
  REQUEST_JSON="{\"order_id\":\"${ORDER_ID}\"}"
fi

TLS_ARGS=()

if [[ "${GRPC_INSECURE:-false}" == "true" ]]; then
  echo "Warning: TLS certificate validation is disabled." >&2
  TLS_ARGS=(-insecure)
elif [[ -n "${GRPC_CA_CERT:-}" ]]; then
  [[ -f "${GRPC_CA_CERT}" ]] || {
    echo "Error: GRPC_CA_CERT file not found: ${GRPC_CA_CERT}" >&2
    exit 1
  }
  TLS_ARGS=(-cacert "${GRPC_CA_CERT}")
fi

HEADER_ARGS=()
if [[ -n "${GRPC_AUTH_TOKEN:-}" ]]; then
  HEADER_ARGS=(-H "authorization: Bearer ${GRPC_AUTH_TOKEN}")
fi

echo "Calling Runtime Fabric unary RPC: ${METHOD}"
echo "Target: ${TARGET}"
echo "Protoset: ${PROTOSET_PATH}"

exec grpcurl -vv \
  "${TLS_ARGS[@]}" \
  -protoset "${PROTOSET_PATH}" \
  "${HEADER_ARGS[@]}" \
  -d "${REQUEST_JSON}" \
  "${TARGET}" \
  "${METHOD}"
