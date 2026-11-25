#!/usr/bin/env bash
set -euo pipefail

certs=$(mktemp -d)
container=
cleanup() {
  if [[ -n "$container" ]]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  rm -rf "$certs"
}
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=RBMQ Test CA" \
  -keyout "$certs/ca.key" -out "$certs/ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
  -keyout "$certs/server.key" -out "$certs/server.csr" >/dev/null 2>&1
printf 'subjectAltName = DNS:localhost\n' > "$certs/extensions"
openssl x509 -req -days 1 -in "$certs/server.csr" \
  -CA "$certs/ca.crt" -CAkey "$certs/ca.key" -CAcreateserial \
  -extfile "$certs/extensions" -out "$certs/server.crt" >/dev/null 2>&1

container=$(docker run -d --user "$(id -u):$(id -g)" -p 127.0.0.1::6379 \
  --mount "type=bind,source=$certs,target=/tls,readonly" "${REDIS_TEST_IMAGE:-redis:8}" \
  redis-server --port 0 --tls-port 6379 --tls-auth-clients no \
  --tls-cert-file /tls/server.crt --tls-key-file /tls/server.key \
  --tls-ca-cert-file /tls/ca.crt --save "")
port=$(docker port "$container" 6379/tcp | cut -d: -f2)

ready() {
  docker exec "$container" redis-cli -h localhost --tls --cacert /tls/ca.crt ping 2>/dev/null | grep -q '^PONG$'
}
for _ in {1..20}; do
  if ready; then break; fi
  sleep 1
done
if ! ready; then
  docker logs "$container"
  exit 1
fi

export SSL_CERT_FILE="$certs/ca.crt" RBMQ_TLS_TEST_PORT="$port"
for feature in tokio-tls smol-tls; do
  cargo test --no-default-features --features "$feature" --test tls -- --ignored
done
