#!/bin/bash
# Entrypoint for the unitycatalog "duck" test image.
#
# Runs inside the container as user `unitycatalog`, cwd /home/unitycatalog.
# Order matters: the data dir is a bind mount, so anything that touches it must
# happen AFTER the mount (i.e. here at runtime), not at image-build time.
#
#   1. ensure the (possibly freshly-mounted, empty) data dir layout exists
#   2. start the UC server in the background
#   3. wait until it answers
#   4. idempotently seed: catalog `duck` + schemas `duck.managed` / `duck.external`
#   5. hand the foreground back to the server (and forward signals for clean stop)
set -euo pipefail

UC_HOME=/home/unitycatalog
# Table data root. UC records ABSOLUTE file:// locations for tables, so a client
# can only open them if the path resolves in its own filesystem namespace:
#   - client INSIDE the container: default $UC_HOME/etc/data is fine.
#   - client on the HOST (e.g. the duckdb unittest binary): run.sh sets
#     DUCK_UC_DATA_DIR to a path bind-mounted IDENTICALLY on host and container,
#     so an absolute path means the same files on both sides.
# Under the root: duck/managed/ (MANAGED tables; UC nests __unitystorage here) and
# duck/external/ (plain EXTERNAL tables). H2 metastore stays at $UC_HOME/etc/db.
DATA_DIR="${DUCK_UC_DATA_DIR:-$UC_HOME/etc/data}"
DUCK_DIR="$DATA_DIR/duck"
MANAGED_ROOT="$DUCK_DIR/managed"
EXTERNAL_ROOT="$DUCK_DIR/external"

cd "$UC_HOME"

# 1. Layout. The bind mount may shadow the image's dirs with an empty host dir.
mkdir -p "$MANAGED_ROOT" "$EXTERNAL_ROOT"

# 1b. Optional S3-compatible (e.g. MinIO) endpoint for the CLI's Hadoop S3A writer.
# Only when DUCK_UC_S3_ENDPOINT is set do we write a core-site.xml (the conf dir is
# already on the classpath from the image build). Unset => local FS / AWS S3 with
# the default endpoint. Static keys here cover EXTERNAL tables (no UC vending);
# MANAGED tables additionally need s3.* vending config in server.properties.
if [ -n "${DUCK_UC_S3_ENDPOINT:-}" ]; then
  cat > "$UC_HOME/conf/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.s3a.endpoint</name><value>${DUCK_UC_S3_ENDPOINT}</value></property>
  <property><name>fs.s3a.endpoint.region</name><value>${DUCK_UC_S3_REGION:-us-east-1}</value></property>
  <property><name>fs.s3a.path.style.access</name><value>true</value></property>
  <property><name>fs.s3a.connection.ssl.enabled</name><value>${DUCK_UC_S3_SSL:-false}</value></property>
  <property><name>fs.s3a.access.key</name><value>${DUCK_UC_S3_KEY:-}</value></property>
  <property><name>fs.s3a.secret.key</name><value>${DUCK_UC_S3_SECRET:-}</value></property>
  <property><name>fs.s3a.aws.credentials.provider</name><value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value></property>
</configuration>
EOF
  echo "duck-entrypoint: wrote core-site.xml for S3 endpoint $DUCK_UC_S3_ENDPOINT"
else
  rm -f "$UC_HOME/conf/core-site.xml"
fi

# 2. Start server in background.
./bin/start-uc-server &
SERVER_PID=$!

# Forward termination so `docker stop` shuts the JVM down cleanly.
term() { kill -TERM "$SERVER_PID" 2>/dev/null || true; }
trap term TERM INT

# 3. Wait for readiness by polling a cheap, auth-free CLI call.
echo "duck-entrypoint: waiting for UC server..."
ready=
for _ in $(seq 1 90); do
  if ./bin/uc catalog list >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "duck-entrypoint: server exited before becoming ready" >&2
    wait "$SERVER_PID"
    exit 1
  fi
  sleep 1
done
if [ -z "$ready" ]; then
  echo "duck-entrypoint: server did not become ready in time" >&2
  term
  exit 1
fi

# 4. Idempotent seed. `get` first so a restart against persisted state is a no-op.
uc() { ./bin/uc "$@"; }

if ! uc catalog get --name duck >/dev/null 2>&1; then
  uc catalog create --name duck --comment "DuckDB test catalog (managed)"
  echo "duck-entrypoint: created catalog 'duck'"
fi

# managed: MANAGED tables (always catalog-managed in this build). Its storage_root
# is duck/managed, so UC allocates per-table locations (and its __unitystorage
# tree) underneath it.
if ! uc schema get --full_name duck.managed >/dev/null 2>&1; then
  uc schema create --catalog duck --name managed \
    --comment "catalog-managed (MANAGED) Delta tables" \
    --storage_root "$MANAGED_ROOT"
  echo "duck-entrypoint: created schema 'duck.managed' -> $MANAGED_ROOT"
fi

# external: EXTERNAL Delta tables (plain log, not catalog-managed). No managed
# storage_root -- each external table brings its own location, which uctl.sh
# places under $EXTERNAL_ROOT (a sibling of duck/managed, so it neither sits under
# nor above managed storage -> no external-location registration needed).
if ! uc schema get --full_name duck.external >/dev/null 2>&1; then
  uc schema create --catalog duck --name external \
    --comment "plain (EXTERNAL) Delta tables"
  echo "duck-entrypoint: created schema 'duck.external' (external tables under $EXTERNAL_ROOT)"
fi

echo "duck-entrypoint: ready. catalog 'duck' with schemas 'managed' and 'external'."

# 5. Hand off to the server in the foreground.
wait "$SERVER_PID"
