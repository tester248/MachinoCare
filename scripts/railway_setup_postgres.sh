#!/usr/bin/env bash
set -euo pipefail

# Usage (minimum):
#   railway login
#   RAILWAY_PROJECT_ID=<project-id> RAILWAY_BACKEND_SERVICE=<backend-service-name> ./scripts/railway_setup_postgres.sh
#
# Optional env vars:
#   RAILWAY_ENVIRONMENT=<environment-name>
#   RAILWAY_POSTGRES_SERVICE=<service-name> (default: machinocare-postgres)
#   RAILWAY_SKIP_CREATE_DB=1  (skip creating postgres service)

POSTGRES_SERVICE="${RAILWAY_POSTGRES_SERVICE:-machinocare-postgres}"
PROJECT_ID="${RAILWAY_PROJECT_ID:-}"
ENVIRONMENT="${RAILWAY_ENVIRONMENT:-}"
BACKEND_SERVICE="${RAILWAY_BACKEND_SERVICE:-}"
SKIP_CREATE_DB="${RAILWAY_SKIP_CREATE_DB:-0}"

if ! command -v railway >/dev/null 2>&1; then
  echo "railway CLI not found. Install with: npm install -g @railway/cli" >&2
  exit 1
fi

if ! railway whoami >/dev/null 2>&1; then
  echo "Railway CLI is not authenticated. Run: railway login" >&2
  exit 1
fi

link_args=()
if [[ -n "$PROJECT_ID" ]]; then
  link_args+=(--project "$PROJECT_ID")
fi
if [[ -n "$ENVIRONMENT" ]]; then
  link_args+=(--environment "$ENVIRONMENT")
fi

if [[ ${#link_args[@]} -gt 0 ]]; then
  echo "Linking workspace to Railway project/environment..."
  railway link "${link_args[@]}" >/dev/null
fi

if [[ "$SKIP_CREATE_DB" != "1" ]]; then
  echo "Ensuring PostgreSQL service exists: $POSTGRES_SERVICE"
  if ! railway add --database postgres --service "$POSTGRES_SERVICE" --json >/tmp/railway_add_postgres.json 2>/tmp/railway_add_postgres.err; then
    echo "PostgreSQL service create returned non-zero; checking if it already exists or requires manual action."
    cat /tmp/railway_add_postgres.err || true
  else
    echo "PostgreSQL service creation output:"
    cat /tmp/railway_add_postgres.json || true
  fi
fi

if [[ -z "$BACKEND_SERVICE" ]]; then
  echo "RAILWAY_BACKEND_SERVICE not provided."
  echo "Set it to your backend service name, then rerun to auto-wire DATABASE_URL and app variables."
  railway status || true
  exit 0
fi

database_ref="\${{${POSTGRES_SERVICE}.DATABASE_URL}}"

echo "Setting backend variables on service: $BACKEND_SERVICE"
railway variable set -s "$BACKEND_SERVICE" \
  "DATABASE_URL=${database_ref}" \
  "MACHINOCARE_DEBUG_RETENTION_DAYS=30" \
  "MACHINOCARE_DEBUG_SAMPLE_RATE=0.10" \
  "MACHINOCARE_LIVE_PUSH_INTERVAL_SECONDS=0.75"

echo "Railway status after configuration:"
railway status || true

echo "Done. Trigger deploy if needed: railway up"
