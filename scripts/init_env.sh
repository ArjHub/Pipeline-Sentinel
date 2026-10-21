#!/usr/bin/env bash
# Create .env from .env.example with fresh random secrets. Never overwrites an existing .env.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  echo ".env already exists; leaving it alone"
  exit 0
fi

rand() { python3 -c "import secrets; print(secrets.token_urlsafe(24))"; }
fernet() { python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"; }

cp .env.example .env
# Portable in-place edit (BSD and GNU sed differ on -i).
tmp=$(mktemp)
sed \
  -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$(rand)|" \
  -e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$(fernet)|" \
  -e "s|^AIRFLOW_SECRET_KEY=.*|AIRFLOW_SECRET_KEY=$(rand)|" \
  -e "s|^AIRFLOW_ADMIN_PASSWORD=.*|AIRFLOW_ADMIN_PASSWORD=$(rand)|" \
  -e "s|^GRAFANA_ADMIN_PASSWORD=.*|GRAFANA_ADMIN_PASSWORD=$(rand)|" \
  -e "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$(id -u)|" \
  .env > "$tmp"
mv "$tmp" .env
chmod 600 .env
echo "created .env with random secrets (see AIRFLOW_ADMIN_PASSWORD for the Airflow UI login)"
