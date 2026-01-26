#!/bin/bash
MB_ADMIN_EMAIL="${MB_ADMIN_EMAIL:-admin@crypto.local}"
MB_ADMIN_PASSWORD="${MB_ADMIN_PASSWORD:-CryptoAdmin_2026!}"
MB_ADMIN_FIRST_NAME="${MB_ADMIN_FIRST_NAME:-Admin}"
MB_ADMIN_LAST_NAME="${MB_ADMIN_LAST_NAME:-Crypto}"
MB_SITE_NAME="${MB_SITE_NAME:-Crypto Analytics}"
MB_SITE_LOCALE="${MB_SITE_LOCALE:-en}"

CLICKHOUSE_NAME="${CLICKHOUSE_NAME:-ClickHouse Crypto}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-crypto}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-admin}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-admin}"

echo "Waiting for Metabase..."
while ! curl -s http://metabase:3000/api/health | grep -q '"status":"ok"'; do
    sleep 5
done
echo "Metabase is ready"

SETUP_TOKEN=$(curl -s http://metabase:3000/api/session/properties | grep -o '"setup-token":"[^"]*"' | sed 's/"setup-token":"//;s/"//')

if [ -n "$SETUP_TOKEN" ]; then
    echo "First run - creating user..."
    curl -s -X POST http://metabase:3000/api/setup \
      -H "Content-Type: application/json" \
      -d @- <<EOF
{
  "token": "$SETUP_TOKEN",
  "user": {
    "email": "$MB_ADMIN_EMAIL",
    "password": "$MB_ADMIN_PASSWORD",
    "first_name": "$MB_ADMIN_FIRST_NAME",
    "last_name": "$MB_ADMIN_LAST_NAME",
    "site_name": "$MB_SITE_NAME"
  },
  "prefs": {
    "site_name": "$MB_SITE_NAME",
    "site_locale": "$MB_SITE_LOCALE"
  }
}
EOF
    echo ""
    echo "User created!"

    echo "Logging in..."
    SESSION=$(curl -s -X POST http://metabase:3000/api/session \
      -H "Content-Type: application/json" \
      -d "{\"username\":\"$MB_ADMIN_EMAIL\",\"password\":\"$MB_ADMIN_PASSWORD\"}" | grep -o '"id":"[^"]*"' | sed 's/"id":"//;s/"//')

    echo "Adding ClickHouse database..."
    curl -s -X POST http://metabase:3000/api/database \
      -H "Content-Type: application/json" \
      -H "X-Metabase-Session: $SESSION" \
      -d @- <<EOF
{
  "engine": "clickhouse",
  "name": "$CLICKHOUSE_NAME",
  "details": {
    "host": "$CLICKHOUSE_HOST",
    "port": $CLICKHOUSE_PORT,
    "dbname": "$CLICKHOUSE_DB",
    "user": "$CLICKHOUSE_USER",
    "password": "$CLICKHOUSE_PASSWORD"
  }
}
EOF
    echo ""
    echo "Setup complete!"
else
    echo "Metabase already configured, skipping setup"
fi