FROM postgres:16

WORKDIR /app

COPY . .

RUN apt-get update \
	&& apt-get install -y --no-install-recommends python3 \
	&& rm -rf /var/lib/apt/lists/*

RUN cat <<'EOF' >/usr/local/bin/run-psql-scripts.sh \
	&& chmod +x /usr/local/bin/run-psql-scripts.sh
#!/bin/sh
set -eu

HOST="${POSTGRES_HOST:-postgres}"
PORT="${POSTGRES_PORT:-5432}"
DB="${POSTGRES_DB:-cit444}"
USER="${POSTGRES_USER:-cit444}"
export PGPASSWORD="${POSTGRES_PASSWORD:-cit444}"

SQL_DIR="/app/database"
if [ ! -d "$SQL_DIR" ]; then
	echo "No database directory found at $SQL_DIR"
	exit 1
fi

for sql_file in \
	"$SQL_DIR/schema_postgres.sql" \
	"$SQL_DIR/hotel_insertion.sql" \
	"$SQL_DIR/processed_reviews.sql" \
	"$SQL_DIR/ratings_insertion.sql"
do
	[ -f "$sql_file" ] || continue
	[ -z "$sql_file" ] && continue
	echo "========================================"
	echo "Running $(basename "$sql_file")"
	echo "========================================"
	psql "host=$HOST port=$PORT dbname=$DB user=$USER" -v ON_ERROR_STOP=1 -f "$sql_file"
done
EOF

CMD ["/usr/local/bin/run-psql-scripts.sh"]