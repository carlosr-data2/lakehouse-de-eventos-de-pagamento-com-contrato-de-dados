#!/usr/bin/env bash
# Materializa a gold num Postgres local e roda as consultas de validacao +
# EXPLAIN ANALYZE - o exercicio que transforma "desenhei a DDL do Redshift"
# em "executei a DDL adaptada e li o plano". Redshift descende do Postgres,
# entao a sintaxe SQL e compativel; o que NAO se traduz (DISTKEY/SORTKEY/
# COPY de Parquet) esta comentado em ddl_postgres.sql.
#
# Uso: bash scripts/redshift_pg/rodar_gold_pg.sh [dt]   (default: 2026-07-02)
# Pre-requisito: gold ja gerada (Passo 5) e LocalStack de pe.
set -euo pipefail

DIA="${1:-2026-07-02}"
AQUI="$(cd "$(dirname "$0")" && pwd)"
RAIZ="$(cd "$AQUI/../.." && pwd)"
PG_CONTAINER="gold-pg"

# 1) Postgres efemero (some com o container; rode de novo quando quiser).
if ! docker ps --format '{{.Names}}' | grep -q "^${PG_CONTAINER}$"; then
  docker run --rm -d --name "$PG_CONTAINER" \
    -e POSTGRES_PASSWORD=gold -p 5433:5432 postgres:16
  echo "aguardando o Postgres aceitar conexoes..."
  until docker exec "$PG_CONTAINER" pg_isready -U postgres -q; do sleep 1; done
fi

# 2) Exporta a gold do lake para CSV (mesma imagem Spark dos jobs).
docker run --rm --network lakehouse-net --user root \
  -v "$RAIZ/scripts/redshift_pg:/opt/pg" -v "$RAIZ/.out:/out" \
  -v "$RAIZ/.ivy:/root/.ivy2" \
  bitnami/spark:3.5.1 spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/pg/exportar_gold_csv.py --endpoint http://localstack:4566

# 3) DDL adaptada + carga via \copy (o COPY do pobre, e do Postgres).
docker exec -i "$PG_CONTAINER" psql -U postgres -q < "$AQUI/ddl_postgres.sql"
cat "$RAIZ"/.out/merchant_daily_csv/part-*.csv | docker exec -i "$PG_CONTAINER" \
  psql -U postgres -q -c "\\copy analytics.merchant_daily FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"

# 4) ANALYZE (identico ao Redshift: estatistica pro planejador) + consultas.
docker exec -i "$PG_CONTAINER" psql -U postgres -q -c "ANALYZE analytics.merchant_daily;"
docker exec -i "$PG_CONTAINER" psql -U postgres -v dia="$DIA" < "$AQUI/consultas_pg.sql"

echo
echo "OK. Para explorar interativamente: docker exec -it $PG_CONTAINER psql -U postgres"
echo "Para encerrar: docker stop $PG_CONTAINER   (container efemero, --rm)"
