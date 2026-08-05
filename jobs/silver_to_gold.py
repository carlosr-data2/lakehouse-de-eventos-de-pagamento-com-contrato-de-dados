import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession


# Mesma configuracao S3A do estagio anterior. AQE ligado para coalescer
# particoes de shuffle automaticamente apos as agregacoes.
def build_spark(endpoint: str) -> SparkSession:
    return (
        SparkSession.builder.appName("silver_to_gold")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# Janela deslizante: le apenas o dt alvo e os N dias anteriores. Ler o silver
# inteiro daria o mesmo resultado com custo crescendo junto com o historico.
def window_dates(dt: str, lookback: int):
    base = datetime.fromisoformat(dt).date()
    return [(base - timedelta(days=i)).isoformat() for i in range(lookback + 1)]


# SQL analitico com CTEs. Escolha consciente: agregacao de negocio em SQL
# (auditavel pela area de negocio), transformacao tecnica em DataFrame API.
GOLD_SQL = """
WITH base AS (
    SELECT
        dt,
        merchant_id,
        customer_id,
        payment_method,
        status,
        amount
    FROM silver_events
    WHERE currency = 'BRL'
),
-- Agregacao por estabelecimento/dia com metricas condicionais.
-- FILTER (WHERE ...) e mais legivel e mais rapido que CASE dentro do SUM.
agg AS (
    SELECT
        dt,
        merchant_id,
        COUNT(*)                                                   AS tx_total,
        COUNT(*) FILTER (WHERE status = 'approved')                AS tx_aprovadas,
        SUM(amount) FILTER (WHERE status = 'approved')             AS gmv_aprovado,
        SUM(amount) FILTER (WHERE status = 'refunded')             AS valor_estornado,
        SUM(amount) FILTER (WHERE payment_method = 'pix'
                              AND status = 'approved')             AS gmv_pix,
        COUNT(DISTINCT customer_id)                                AS clientes_unicos
    FROM base
    GROUP BY dt, merchant_id
),
-- Enriquecimento com a dimensao. BROADCAST explicito: 300 linhas contra
-- centenas de milhares de eventos - o shuffle e desnecessario.
enriched AS (
    SELECT /*+ BROADCAST(m) */
        a.*,
        m.merchant_name,
        m.category,
        m.state
    FROM agg a
    LEFT JOIN dim_merchants m ON a.merchant_id = m.merchant_id
),
-- Funcoes de janela: ranking dentro da categoria no dia e comparacao com o
-- dia anterior do mesmo estabelecimento (LAG sobre particao por merchant).
final AS (
    SELECT
        dt,
        merchant_id,
        merchant_name,
        category,
        state,
        tx_total,
        tx_aprovadas,
        clientes_unicos,
        ROUND(tx_aprovadas / NULLIF(tx_total, 0), 4)                  AS taxa_aprovacao,
        COALESCE(gmv_aprovado, 0)                                     AS gmv_aprovado,
        COALESCE(valor_estornado, 0)                                  AS valor_estornado,
        ROUND(COALESCE(gmv_aprovado, 0) / NULLIF(tx_aprovadas, 0), 2) AS ticket_medio,
        ROUND(COALESCE(gmv_pix, 0) / NULLIF(gmv_aprovado, 0), 4)      AS share_pix,
        DENSE_RANK() OVER (
            PARTITION BY dt, category ORDER BY COALESCE(gmv_aprovado, 0) DESC
        )                                                             AS rank_categoria,
        LAG(COALESCE(gmv_aprovado, 0)) OVER (
            PARTITION BY merchant_id ORDER BY dt
        )                                                             AS gmv_dia_anterior,
        ROUND(
            (COALESCE(gmv_aprovado, 0) - LAG(COALESCE(gmv_aprovado, 0))
                OVER (PARTITION BY merchant_id ORDER BY dt))
            / NULLIF(LAG(COALESCE(gmv_aprovado, 0))
                OVER (PARTITION BY merchant_id ORDER BY dt), 0),
            4
        )                                                             AS variacao_gmv_pct
    FROM enriched
)
SELECT * FROM final WHERE dt = '{target_dt}'
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True)
    parser.add_argument("--lookback", type=int, default=2)
    parser.add_argument("--project", default="evt-lakehouse")
    parser.add_argument("--endpoint", default="http://localstack:4566")
    args = parser.parse_args()

    spark = build_spark(args.endpoint)
    p = args.project
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    # Le apenas as particoes da janela que existem. basePath preserva a
    # coluna dt como coluna de particao ao ler caminhos especificos.
    dates = window_dates(args.dt, args.lookback)
    paths = [f"s3a://{p}-silver/events/dt={d}/" for d in dates]
    silver = (
        spark.read.option("basePath", f"s3a://{p}-silver/events/")
        .parquet(*paths)
    )
    silver.createOrReplaceTempView("silver_events")

    # Dimensao pequena lida do proprio bucket gold, alvo do broadcast join.
    dim = (
        spark.read.option("header", "true")
        .csv(f"s3a://{p}-gold/dim_merchants/merchants.csv")
    )
    dim.createOrReplaceTempView("dim_merchants")

    gold = spark.sql(GOLD_SQL.format(target_dt=args.dt))

    # repartition por dt antes da escrita: um arquivo por particao em vez de
    # dezenas de fragmentos herdados do shuffle das janelas.
    (
        gold.repartition("dt")
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(f"s3a://{p}-gold/merchant_daily/")
    )

    # Metricas do estagio, no mesmo padrao do silver, para o plano de controle.
    total = gold.count()
    metrics = {
        "stage": "gold",
        "dt": args.dt,
        "input_records": total,
        "valid_records": total,
        "rejected_records": 0,
        "merchants": gold.select("merchant_id").distinct().count(),
        "generated_at": run_ts,
    }
    (
        spark.createDataFrame([metrics])
        .coalesce(1)
        .write.mode("overwrite")
        .json(f"s3a://{p}-artifacts/metrics/gold/dt={args.dt}/")
    )

    print(f"[silver_to_gold] {metrics}")
    spark.stop()


if __name__ == "__main__":
    main()
