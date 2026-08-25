"""Job silver -> gold: o agregado diario merchant_daily.

Le a janela deslizante do silver (o dt alvo + N dias anteriores, o
historico de que as funcoes de janela precisam).

Junta a dimensao de estabelecimentos via broadcast e materializa o
agregado de negocio do dia com o SQL analitico de GOLD_SQL (CTEs, FILTER,
ranking e LAG).

Origens:
    lake.silver.events                      (tabela Iceberg; janela de
        lookback+1 dias via pruning de particao)
    s3://{project}-gold/dim_merchants/merchants.csv

Destinos:
    s3://{project}-gold/merchant_daily/     (tabela Delta particionada por
        dt, sobrescrita idempotente via replaceWhere)
    s3://{project}-artifacts/metrics/gold/dt={dt}/  (JSON, plano de controle)
"""

import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark(endpoint: str, project: str) -> SparkSession:
    """Cria a SparkSession com a mesma configuracao S3A do estagio anterior.

    AQE com coalescePartitions ligado para coalescer particoes de shuffle
    automaticamente apos as agregacoes.

    Este estagio fala os DOIS formatos: le o silver pelo catalogo Iceberg
    "lake" (mesma configuracao do estagio anterior) e escreve o gold em
    Delta -- dai as duas extensions e o DeltaCatalog no spark_catalog.

    Args:
        endpoint: URL do S3 (LocalStack no ciclo local).
        project: Prefixo dos buckets (define o warehouse do catalogo).

    Returns:
        SparkSession pronta para ler e escrever no lake.
    """
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
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", f"s3a://{project}-silver/warehouse")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def window_dates(dt: str, lookback: int):
    """Lista as datas da janela deslizante: o dt alvo e os N anteriores.

    O job le apenas essa janela porque o LAG por merchant do GOLD_SQL so
    precisa do historico imediato.

    Ler o silver inteiro daria o mesmo resultado com custo crescendo junto
    com o historico.

    Args:
        dt: Data alvo no formato YYYY-MM-DD.
        lookback: Quantos dias anteriores incluir na leitura.

    Returns:
        Lista de datas ISO, da mais recente para a mais antiga.
    """
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
    """Executa o estagio gold de um dt: janela, broadcast, SQL e escrita."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True)
    parser.add_argument("--lookback", type=int, default=2)
    parser.add_argument("--project", default="evt-lakehouse")
    parser.add_argument("--endpoint", default="http://localstack:4566")
    args = parser.parse_args()

    spark = build_spark(args.endpoint, args.project)
    p = args.project
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    # Le a tabela Iceberg filtrando a janela: o pruning de particao do
    # formato resolve o que antes exigia montar a lista de caminhos na mao
    # (e quebrava se uma particao da janela nao existisse no S3).
    dates = window_dates(args.dt, args.lookback)
    silver = spark.read.table("lake.silver.events").where(F.col("dt").isin(dates))
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
    #
    # Delta com replaceWhere: sobrescreve APENAS a particao do dt alvo, em
    # transacao registrada no _delta_log -- re-execucao do mesmo dt nao
    # duplica nem apaga vizinhos. O caminho fisico do merchant_daily nao
    # muda em relacao ao Parquet; o log de transacoes passa a morar junto.
    (
        gold.repartition("dt")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"dt = '{args.dt}'")
        .partitionBy("dt")
        .save(f"s3a://{p}-gold/merchant_daily/")
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
