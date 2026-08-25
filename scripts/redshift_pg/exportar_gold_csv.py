"""Exporta a gold merchant_daily do lake para um unico CSV com header.

A ponte entre o mundo colunar (tabela Delta no S3/LocalStack) e o \\copy
do Postgres de serving. Roda dentro do container Spark, igual aos jobs
(ver rodar_gold_pg.sh, que monta /out no host).

Origem:
    s3://{project}-gold/merchant_daily/

Destino:
    /out/merchant_daily_csv/part-*.csv (um arquivo; consumido pelo \\copy)
"""
import argparse

from pyspark.sql import SparkSession


def build_spark(endpoint):
    """Cria a SparkSession com a mesma configuracao S3A dos jobs do lake.

    A gold agora e tabela Delta: o leitor precisa do jar (via --packages no
    rodar_gold_pg.sh) pra resolver o _delta_log em vez de somar parquet de
    versoes antigas.
    """
    return (
        SparkSession.builder.appName("exportar_gold_csv")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main():
    """Le a gold inteira e grava o CSV unico na ordem de colunas da DDL."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="evt-lakehouse")
    parser.add_argument("--endpoint", default="http://localstack:4566")
    parser.add_argument("--saida", default="/out/merchant_daily_csv")
    args = parser.parse_args()

    spark = build_spark(args.endpoint)
    gold = spark.read.format("delta").load(f"s3a://{args.project}-gold/merchant_daily/")

    # coalesce(1): um arquivo so, porque o destino e um \copy sequencial -
    # aqui paralelismo de escrita nao compra nada e complica o COPY.
    # A ordem das colunas segue a DDL (dt e a particao, entao volta a ser
    # coluna comum no select explicito).
    (
        gold.select(
            "dt", "merchant_id", "merchant_name", "category", "state",
            "tx_total", "tx_aprovadas", "clientes_unicos", "taxa_aprovacao",
            "gmv_aprovado", "valor_estornado", "ticket_medio", "share_pix",
            "rank_categoria", "gmv_dia_anterior", "variacao_gmv_pct",
        )
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(args.saida)
    )
    print(f"gold exportada para {args.saida} ({gold.count()} linhas)")
    spark.stop()


if __name__ == "__main__":
    main()
