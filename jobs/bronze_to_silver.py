"""Job bronze -> silver: aplica o contrato de dados e roteia a quarentena.

Le a particao do dia da bronze (JSON Lines gzip) e roda o pipeline do
contrato (tipagem -> deduplicacao -> regras -> separacao, ver contract.py).

Escreve tres saidas: o silver modelado, a quarentena com os motivos de
rejeicao e as metricas do estagio para o plano de controle.

Origem:
    s3://{project}-bronze/events/dt={dt}/

Destinos:
    lake.silver.events                     (tabela Iceberg particionada por
        dt; warehouse em s3://{project}-silver/warehouse/)
    s3://{project}-quarantine/events/      (Parquet, com rejection_reasons)
    s3://{project}-artifacts/metrics/silver/dt={dt}/  (JSON lido pela
        Lambda checkpoint_stage do plano de controle)
"""

import argparse
from datetime import datetime, timezone

from contract import (
    RAW_SCHEMA,
    apply_contract,
    cast_types,
    deduplicate,
    shape_silver,
    split_valid_quarantine,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark(endpoint: str, project: str) -> SparkSession:
    """Cria a SparkSession configurada para falar S3A com o LocalStack.

    path.style.access e obrigatorio; o provider de credencial simples evita
    busca por metadata de instancia.

    O catalogo Iceberg "lake" (tipo hadoop, warehouse no proprio bucket
    silver) e quem resolve o nome lake.silver.events; os jars entram por
    --packages no spark-submit (Makefile), nao por codigo.

    Args:
        endpoint: URL do S3 (LocalStack no ciclo local).
        project: Prefixo dos buckets (define o warehouse do catalogo).

    Returns:
        SparkSession pronta para ler e escrever no lake.
    """
    return (
        SparkSession.builder.appName("bronze_to_silver")
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
        # dynamic segue necessario para a QUARENTENA, que continua Parquet
        # particionado (ADR-010): sem ele, o overwrite do dt apagaria as
        # particoes dos outros dias. O silver (Iceberg) nao depende disso.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", f"s3a://{project}-silver/warehouse")
        .getOrCreate()
    )


def main():
    """Executa o estagio silver de um dt: le, valida, separa e mede."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True)
    parser.add_argument("--project", default="evt-lakehouse")
    parser.add_argument("--endpoint", default="http://localstack:4566")
    args = parser.parse_args()

    spark = build_spark(args.endpoint, args.project)
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    p = args.project

    # Leitura da particao do dia com schema fixo. cache porque o DataFrame e
    # usado tres vezes (contagem, valido, quarentena) e recalcular custaria
    # tres leituras completas do S3.
    raw = spark.read.schema(RAW_SCHEMA).json(f"s3a://{p}-bronze/events/dt={args.dt}/")
    raw = raw.cache()
    input_records = raw.count()

    # Pipeline do contrato: tipagem -> deduplicacao -> regras -> separacao.
    typed = cast_types(raw)
    deduped = deduplicate(typed)
    deduped_records = deduped.count()
    checked = apply_contract(deduped)
    valid_df, quarantine_df = split_valid_quarantine(checked)

    silver = shape_silver(valid_df, args.dt, run_ts)
    valid_records = silver.count()
    rejected_records = deduped_records - valid_records

    # Escrita do silver como tabela Iceberg. overwritePartitions substitui
    # apenas as particoes presentes no DataFrame (idempotente como o
    # partitionOverwriteMode dynamic do Parquet), mas num commit ATOMICO:
    # leitor concorrente ve o snapshot anterior ou o novo, nunca dado
    # parcial. coalesce dimensionado por volume evita arquivos pequenos.
    target_files = max(1, valid_records // 500000 + 1)
    shaped = silver.coalesce(target_files)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    if spark.catalog.tableExists("lake.silver.events"):
        shaped.writeTo("lake.silver.events").overwritePartitions()
    else:
        # format-version 2 habilita row-level deletes (merge-on-read) -- a
        # base para um futuro MERGE INTO de upserts de CDC sem reescrever
        # a particao inteira.
        (
            shaped.writeTo("lake.silver.events")
            .partitionedBy(F.col("dt"))
            .tableProperty("format-version", "2")
            .create()
        )

    # Quarentena com o motivo preservado: dado reprovado nao e descartado,
    # e material de evidencia para cobrar correcao na origem.
    (
        quarantine_df.withColumn("_rejected_at", F.lit(run_ts).cast("timestamp"))
        .withColumn("dt", F.lit(args.dt))
        .coalesce(1)
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(f"s3a://{p}-quarantine/events/")
    )

    # Quebra dos motivos de rejeicao: e isso que responde "o que exatamente
    # esta errado?" sem precisar abrir o dado bruto.
    reasons = (
        quarantine_df.select(F.explode("rejection_reasons").alias("reason"))
        .groupBy("reason").count()
        .collect()
    )

    # Publica metricas como JSON no bucket de artifacts. O plano de controle
    # (Lambda checkpoint_stage) le exatamente deste caminho.
    metrics = {
        "stage": "silver",
        "dt": args.dt,
        "input_records": input_records,
        "duplicates_removed": input_records - deduped_records,
        "valid_records": valid_records,
        "rejected_records": rejected_records,
        "reject_rate": round(rejected_records / deduped_records, 4) if deduped_records else 1.0,
        "reasons": {r["reason"]: r["count"] for r in reasons},
        "generated_at": run_ts,
    }
    (
        spark.createDataFrame([metrics])
        .coalesce(1)
        .write.mode("overwrite")
        .json(f"s3a://{p}-artifacts/metrics/silver/dt={args.dt}/")
    )

    print(f"[bronze_to_silver] {metrics}")
    spark.stop()


if __name__ == "__main__":
    main()
