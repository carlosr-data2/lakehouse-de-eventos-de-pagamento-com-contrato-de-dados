import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from contract import (
    RAW_SCHEMA,
    apply_contract,
    cast_types,
    deduplicate,
    shape_silver,
    split_valid_quarantine,
)


# SparkSession configurada para falar S3A com o LocalStack. path.style.access
# e obrigatorio; o provider de credencial simples evita busca por metadata.
def build_spark(endpoint: str) -> SparkSession:
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
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True)
    parser.add_argument("--project", default="evt-lakehouse")
    parser.add_argument("--endpoint", default="http://localstack:4566")
    args = parser.parse_args()

    spark = build_spark(args.endpoint)
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

    # Escrita do silver em Parquet particionado. coalesce dimensionado por
    # volume evita o problema de arquivos pequenos.
    target_files = max(1, valid_records // 500000 + 1)
    (
        silver.coalesce(target_files)
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(f"s3a://{p}-silver/events/")
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
