"""Contrato de dados bronze -> silver: as regras que separam valido de rejeitado.

Funcoes puras de DataFrame -> DataFrame, sem I/O e sem SparkSession propria:
quem le e escreve e o job (bronze_to_silver.py), o que permite testar cada
regra com dado fabricado em memoria (tests/test_contract.py).

Ordem de aplicacao no job: cast_types -> deduplicate -> apply_contract ->
split_valid_quarantine -> shape_silver.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

VALID_CURRENCIES = ["BRL", "USD", "EUR"]
VALID_STATUSES = ["approved", "declined", "refunded"]

# Schema explicito, tudo string. Evita inferencia (custosa e instavel entre
# dias) e permite que valor malformado vire nulo controlado no cast.
RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("occurred_at", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("amount_cents", StringType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
])


def cast_types(df: DataFrame) -> DataFrame:
    """Converte amount_cents e occurred_at para tipos fortes, tolerando lixo.

    Usa try_cast via expressao SQL (a funcao so existe no modulo Python a
    partir do PySpark 4): valor invalido vira NULL em vez de derrubar o job.

    O NULL resultante e capturado depois pelas regras de apply_contract.

    Args:
        df: DataFrame bruto com as colunas string do RAW_SCHEMA.

    Returns:
        DataFrame com as colunas derivadas amount_cents_typed (long) e
        occurred_at_typed (timestamp).
    """
    return (
        df.withColumn("amount_cents_typed", F.expr("try_cast(amount_cents AS long)"))
          .withColumn("occurred_at_typed", F.to_timestamp("occurred_at"))
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove duplicatas de event_id de forma deterministica.

    Entre eventos com o mesmo id, mantem o de occurred_at mais recente
    (janela por event_id ordenada por occurred_at_typed desc);
    dropDuplicates manteria um registro arbitrario.

    E essa ordenacao temporal que tambem absorve o upsert do CDC sem
    mudanca no pipeline (ver ingest/generate_cdc_updates.py).

    Args:
        df: DataFrame ja tipado por cast_types.

    Returns:
        DataFrame com uma linha por event_id.
    """
    window = Window.partitionBy("event_id").orderBy(
        F.col("occurred_at_typed").desc_nulls_last()
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def apply_contract(df: DataFrame) -> DataFrame:
    """Avalia as seis regras do contrato e anota os motivos de rejeicao.

    Acumula TODOS os motivos violados num array: guardar so o primeiro
    destruiria informacao de diagnostico -- quem conserta a origem precisa
    da lista completa.

    Args:
        df: DataFrame tipado (e, no pipeline, ja deduplicado).

    Returns:
        DataFrame com a coluna rejection_reasons (array vazio = valido).
    """
    reasons = F.array_compact(F.array(
        F.when(F.col("event_id").isNull(), F.lit("event_id_nulo")),
        F.when(F.col("customer_id").isNull(), F.lit("customer_id_nulo")),
        F.when(
            F.col("amount_cents_typed").isNull() | (F.col("amount_cents_typed") <= 0),
            F.lit("amount_invalido"),
        ),
        F.when(~F.col("currency").isin(VALID_CURRENCIES), F.lit("moeda_fora_dominio")),
        F.when(F.col("occurred_at_typed").isNull(), F.lit("timestamp_invalido")),
        F.when(~F.col("status").isin(VALID_STATUSES), F.lit("status_fora_dominio")),
    ))
    return df.withColumn("rejection_reasons", reasons)


def split_valid_quarantine(df: DataFrame):
    """Separa aprovados de reprovados pelo array de motivos.

    Nada e descartado: o rejeitado vai para a quarentena com o motivo,
    para que a origem possa ser cobrada com evidencia.

    Args:
        df: DataFrame com a coluna rejection_reasons de apply_contract.

    Returns:
        Tupla (validos, quarentena), nesta ordem.
    """
    valid = df.filter(F.size("rejection_reasons") == 0)
    quarantine = df.filter(F.size("rejection_reasons") > 0)
    return valid, quarantine


def shape_silver(df: DataFrame, dt: str, run_ts: str) -> DataFrame:
    """Modela o registro final do silver.

    Nomes de negocio, tipos corretos, colunas derivadas uteis (amount em
    decimal, occurred_hour) e metadados tecnicos de linhagem (_ingested_at,
    _source_job e a coluna de particao dt).

    Args:
        df: DataFrame de eventos validos.
        dt: Particao (YYYY-MM-DD) sendo processada.
        run_ts: Timestamp ISO da execucao, gravado em _ingested_at.

    Returns:
        DataFrame pronto para a escrita particionada por dt.
    """
    return df.select(
        F.col("event_id"),
        F.col("occurred_at_typed").alias("occurred_at"),
        F.col("customer_id"),
        F.col("merchant_id"),
        F.col("payment_method"),
        F.col("currency"),
        F.col("amount_cents_typed").alias("amount_cents"),
        (F.col("amount_cents_typed") / 100).cast("decimal(18,2)").alias("amount"),
        F.col("status"),
        F.col("channel"),
        F.hour("occurred_at_typed").alias("occurred_hour"),
        F.lit(run_ts).cast("timestamp").alias("_ingested_at"),
        F.lit("bronze_to_silver").alias("_source_job"),
        F.lit(dt).alias("dt"),
    )
