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


# Conversao tipada com try_cast: valor invalido vira NULL em vez de derrubar
# o job. O NULL resultante e capturado depois pelas regras do contrato.
def cast_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("amount_cents_typed", F.col("amount_cents").try_cast("long"))
          .withColumn("occurred_at_typed", F.to_timestamp("occurred_at"))
    )


# Deduplicacao deterministica: entre eventos com o mesmo id, mantem o de
# occurred_at mais recente. dropDuplicates manteria um registro arbitrario.
def deduplicate(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("event_id").orderBy(
        F.col("occurred_at_typed").desc_nulls_last()
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


# Aplica as seis regras e acumula TODOS os motivos de rejeicao num array.
# Guardar so o primeiro motivo destruiria informacao de diagnostico.
def apply_contract(df: DataFrame) -> DataFrame:
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


# Separa aprovados de reprovados. Nada e descartado: o rejeitado vai para a
# quarentena com o motivo, para que a origem possa ser cobrada com evidencia.
def split_valid_quarantine(df: DataFrame):
    valid = df.filter(F.size("rejection_reasons") == 0)
    quarantine = df.filter(F.size("rejection_reasons") > 0)
    return valid, quarantine


# Modelagem final do silver: nomes de negocio, tipos corretos, colunas
# derivadas uteis e metadados tecnicos de linhagem.
def shape_silver(df: DataFrame, dt: str, run_ts: str) -> DataFrame:
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
