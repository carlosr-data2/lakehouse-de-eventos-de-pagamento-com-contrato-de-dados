"""Fixture unica de SparkSession para toda a suite.

Uma JVM so por sessao de pytest: spark.jars.packages e classpath sao
decididos na PRIMEIRA SparkSession do processo, entao a fixture que
carrega Iceberg + Delta precisa ser a unica -- por isso ela mora aqui e
nao em cada arquivo de teste.

Os mesmos jars que o spark-submit dos jobs recebe por --packages
(Makefile) entram aqui por spark.jars.packages: a suite prova as
garantias de formato com o MESMO runtime que roda o pipeline.

O warehouse do catalogo Iceberg e um diretorio temporario local -- nada
aqui toca S3 ou LocalStack, igual ao resto da suite.
"""
import sys
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

# Mesmas coordenadas do PACKAGES do Makefile (menos hadoop-aws: sem S3 aqui).
ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
DELTA = "io.delta:delta-spark_2.12:3.2.0"


@pytest.fixture(scope="session")
def spark():
    """SparkSession local reutilizada por toda a sessao de teste.

    Criar e destruir por teste tornaria a suite lenta demais para o CI.
    """
    warehouse = tempfile.mkdtemp(prefix="lake-warehouse-")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", f"{ICEBERG},{DELTA}")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", warehouse)
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    yield session
    session.stop()
