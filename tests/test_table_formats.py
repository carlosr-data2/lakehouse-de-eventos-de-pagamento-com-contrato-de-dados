"""Testes das garantias de formato: Iceberg (silver) e Delta (gold).

O test_contract prova a LOGICA do pipeline; aqui provamos as GARANTIAS
dos formatos de tabela de que os jobs dependem:

* re-execucao de um dt substitui a particao sem duplicar nem tocar as
  vizinhas (overwritePartitions no Iceberg, replaceWhere no Delta);
* cada escrita e um commit com historico -- time travel le o estado
  anterior;
* schema evolution no Iceberg: coluna nova entra por ALTER TABLE, dado
  antigo continua legivel (a coluna vem nula).

Mesma filosofia da suite: dado fabricado em memoria, sem S3 e sem
LocalStack -- o warehouse Iceberg e um diretorio temporario e o Delta
escreve em tmp local. Os jars entram por spark.jars.packages no
conftest.py, os mesmos que o spark-submit dos jobs recebe por --packages.
"""
import tempfile

import pytest
from pyspark.sql import functions as F

SCHEMA = "dt string, merchant_id string, amount double"
TABELA = "lake.silver.eventos_lab"


def _eventos(spark, dt, linhas):
    """Fabrica um DataFrame minimo de eventos de um unico dt."""
    return spark.createDataFrame([(dt, m, v) for m, v in linhas], SCHEMA)


@pytest.fixture(scope="module")
def tabela_iceberg(spark):
    """Tabela Iceberg com tres commits, no formato usado pelo job silver.

    create + 2x overwritePartitions do mesmo dt -- exatamente o ciclo de
    um run e um re-run do bronze_to_silver.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql(f"DROP TABLE IF EXISTS {TABELA}")
    (
        _eventos(spark, "2026-07-01", [("m1", 10.0), ("m2", 20.0)])
        .writeTo(TABELA)
        .partitionedBy(F.col("dt"))
        .tableProperty("format-version", "2")
        .create()
    )
    valores_errados = [("m1", 999.0), ("m2", 999.0)]
    _eventos(spark, "2026-07-02", valores_errados).writeTo(TABELA).overwritePartitions()
    # re-execucao do dt=02 com o dado corrigido: e isto que nao pode duplicar
    _eventos(spark, "2026-07-02", [("m1", 5.0)]).writeTo(TABELA).overwritePartitions()
    return TABELA


def test_iceberg_overwrite_de_particao_e_idempotente(spark, tabela_iceberg):
    df = spark.table(tabela_iceberg)
    # o re-run substituiu a particao inteira (1 linha, nao 2+1)...
    assert df.filter("dt = '2026-07-02'").count() == 1
    assert df.filter("dt = '2026-07-02'").first()["amount"] == 5.0
    # ...e a particao vizinha ficou intacta
    assert df.filter("dt = '2026-07-01'").count() == 2


def test_iceberg_historico_de_snapshots_e_time_travel(spark, tabela_iceberg):
    snaps = spark.sql(
        f"SELECT snapshot_id FROM {tabela_iceberg}.snapshots ORDER BY committed_at"
    ).collect()
    # um commit por escrita: create + 2 overwrites
    assert len(snaps) >= 3
    primeiro = snaps[0]["snapshot_id"]
    antes = spark.sql(f"SELECT * FROM {tabela_iceberg} VERSION AS OF {primeiro}")
    # o primeiro snapshot so tinha o dt=01: o overwrite nao reescreveu o passado
    assert antes.count() == 2
    assert antes.filter("dt = '2026-07-02'").count() == 0


def test_iceberg_schema_evolution_sem_reescrever_a_tabela(spark, tabela_iceberg):
    spark.sql(f"ALTER TABLE {tabela_iceberg} ADD COLUMN canal STRING")
    nova = spark.createDataFrame(
        [("2026-07-03", "m9", 1.0, "pix")], SCHEMA + ", canal string"
    )
    nova.writeTo(tabela_iceberg).append()
    df = spark.table(tabela_iceberg)
    assert "canal" in df.columns
    # dado antigo continua legivel; a coluna nova vem nula, sem rewrite
    antigos = df.filter("dt = '2026-07-01'").select("canal").collect()
    assert antigos and all(r["canal"] is None for r in antigos)
    assert df.filter("dt = '2026-07-03'").first()["canal"] == "pix"


@pytest.fixture(scope="module")
def caminho_delta(spark):
    """Tabela Delta com tres versoes, no formato usado pelo job gold.

    Cada escrita usa replaceWhere do proprio dt -- o ciclo de dois runs e
    um re-run do silver_to_gold.
    """
    caminho = tempfile.mkdtemp(prefix="gold-delta-") + "/merchant_daily"

    def escrever(dt, linhas):
        (
            _eventos(spark, dt, linhas)
            .write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"dt = '{dt}'")
            .partitionBy("dt")
            .save(caminho)
        )

    escrever("2026-07-01", [("m1", 100.0), ("m2", 200.0)])   # versao 0
    escrever("2026-07-02", [("m1", 999.0), ("m2", 999.0)])   # versao 1
    escrever("2026-07-02", [("m1", 50.0)])                   # versao 2 (re-run)
    return caminho


def test_delta_replace_where_e_idempotente(spark, caminho_delta):
    df = spark.read.format("delta").load(caminho_delta)
    # o re-run substituiu so a particao do predicado...
    assert df.filter("dt = '2026-07-02'").count() == 1
    assert df.filter("dt = '2026-07-02'").first()["amount"] == 50.0
    # ...sem tocar a vizinha
    assert df.filter("dt = '2026-07-01'").count() == 2


def test_delta_time_travel_por_versao(spark, caminho_delta):
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(caminho_delta)
    # a versao 0 so conhecia o dt=01 -- cada replaceWhere virou um commit novo
    assert v0.count() == 2
    assert v0.filter("dt = '2026-07-02'").count() == 0
