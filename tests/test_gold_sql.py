"""Testes unitarios do GOLD_SQL: cada construcao do SQL analitico da gold.

Guardam o GOLD_SQL desde ANTES do passo que executa o job: unitario testa
CODIGO com dado fabricado, nao o estado do lake -- rodam verdes sem gold
gerada, sem silver, sem LocalStack.

E rodam JUNTO com os do contrato porque a suite e do repositorio, nao do
passo: pytest tests varre a pasta inteira -- a rede de regressao que
confere, a cada mudanca, tambem o que voce "nao tocou" (um ajuste no
contrato pode mudar coluna que a gold consome).

Filtre com -k ou por arquivo pra iterar; feche o trabalho com a suite
inteira.
"""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from silver_to_gold import GOLD_SQL

TARGET = "2026-07-02"
ANTERIOR = "2026-07-01"


@pytest.fixture(scope="session")
def spark():
    """SparkSession local reutilizada por toda a sessao de teste."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests-gold")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def silver_view(spark):
    """Cria a view silver_events com o cenario minimo do GOLD_SQL.

    Exercita cada construcao: FILTER condicional, ranking por categoria,
    LAG por merchant e o filtro de moeda.

    m1 e m2 disputam a mesma categoria; m3 esta sozinho em outra; m1 tem
    historico no dia anterior (LAG preenchido); a linha USD nao pode
    contar.
    """
    rows = [
        # dt, merchant, customer, metodo, status, amount, moeda
        (ANTERIOR, "m1", "c01", "card", "approved", 100.0, "BRL"),
        (TARGET, "m1", "c02", "card", "approved", 100.0, "BRL"),
        (TARGET, "m1", "c03", "card", "approved", 100.0, "BRL"),
        (TARGET, "m1", "c04", "card", "approved", 100.0, "BRL"),
        (TARGET, "m1", "c05", "card", "approved", 999.0, "USD"),
        (TARGET, "m2", "c06", "card", "approved", 100.0, "BRL"),
        (TARGET, "m2", "c07", "card", "approved", 100.0, "BRL"),
        (TARGET, "m2", "c08", "card", "refunded", 50.0, "BRL"),
        (TARGET, "m3", "c09", "pix", "approved", 100.0, "BRL"),
    ]
    schema = (
        "dt STRING, merchant_id STRING, customer_id STRING, "
        "payment_method STRING, status STRING, amount DOUBLE, currency STRING"
    )
    spark.createDataFrame(rows, schema).createOrReplaceTempView("silver_events")


def dim_view(spark):
    """Cria a view dim_merchants minima para o broadcast join da gold."""
    rows = [
        ("m1", "Loja Um", "varejo", "SP"),
        ("m2", "Loja Dois", "varejo", "RJ"),
        ("m3", "Servico Tres", "servicos", "MG"),
    ]
    schema = "merchant_id STRING, merchant_name STRING, category STRING, state STRING"
    spark.createDataFrame(rows, schema).createOrReplaceTempView("dim_merchants")


@pytest.fixture(scope="session")
def gold(spark):
    """Executa o GOLD_SQL real sobre as views fabricadas, indexado por merchant."""
    silver_view(spark)
    dim_view(spark)
    result = spark.sql(GOLD_SQL.format(target_dt=TARGET)).collect()
    return {row["merchant_id"]: row.asDict() for row in result}


def test_apenas_dt_alvo_e_moeda_brl(gold):
    """So os merchants do dt alvo saem; a linha USD nao cria merchant extra."""
    assert set(gold) == {"m1", "m2", "m3"}
    assert gold["m1"]["gmv_aprovado"] == 300.0  # USD 999 fora


def test_agregados_e_estorno(gold):
    """Contagens, GMV, estorno, taxa e ticket batem para o merchant m2."""
    m2 = gold["m2"]
    assert m2["tx_total"] == 3
    assert m2["tx_aprovadas"] == 2
    assert m2["gmv_aprovado"] == 200.0
    assert m2["valor_estornado"] == 50.0
    assert m2["taxa_aprovacao"] == pytest.approx(0.6667)
    assert m2["ticket_medio"] == 100.0


def test_rank_por_categoria(gold):
    """DENSE_RANK por (dt, category): m1 e m2 disputam varejo; m3 e rank 1 sozinho."""
    assert gold["m1"]["rank_categoria"] == 1
    assert gold["m2"]["rank_categoria"] == 2
    assert gold["m3"]["rank_categoria"] == 1


def test_variacao_contra_dia_anterior(gold):
    """LAG por merchant: historico preenchido para m1, nulo para estreante.

    m1 tem historico (variacao (300-100)/100 = 2.0); m2 estreou hoje,
    entao dia anterior e variacao ficam nulos -- e nulo aqui e o
    comportamento certo, nao zero (zero mentiria "estavel").
    """
    m1 = gold["m1"]
    assert m1["gmv_dia_anterior"] == 100.0
    assert m1["variacao_gmv_pct"] == pytest.approx(2.0)
    assert gold["m2"]["gmv_dia_anterior"] is None
    assert gold["m2"]["variacao_gmv_pct"] is None


def test_share_pix(gold):
    """share_pix e a fracao do GMV aprovado que entrou via pix."""
    assert gold["m3"]["share_pix"] == pytest.approx(1.0)
    assert gold["m1"]["share_pix"] == pytest.approx(0.0)
