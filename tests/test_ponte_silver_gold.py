"""Teste de PONTE silver->gold: o unico da suite que ATRAVESSA camadas.

Os unitarios por camada tem um ponto cego deliberado: o do contrato
fabrica entrada crua e para no silver; o da gold fabrica um silver
proprio e para na gold. Renomear uma coluna em shape_silver deixa os
DOIS verdes -- e quebra o job real em producao.

Este teste liga as pontas: passa dado fabricado PELO contrato e entrega
o resultado real dele ao GOLD_SQL.

Deriva de schema entre as camadas falha AQUI, em segundos, nao no
pipeline rodando de madrugada.

(Experimento que motivou o teste: mude o alias "amount" em shape_silver
e rode a suite -- so este teste cai, com UNRESOLVED_COLUMN apontando a
coluna que a gold consome e o contrato parou de entregar.)
"""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from contract import (
    apply_contract,
    cast_types,
    deduplicate,
    shape_silver,
    split_valid_quarantine,
)
from silver_to_gold import GOLD_SQL

TARGET = "2026-07-02"
ANTERIOR = "2026-07-01"


@pytest.fixture(scope="session")
def spark():
    """SparkSession local reutilizada por toda a sessao de teste."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests-ponte")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


# Mesmo padrao do test_contract: linha crua perfeita, cada caso sabota so
# o que quer exercitar. Duplicado de proposito -- arquivos de teste nao se
# importam entre si; a clareza local vale a repeticao.
COLS = ["event_id", "occurred_at", "customer_id", "merchant_id",
        "payment_method", "currency", "amount_cents", "status", "channel"]


def _evento(**kwargs):
    """Linha crua perfeita; cada caso sabota so o campo que quer exercitar."""
    base = {
        "event_id": "e1", "occurred_at": f"{TARGET}T10:00:00", "customer_id": "cus_1",
        "merchant_id": "m1", "payment_method": "pix", "currency": "BRL",
        "amount_cents": "10000", "status": "approved", "channel": "app",
    }
    base.update(kwargs)
    return base


def raw_df(spark, rows):
    """Monta o DataFrame cru todo string, como no RAW_SCHEMA do contrato."""
    return spark.createDataFrame(
        [tuple(str(r[c]) if r[c] is not None else None for c in COLS) for r in rows],
        schema=COLS,
    )


def test_o_que_o_contrato_entrega_e_o_que_a_gold_consome(spark):
    """Roda contrato -> shape_silver -> GOLD_SQL de ponta a ponta em memoria.

    Dois dias fabricados atravessam o contrato real; o silver resultante
    alimenta o GOLD_SQL real. Quarentena, agregados, LAG e variacao sao
    conferidos no resultado final -- deriva de schema entre camadas falha
    aqui.
    """
    dias = {
        ANTERIOR: [
            _evento(event_id="a1", occurred_at=f"{ANTERIOR}T09:00:00"),
        ],
        TARGET: [
            _evento(event_id="b1", customer_id="cus_1"),
            _evento(event_id="b2", customer_id="cus_2", amount_cents="20000",
                    payment_method="card"),
            # invalido: nao pode chegar na gold -- prova que a quarentena
            # tambem atravessa a ponte do jeito certo
            _evento(event_id="b3", currency="xxx", amount_cents="99999"),
        ],
    }

    silvers = []
    for dt, rows in dias.items():
        checked = apply_contract(deduplicate(cast_types(raw_df(spark, rows))))
        valid, _quarentena = split_valid_quarantine(checked)
        silvers.append(shape_silver(valid, dt, f"{TARGET}T12:00:00"))
    silver = silvers[0].unionByName(silvers[1])
    silver.createOrReplaceTempView("silver_events")

    spark.createDataFrame(
        [("m1", "Loja Um", "varejo", "SP")],
        "merchant_id STRING, merchant_name STRING, category STRING, state STRING",
    ).createOrReplaceTempView("dim_merchants")

    gold = spark.sql(GOLD_SQL.format(target_dt=TARGET)).collect()
    assert len(gold) == 1
    m1 = gold[0].asDict()

    # o evento invalido (b3) ficou na quarentena: 2 transacoes, nao 3
    assert m1["tx_total"] == 2
    assert m1["clientes_unicos"] == 2
    # amount derivado no contrato (centavos/100) somado pela gold
    assert float(m1["gmv_aprovado"]) == pytest.approx(300.0)
    # LAG contra o dia anterior que tambem veio do contrato
    assert float(m1["gmv_dia_anterior"]) == pytest.approx(100.0)
    assert float(m1["variacao_gmv_pct"]) == pytest.approx(2.0)
