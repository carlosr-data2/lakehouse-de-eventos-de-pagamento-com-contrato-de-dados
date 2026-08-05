import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from contract import (
    apply_contract,
    cast_types,
    deduplicate,
    split_valid_quarantine,
)


# SparkSession local reutilizada por toda a sessao de teste. Criar e destruir
# por teste tornaria a suite lenta demais para rodar no CI.
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


# Helper que monta o DataFrame cru a partir de dicionarios, ja com todos os
# campos como string (igual ao que sai do RAW_SCHEMA).
def raw_df(spark, rows):
    cols = ["event_id", "occurred_at", "customer_id", "merchant_id",
            "payment_method", "currency", "amount_cents", "status", "channel"]
    return spark.createDataFrame(
        [tuple(str(r[c]) if r[c] is not None else None for c in cols) for r in rows],
        schema=cols,
    )


def _row(**kwargs):
    base = {
        "event_id": "e1", "occurred_at": "2026-07-03T10:00:00", "customer_id": "cus_1",
        "merchant_id": "mer_1", "payment_method": "pix", "currency": "BRL",
        "amount_cents": "1000", "status": "approved", "channel": "app",
    }
    base.update(kwargs)
    return base


# Valor nao numerico deve virar NULL, nao excecao. E o contrato entre
# try_cast e a regra amount_invalido.
def test_cast_invalido_vira_nulo(spark):
    df = cast_types(raw_df(spark, [_row(amount_cents="abc", occurred_at="31/02/2026")]))
    row = df.select("amount_cents_typed", "occurred_at_typed").first()
    assert row["amount_cents_typed"] is None
    assert row["occurred_at_typed"] is None


# Duplicata pelo mesmo event_id deve manter o registro mais recente,
# de forma deterministica.
def test_deduplicacao_mantem_mais_recente(spark):
    rows = [
        _row(event_id="e1", occurred_at="2026-07-03T08:00:00", status="declined"),
        _row(event_id="e1", occurred_at="2026-07-03T09:00:00", status="approved"),
    ]
    result = deduplicate(cast_types(raw_df(spark, rows)))
    assert result.count() == 1
    assert result.first()["status"] == "approved"


# Um registro que viola tres regras deve acumular os tres motivos.
def test_multiplos_motivos_de_rejeicao(spark):
    rows = [_row(amount_cents="-5", currency="xxx", status="unknown")]
    df = apply_contract(cast_types(raw_df(spark, rows)))
    reasons = set(df.first()["rejection_reasons"])
    assert reasons == {"amount_invalido", "moeda_fora_dominio", "status_fora_dominio"}


# Registro limpo nao pode ser rejeitado (protege contra regra agressiva
# demais, que e o erro mais caro num contrato de dados).
def test_registro_valido_passa(spark):
    df = apply_contract(cast_types(raw_df(spark, [_row()])))
    valid, quarantine = split_valid_quarantine(df)
    assert valid.count() == 1
    assert quarantine.count() == 0
