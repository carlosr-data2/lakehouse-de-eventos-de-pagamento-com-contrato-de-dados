"""Testes unitarios do contrato de dados: "a LOGICA do contrato esta certa?"

Suite isolada de infra de proposito: nada aqui toca S3, LocalStack ou
pipeline (quem responde "a infra sobe?" e o smoke test; "o dado de HOJE
esta ok?" e a quarentena + quality gate, em runtime).

Os dados de teste nao vem do lake: cada teste FABRICA em memoria as 1-2
linhas de que precisa (ver _row/raw_df) -- minimas, cirurgicas e
deterministicas.

Dado real e pessimo pra testar logica: grande (lento), aleatorio (o
cenario pode nem existir nele) e instavel (muda sem o codigo mudar).

Rodar local (make test) e o ciclo de feedback de quem desenvolve; o CI
roda a MESMA suite a cada push, numa maquina limpa -- um itera rapido, o
outro e o portao do repositorio. Um nao substitui o outro.

Nada aqui exige Docker: pip install pyspark ja embute o Spark inteiro
(basta uma JVM na maquina) -- o estagio unit do CI roda exatamente assim.

E nada exige Glue/EMR: os jobs sao PySpark puro sem GlueContext (ADR-005),
entao a logica e testavel em qualquer Python com Java.
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
    split_valid_quarantine,
)


@pytest.fixture(scope="session")
def spark():
    """SparkSession local reutilizada por toda a sessao de teste.

    Criar e destruir por teste tornaria a suite lenta demais para o CI.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def raw_df(spark, rows):
    """Monta o DataFrame cru a partir de dicionarios, todo string.

    Igual ao que sai do RAW_SCHEMA. createDataFrame constroi tudo na
    memoria do Spark local -- e por isso que a suite roda sem nenhuma
    infraestrutura de pe.
    """
    cols = ["event_id", "occurred_at", "customer_id", "merchant_id",
            "payment_method", "currency", "amount_cents", "status", "channel"]
    return spark.createDataFrame(
        [tuple(str(r[c]) if r[c] is not None else None for c in cols) for r in rows],
        schema=cols,
    )


def _row(**kwargs):
    """A "linha perfeita": um evento valido em todos os campos.

    Cada teste sabota SO o campo que quer exercitar
    (_row(amount_cents="abc")) -- assim fica obvio qual regra causou o
    resultado, e o test_registro_valido_passa ganha de graca o seu caso
    base.

    Padrao arrange-act-assert: monta o dado, aplica a funcao real, confere
    a saida.
    """
    base = {
        "event_id": "e1", "occurred_at": "2026-07-03T10:00:00", "customer_id": "cus_1",
        "merchant_id": "mer_1", "payment_method": "pix", "currency": "BRL",
        "amount_cents": "1000", "status": "approved", "channel": "app",
    }
    base.update(kwargs)
    return base


def test_cast_invalido_vira_nulo(spark):
    """Valor nao numerico vira NULL, nao excecao (try_cast + amount_invalido)."""
    df = cast_types(raw_df(spark, [_row(amount_cents="abc", occurred_at="31/02/2026")]))
    row = df.select("amount_cents_typed", "occurred_at_typed").first()
    assert row["amount_cents_typed"] is None
    assert row["occurred_at_typed"] is None


def test_deduplicacao_mantem_mais_recente(spark):
    """Duplicata pelo mesmo event_id mantem o mais recente, deterministico."""
    rows = [
        _row(event_id="e1", occurred_at="2026-07-03T08:00:00", status="declined"),
        _row(event_id="e1", occurred_at="2026-07-03T09:00:00", status="approved"),
    ]
    result = deduplicate(cast_types(raw_df(spark, rows)))
    assert result.count() == 1
    assert result.first()["status"] == "approved"


def test_multiplos_motivos_de_rejeicao(spark):
    """Registro que viola tres regras acumula os tres motivos."""
    rows = [_row(amount_cents="-5", currency="xxx", status="unknown")]
    df = apply_contract(cast_types(raw_df(spark, rows)))
    reasons = set(df.first()["rejection_reasons"])
    assert reasons == {"amount_invalido", "moeda_fora_dominio", "status_fora_dominio"}


def test_registro_valido_passa(spark):
    """Registro limpo nao pode ser rejeitado.

    Protege contra regra agressiva demais, que e o erro mais caro num
    contrato de dados.
    """
    df = apply_contract(cast_types(raw_df(spark, [_row()])))
    valid, quarantine = split_valid_quarantine(df)
    assert valid.count() == 1
    assert quarantine.count() == 0
