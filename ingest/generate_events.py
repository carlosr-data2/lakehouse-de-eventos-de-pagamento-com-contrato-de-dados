"""Gerador de eventos sinteticos de pagamento para a camada bronze.

Fabrica os dias de eventos com defeitos injetados de proposito (cada
defeito mapeia para uma regra do contrato aplicado no bronze_to_silver) e
duplicatas exatas (entrega "at least once" de broker), alem da dimensao de
estabelecimentos usada no broadcast join da gold. Seed fixa por padrao:
mesmos argumentos geram os mesmos eventos, o que torna reprocessos e
comparacoes deterministicos.

Destinos:
    s3://evt-lakehouse-bronze/events/dt={dt}/events-{dt}.jsonl.gz
    s3://evt-lakehouse-gold/dim_merchants/merchants.csv
"""

import argparse
import csv
import gzip
import io
import json
import random
import uuid
from datetime import datetime, timedelta, timezone

import boto3

PROJECT = "evt-lakehouse"
CURRENCIES = ["BRL", "BRL", "BRL", "USD", "EUR"]
METHODS = ["credit_card", "debit_card", "pix", "boleto"]
STATUSES = ["approved", "approved", "approved", "declined", "refunded"]
CHANNELS = ["app", "web", "pos"]
CATEGORIES = ["varejo", "alimentacao", "servicos", "viagem", "saude"]


def s3_client(endpoint):
    """Cria o cliente S3 apontado para o endpoint dado.

    Endpoint parametrizavel para que o mesmo script rode contra o
    LocalStack ou a AWS real trocando um argumento.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def clean_event(rng, dt, merchant_ids):
    """Gera um evento valido "limpo", base sobre a qual defeitos sao injetados.

    Args:
        rng: Gerador random.Random com seed controlada.
        dt: Dia (YYYY-MM-DD) dentro do qual o occurred_at e sorteado.
        merchant_ids: Universo de estabelecimentos possiveis.

    Returns:
        Dict no formato de chegada da bronze (RAW_SCHEMA do contrato).
    """
    base = datetime.fromisoformat(dt).replace(tzinfo=timezone.utc)
    occurred = base + timedelta(seconds=rng.randint(0, 86399))
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "occurred_at": occurred.isoformat(),
        "customer_id": f"cus_{rng.randint(1, 4000):06d}",
        "merchant_id": rng.choice(merchant_ids),
        "payment_method": rng.choice(METHODS),
        "currency": rng.choice(CURRENCIES),
        "amount_cents": rng.randint(500, 900000),
        "status": rng.choice(STATUSES),
        "channel": rng.choice(CHANNELS),
    }


def corrupt(event, rng):
    """Injeta no evento, com probabilidade fixa, um defeito de cada familia.

    Cada defeito mapeia para uma regra do contrato de dados
    (amount_invalido, moeda_fora_dominio, timestamp_invalido,
    customer_id_nulo); a maioria dos eventos sai intacta.

    Args:
        event: Evento gerado por clean_event (mutado in place).
        rng: Gerador random.Random com seed controlada.

    Returns:
        O mesmo evento, possivelmente corrompido.
    """
    roll = rng.random()
    if roll < 0.020:
        event["amount_cents"] = rng.choice([None, -rng.randint(100, 5000)])
    elif roll < 0.032:
        event["currency"] = rng.choice(["BR", "xxx", ""])
    elif roll < 0.042:
        event["occurred_at"] = "31/02/2026 99:99"
    elif roll < 0.050:
        event["customer_id"] = None
    return event


def build_day(rng, dt, merchant_ids, volume):
    """Monta a lista completa de eventos de um dia.

    Eventos limpos, corrompidos e ~3% de duplicatas exatas embaralhadas
    (simulando a entrega "at least once" de um broker) -- e o que exercita
    a deduplicacao do contrato.

    Args:
        rng: Gerador random.Random com seed controlada.
        dt: Dia (YYYY-MM-DD) sendo gerado.
        merchant_ids: Universo de estabelecimentos possiveis.
        volume: Quantidade de eventos antes das duplicatas.

    Returns:
        Lista de eventos do dia, em ordem aleatoria.
    """
    events = [corrupt(clean_event(rng, dt, merchant_ids), rng) for _ in range(volume)]
    duplicates = [dict(e) for e in rng.sample(events, k=int(volume * 0.03))]
    events.extend(duplicates)
    rng.shuffle(events)
    return events


def upload_day(s3, dt, events):
    """Serializa o dia em JSON Lines + gzip em memoria e envia num PutObject.

    Bronze guarda o formato de chegada, sem conversao colunar -- a
    conversao para Parquet e papel do silver.

    Args:
        s3: Cliente S3 de s3_client.
        dt: Dia (YYYY-MM-DD) sendo gravado.
        events: Lista de eventos de build_day.

    Returns:
        Tupla (chave do objeto, total de eventos gravados).
    """
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        for event in events:
            gz.write((json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"))
    key = f"events/dt={dt}/events-{dt}.jsonl.gz"
    s3.put_object(Bucket=f"{PROJECT}-bronze", Key=key, Body=buffer.getvalue())
    return key, len(events)


def upload_merchants(s3, merchant_ids, rng):
    """Gera e envia a dimensao de estabelecimentos (CSV com header).

    Tabela pequena, ideal para o broadcast join na camada gold. Vai direto
    para o bucket gold -- e dado de consumo analitico, nao artefato de
    deploy nem evento bruto, entao nem bronze nem artifacts fazem sentido.

    Args:
        s3: Cliente S3 de s3_client.
        merchant_ids: Universo de estabelecimentos.
        rng: Gerador random.Random com seed controlada.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["merchant_id", "merchant_name", "category", "state"])
    for mid in merchant_ids:
        writer.writerow([
            mid,
            f"Estabelecimento {mid[-4:]}",
            rng.choice(CATEGORIES),
            rng.choice(["SP", "RJ", "MG", "RS", "PE", "BA"]),
        ])
    s3.put_object(
        Bucket=f"{PROJECT}-gold",
        Key="dim_merchants/merchants.csv",
        Body=buffer.getvalue().encode("utf-8"),
    )


def main():
    """Gera a dimensao e um arquivo de eventos por data pedida."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dates", nargs="+", required=True)
    parser.add_argument("--volume", type=int, default=40000)
    parser.add_argument("--endpoint", default="http://localhost:4566")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    s3 = s3_client(args.endpoint)
    merchant_ids = [f"mer_{i:05d}" for i in range(1, 301)]

    upload_merchants(s3, merchant_ids, rng)
    for dt in args.dates:
        key, total = upload_day(s3, dt, build_day(rng, dt, merchant_ids, args.volume))
        print(f"dt={dt} -> {total} eventos gravados em {key}")


if __name__ == "__main__":
    main()
