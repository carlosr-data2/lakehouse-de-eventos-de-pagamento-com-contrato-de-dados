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


# Cliente S3 apontado para o LocalStack. Endpoint parametrizavel para que o
# mesmo script rode contra AWS real trocando um argumento.
def s3_client(endpoint):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


# Evento valido "limpo". Base sobre a qual os defeitos serao injetados.
def clean_event(rng, dt, merchant_ids):
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


# Injeta defeitos com probabilidade fixa. Cada defeito mapeia para uma regra
# do contrato de dados que sera aplicada no passo 4.
def corrupt(event, rng):
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


# Monta a lista completa do dia: eventos limpos, corrompidos e duplicatas
# exatas (simulando entrega "at least once" de um broker).
def build_day(rng, dt, merchant_ids, volume):
    events = [corrupt(clean_event(rng, dt, merchant_ids), rng) for _ in range(volume)]
    duplicates = [dict(e) for e in rng.sample(events, k=int(volume * 0.03))]
    events.extend(duplicates)
    rng.shuffle(events)
    return events


# Serializa em JSON Lines + gzip em memoria e envia num unico PutObject.
# Bronze guarda o formato de chegada, sem conversao colunar.
def upload_day(s3, dt, events):
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        for event in events:
            gz.write((json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"))
    key = f"events/dt={dt}/events-{dt}.jsonl.gz"
    s3.put_object(Bucket=f"{PROJECT}-bronze", Key=key, Body=buffer.getvalue())
    return key, len(events)


# Dimensao de estabelecimentos: tabela pequena, ideal para broadcast join
# na camada gold. Vai direto para o bucket gold -- e dado de consumo
# analitico, nao artefato de deploy nem evento bruto, entao nem bronze
# nem artifacts fazem sentido aqui.
def upload_merchants(s3, merchant_ids, rng):
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
