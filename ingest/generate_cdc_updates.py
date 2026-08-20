"""Simulador de CDC sem DMS: re-emite eventos existentes como upserts.

Re-emite eventos JA EXISTENTES de um dt com conteudo atualizado (status
vira refunded, occurred_at avanca 1h) -- o formato de chegada de um upsert
vindo de um CDC real (DMS/Debezium).

O ponto da demonstracao e que o pipeline NAO precisa mudar: a deduplicacao
do contrato (janela por event_id ordenada por occurred_at, mantendo o mais
recente) absorve o upsert por construcao.

Rode o bronze_to_silver de novo depois deste script e confira o status
trocado.

Por que nao DMS de verdade: o LocalStack community nao emula DMS. O
desenho completo com DMS -> S3 esta em docs/CDC-DMS.md.

Este script prova a SEMANTICA (upsert por chave + ordenacao temporal), que
e o que o contrato precisa suportar -- a ferramenta de captura e
intercambiavel.

Origem e destino:
    s3://evt-lakehouse-bronze/events/dt={dt}/  (le o batch do dia e grava
    cdc-updates-{hhmmss}.jsonl.gz ao lado)
"""
import argparse
import gzip
import io
import json
from datetime import datetime, timedelta, timezone

import boto3

PROJECT = "evt-lakehouse"


def s3_client(endpoint):
    """Cria o cliente S3 apontado para o endpoint dado (LocalStack ou AWS)."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def sample_events(s3, dt, quantidade):
    """Le os primeiros eventos validos do arquivo batch do dia.

    Sao eles que vao "sofrer update". Validos porque evento quarentenado
    nao chegaria a ter update legitimo na origem.

    Args:
        s3: Cliente S3 de s3_client.
        dt: Dia (YYYY-MM-DD) de onde amostrar.
        quantidade: Maximo de eventos a retornar.

    Returns:
        Lista de eventos com event_id e occurred_at presentes.
    """
    key = f"events/dt={dt}/events-{dt}.jsonl.gz"
    body = s3.get_object(Bucket=f"{PROJECT}-bronze", Key=key)["Body"].read()
    events = []
    with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
        for line in gz:
            event = json.loads(line)
            if event.get("event_id") and event.get("occurred_at"):
                events.append(event)
            if len(events) >= quantidade:
                break
    return events


def main():
    """Gera e grava o lote de upserts CDC do dt pedido."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True)
    parser.add_argument("--quantidade", type=int, default=50)
    parser.add_argument("--endpoint", default="http://localhost:4566")
    args = parser.parse_args()

    s3 = s3_client(args.endpoint)
    updates = []
    for event in sample_events(s3, args.dt, args.quantidade):
        try:
            occurred = datetime.fromisoformat(event["occurred_at"])
        except ValueError:
            continue
        update = dict(event)
        update["status"] = "refunded"
        update["occurred_at"] = (occurred + timedelta(hours=1)).isoformat()
        updates.append(update)

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        for update in updates:
            gz.write((json.dumps(update, ensure_ascii=False) + "\n").encode("utf-8"))

    stamp = datetime.now(timezone.utc).strftime("%H%M%S")
    key = f"events/dt={args.dt}/cdc-updates-{stamp}.jsonl.gz"
    s3.put_object(Bucket=f"{PROJECT}-bronze", Key=key, Body=buffer.getvalue())
    print(
        f"{len(updates)} updates CDC gravados em s3://{PROJECT}-bronze/{key}. "
        f"Reprocesse o silver do dt={args.dt} e confira: os event_ids "
        f"atualizados devem aparecer UMA vez, com status=refunded."
    )


if __name__ == "__main__":
    main()
