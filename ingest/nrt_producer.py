# Producer near real time: envia eventos (com os mesmos defeitos do gerador
# batch) para o Kinesis Data Stream; o Firehose entrega na bronze sob
# events_nrt/dt=.../ em micro lotes de ate 60s. Reusa clean_event/corrupt do
# gerador batch de proposito - NRT e batch sao caminhos de ENTREGA
# diferentes do mesmo dado, nao dados diferentes.
#
# A PartitionKey e o merchant_id - a mesma chave de negocio dos jobs. Com um
# estabelecimento dominante isso concentraria trafego num shard so: o skew
# do Passo 8, na versao streaming (hot shard). A escolha esta comentada
# aqui porque ela e uma decisao, nao um default.
import argparse
import json
import random
import time
from datetime import datetime, timezone

import boto3
from generate_events import clean_event, corrupt

PROJECT = "evt-lakehouse"


def kinesis_client(endpoint):
    return boto3.client(
        "kinesis",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eventos", type=int, default=200)
    parser.add_argument("--intervalo", type=float, default=0.05,
                        help="segundos entre envios (simula chegada continua)")
    parser.add_argument("--endpoint", default="http://localhost:4566")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    kinesis = kinesis_client(args.endpoint)
    merchant_ids = [f"mer_{i:05d}" for i in range(1, 301)]
    dt = datetime.now(timezone.utc).date().isoformat()

    for i in range(args.eventos):
        event = corrupt(clean_event(rng, dt, merchant_ids), rng)
        kinesis.put_record(
            StreamName=f"{PROJECT}-events-nrt",
            # newline no fim: o Firehose concatena records no objeto S3, e
            # sem o delimitador o JSON Lines resultante seria ilegivel.
            Data=(json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"),
            PartitionKey=event["merchant_id"],
        )
        if args.intervalo:
            time.sleep(args.intervalo)

    print(
        f"{args.eventos} eventos enviados ao stream {PROJECT}-events-nrt; "
        f"o Firehose entrega em s3://{PROJECT}-bronze/events_nrt/dt={dt}/ "
        f"em ate 60s (buffer)."
    )


if __name__ == "__main__":
    main()
