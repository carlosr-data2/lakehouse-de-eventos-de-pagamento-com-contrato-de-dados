import json
import os
from datetime import datetime, timedelta, timezone

import boto3

PROJECT = os.environ["PROJECT"]
METRICS_TABLE = os.environ["METRICS_TABLE"]
MAX_REJECT_RATE = float(os.environ.get("MAX_REJECT_RATE", "0.05"))
REGION = os.environ.get("AWS_REGION", "us-east-1")

# Prefixo de saida de cada estagio. Fonte unica de verdade sobre onde cada
# camada escreve - o Spark usa exatamente os mesmos caminhos.
STAGE_OUTPUT = {
    "silver": (f"{PROJECT}-silver", "events"),
    "gold": (f"{PROJECT}-gold", "merchant_daily"),
}


# Erros de dominio com nome proprio: o Step Functions faz Retry/Catch pelo
# nome da classe, entao errar aqui quebra a politica de resiliencia.
class LandingEmptyError(Exception):
    pass


class StageOutputMissingError(Exception):
    pass


# Dentro do LocalStack a Lambda enxerga o emulador pelo hostname injetado
# em LOCALSTACK_HOSTNAME. Na AWS real a variavel nao existe e o boto3
# resolve o endpoint padrao - o mesmo codigo serve para os dois ambientes.
def _client(service):
    host = os.environ.get("LOCALSTACK_HOSTNAME")
    if host:
        return boto3.client(service, endpoint_url=f"http://{host}:4566", region_name=REGION)
    return boto3.client(service, region_name=REGION)


# Conta objetos sob um prefixo usando paginacao. list_objects_v2 devolve no
# maximo 1000 chaves por chamada; sem paginator a contagem fica errada em
# particao grande.
def _count_objects(bucket, prefix):
    s3 = _client("s3")
    total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        total += page.get("KeyCount", 0)
    return total


# Le o arquivo de metricas que o job Spark escreveu. O Spark grava um
# diretorio com part-*.json, entao localizamos a primeira chave .json
# em vez de assumir um nome fixo.
def _read_stage_metrics(stage, dt):
    s3 = _client("s3")
    bucket = f"{PROJECT}-artifacts"
    prefix = f"metrics/{stage}/dt={dt}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".json"):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8")
            return json.loads(body.strip().splitlines()[0])
    return {}


# Publica metricas custom no CloudWatch - a serie temporal que o alarme de
# monitoring.tf observa. Best-effort de proposito: metrica indisponivel nao
# pode derrubar o pipeline (o dado dela ja esta no DynamoDB de qualquer
# forma); a falha vai pro log e a execucao segue.
def _publish_metrics(dimensions, values):
    try:
        _client("cloudwatch").put_metric_data(
            Namespace=f"{PROJECT}/pipeline",
            MetricData=[
                {
                    "MetricName": name,
                    "Dimensions": [
                        {"Name": k, "Value": v} for k, v in dimensions.items()
                    ],
                    "Value": float(value),
                    "Timestamp": datetime.now(timezone.utc),
                }
                for name, value in values.items()
            ],
        )
    except Exception as exc:  # noqa: BLE001 - best-effort, nunca derruba o run
        print(f"[metrics] falha ao publicar no CloudWatch: {exc}")


# Acao 1: valida se a particao do dia chegou no bronze. E a pre-condicao do
# pipeline inteiro - falhar cedo aqui evita queimar recurso de cluster.
# dt="auto" (ou ausente) resolve para D-1 em UTC: e assim que o agendamento
# do EventBridge dispara sem saber aritmetica de data - os estados seguintes
# leem o dt resolvido daqui ($.landing.dt), nao do input.
def validate_landing(event):
    dt = event.get("dt")
    if not dt or dt == "auto":
        dt = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    bucket = f"{PROJECT}-bronze"
    prefix = f"events/dt={dt}/"
    count = _count_objects(bucket, prefix)
    if count == 0:
        raise LandingEmptyError(f"Nenhum arquivo em s3://{bucket}/{prefix}")
    return {"dt": dt, "landing_objects": count}


# Acao 2: confirma que o estagio produziu saida, le as metricas dele e
# persiste no DynamoDB. Se a saida nao existe, lanca erro retentavel.
def checkpoint_stage(event):
    dt, stage, run_id = event["dt"], event["stage"], event["run_id"]
    bucket, root = STAGE_OUTPUT[stage]
    prefix = f"{root}/dt={dt}/"
    count = _count_objects(bucket, prefix)
    if count == 0:
        raise StageOutputMissingError(f"Estagio {stage} sem saida em s3://{bucket}/{prefix}")

    metrics = _read_stage_metrics(stage, dt)
    _client("dynamodb").put_item(
        TableName=METRICS_TABLE,
        Item={
            "run_id": {"S": run_id},
            "stage": {"S": stage},
            "dt": {"S": dt},
            "output_objects": {"N": str(count)},
            "metrics": {"S": json.dumps(metrics)},
            "recorded_at": {"S": datetime.now(timezone.utc).isoformat()},
        },
    )

    # Serie temporal por estagio. Dimensao so por Stage - dt como dimensao
    # criaria uma serie nova por dia (alta cardinalidade), o anti-padrao
    # classico de custo em metrica custom.
    _publish_metrics(
        {"Stage": stage},
        {
            "output_objects": count,
            "input_records": int(metrics.get("input_records", 0)),
            "rejected_records": int(metrics.get("rejected_records", 0)),
        },
    )
    return {"stage": stage, "output_objects": count, "metrics": metrics}


# Acao 3: portao de qualidade. Le todas as metricas da execucao e compara a
# taxa de rejeicao do silver contra o limite. Retorna PASS/FAIL e a mensagem
# que sera publicada no SNS.
def quality_gate(event):
    run_id, dt = event["run_id"], event["dt"]
    resp = _client("dynamodb").query(
        TableName=METRICS_TABLE,
        KeyConditionExpression="run_id = :r",
        ExpressionAttributeValues={":r": {"S": run_id}},
    )
    silver = {}
    for item in resp.get("Items", []):
        if item["stage"]["S"] == "silver":
            silver = json.loads(item["metrics"]["S"])

    total = int(silver.get("input_records", 0))
    rejected = int(silver.get("rejected_records", 0))
    rate = (rejected / total) if total else 1.0
    status = "PASS" if rate <= MAX_REJECT_RATE else "FAIL"

    # E esta metrica que o alarme reject-rate-alto observa: o quality gate
    # decide POR EXECUCAO; o alarme acompanha a TENDENCIA entre execucoes.
    _publish_metrics(
        {"Gate": "quality"},
        {"reject_rate": rate, "gate_pass": 1.0 if status == "PASS" else 0.0},
    )

    return {
        "status": status,
        "reject_rate": round(rate, 4),
        "threshold": MAX_REJECT_RATE,
        "message": (
            f"[{status}] run={run_id} dt={dt} "
            f"registros={total} rejeitados={rejected} "
            f"taxa={rate:.2%} limite={MAX_REJECT_RATE:.2%}"
        ),
    }


ACTIONS = {
    "validate_landing": validate_landing,
    "checkpoint_stage": checkpoint_stage,
    "quality_gate": quality_gate,
}


# Despacho unico. Acao desconhecida vira erro explicito em vez de retorno
# silencioso vazio, que seria muito pior de diagnosticar.
def handler(event, context):
    action = event.get("action")
    if action not in ACTIONS:
        raise ValueError(f"Acao desconhecida: {action}")
    return ACTIONS[action](event)
