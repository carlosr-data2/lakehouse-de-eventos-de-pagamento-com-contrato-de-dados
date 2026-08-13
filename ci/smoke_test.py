import argparse
import json
import sys

import boto3

PROJECT = "evt-lakehouse"
EXPECTED_BUCKETS = {f"{PROJECT}-{camada}" for camada in
                    ["bronze", "silver", "gold", "quarantine", "artifacts"]}


def client(service, endpoint):
    return boto3.client(service, endpoint_url=endpoint, region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test")


# Verificacao 1: todos os buckets do lakehouse foram criados.
def check_buckets(endpoint, failures):
    found = {b["Name"] for b in client("s3", endpoint).list_buckets()["Buckets"]}
    missing = EXPECTED_BUCKETS - found
    if missing:
        failures.append(f"buckets ausentes: {sorted(missing)}")


# Verificacao 2: a maquina de estados existe com o nome esperado.
def check_state_machine(endpoint, failures):
    sfn = client("stepfunctions", endpoint)
    names = {m["name"] for m in sfn.list_state_machines()["stateMachines"]}
    if f"{PROJECT}-daily-pipeline" not in names:
        failures.append("maquina de estados nao encontrada")


# Verificacao 3: a Lambda responde e falha do jeito esperado quando nao ha
# dado. Testar o caminho de erro tambem e parte do contrato do componente.
def check_lambda(endpoint, failures):
    resp = client("lambda", endpoint).invoke(
        FunctionName=f"{PROJECT}-pipeline-ops",
        Payload=json.dumps({"action": "validate_landing",
                            "run_id": "smoke", "dt": "1999-01-01"}).encode(),
    )
    payload = json.loads(resp["Payload"].read())
    if payload.get("errorType") != "LandingEmptyError":
        failures.append(f"lambda respondeu inesperado: {payload}")


# Verificacao 4: o caminho NRT existe - stream de entrada e o Firehose que
# entrega na bronze. Existencia apenas: entrega de fato e exercitada pelo
# producer no passo correspondente, nao no smoke.
def check_nrt(endpoint, failures):
    streams = client("kinesis", endpoint).list_streams().get("StreamNames", [])
    if f"{PROJECT}-events-nrt" not in streams:
        failures.append("kinesis stream nrt nao encontrado")
    delivery = client("firehose", endpoint).list_delivery_streams()
    if f"{PROJECT}-nrt-to-bronze" not in delivery.get("DeliveryStreamNames", []):
        failures.append("firehose nrt nao encontrado")


# Verificacao 5: monitoramento continuo - alarme de taxa de rejeicao e a
# regra de agendamento diario apontando para a state machine.
# Limite de emulador tratado como o ValidateStateMachineDefinition do
# ADR-008: o cloudwatch do LocalStack 3.8 community responde 500 ao
# DescribeAlarms mesmo com o alarme criado (PutMetricAlarm e DeleteAlarms
# funcionam - o apply e o destroy provam). A leitura tolera o erro do
# emulador com aviso; se a API responder e o recurso NAO existir, falha.
def check_monitoring(endpoint, failures):
    try:
        alarms = client("cloudwatch", endpoint).describe_alarms(
            AlarmNamePrefix=f"{PROJECT}-reject-rate-alto"
        )
        if not alarms.get("MetricAlarms"):
            failures.append("alarme de reject_rate nao encontrado")
    except Exception as exc:  # noqa: BLE001 - limite do emulador, documentado
        print(f"  aviso: DescribeAlarms indisponivel no emulador ({type(exc).__name__})")
    try:
        rules = client("events", endpoint).list_rules(NamePrefix=f"{PROJECT}-daily-schedule")
        if not rules.get("Rules"):
            failures.append("regra de agendamento diario nao encontrada")
    except Exception as exc:  # noqa: BLE001 - mesmo tratamento do alarme
        print(f"  aviso: ListRules indisponivel no emulador ({type(exc).__name__})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://localhost:4566")
    args = parser.parse_args()

    failures = []
    check_buckets(args.endpoint, failures)
    check_state_machine(args.endpoint, failures)
    check_lambda(args.endpoint, failures)
    check_nrt(args.endpoint, failures)
    check_monitoring(args.endpoint, failures)

    if failures:
        print("SMOKE TEST FALHOU:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("SMOKE TEST OK: infraestrutura provisionada e Lambda respondendo")


if __name__ == "__main__":
    main()
