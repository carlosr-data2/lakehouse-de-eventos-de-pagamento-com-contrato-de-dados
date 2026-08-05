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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://localhost:4566")
    args = parser.parse_args()

    failures = []
    check_buckets(args.endpoint, failures)
    check_state_machine(args.endpoint, failures)
    check_lambda(args.endpoint, failures)

    if failures:
        print("SMOKE TEST FALHOU:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("SMOKE TEST OK: infraestrutura provisionada e Lambda respondendo")


if __name__ == "__main__":
    main()
