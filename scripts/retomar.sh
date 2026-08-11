#!/usr/bin/env bash
# Retomada automatizada do laboratório: valida a máquina (WSL), sobe o
# LocalStack, resolve state órfão, aplica o Terraform e verifica o resultado.
#
# Uso:  make retomar               (só infraestrutura)
#       make retomar COM_DADOS=1   (infra + reidrata bronze/silver/gold)
#
# Princípios: nunca destrói nada (state vira backup datado, nunca delete;
# nenhum compose down, nenhum terraform destroy) e no caminho feliz roda do
# início ao fim sem perguntar nada. A saída bruta do Terraform vai para
# $LOG_TF; no terminal fica só o resumo.
set -euo pipefail

# --- parâmetros (sobrescrevíveis via ambiente: ESPERADO=11 make retomar) ----
ENDPOINT="${ENDPOINT:-http://localhost:4566}"
INFRA_DIR="infra"
ESPERADO="${ESPERADO:-17}"     # recursos no state com as flags padrão (passos 1-2)
PARALELISMO="${PARALELISMO:-2}" # >2 derruba o plugin do provider por OOM no WSL
DRIFT_MAX=5                     # segundos de drift de relógio tolerados
SENTINELA="evt-lakehouse-bronze"
SERVICOS="s3 iam sts dynamodb lambda stepfunctions sns sqs logs"
LOG_TF="${LOG_TF:-/tmp/retomar-terraform.log}"
COM_DADOS=0
[ "${1:-}" = "--com-dados" ] && COM_DADOS=1

# credenciais fake do LocalStack, pros comandos aws cli do smoke test
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1

etapa() { printf '\n==> [%s/6] %s\n' "$1" "$2"; }
ok()    { printf '    %s ... OK%s\n' "$1" "${2:+ ($2)}"; }
aviso() { printf '    AVISO: %s\n' "$1"; }
falha() { printf '    ERRO: %s\n' "$1"; exit 1; }

confirmar() { # pergunta só em terminal interativo; em CI/pipe segue com aviso
  if [ -t 0 ]; then
    read -r -p "    Continuar mesmo assim? [s/N] " r
    [ "$r" = "s" ] || [ "$r" = "S" ] || exit 1
  else
    aviso "terminal não interativo -- seguindo mesmo assim"
  fi
}

cd "$(dirname "$0")/.."   # raiz do projeto, independente de onde foi chamado
command -v terraform >/dev/null || falha "terraform não encontrado no PATH"
command -v docker    >/dev/null || falha "docker não encontrado no PATH"

# --- 1. pré-checagens da máquina --------------------------------------------
# As duas pegadinhas reais do WSL2 que já quebraram o apply neste projeto:
# relógio descolado do host depois de suspend (timeouts falsos, tempos
# negativos no log do Terraform) e falta de memória matando o plugin do
# provider ("Plugin did not respond").
etapa 1 "Pré-checagens da máquina"

ref=""
for url in https://www.google.com https://registry.terraform.io; do
  ref="$(curl -fsSI --max-time 5 "$url" 2>/dev/null | tr -d '\r' | sed -n 's/^[Dd]ate: //p' | head -1)"
  [ -n "$ref" ] && break
done
if [ -n "$ref" ]; then
  drift=$(( $(date -u +%s) - $(date -ud "$ref" +%s) ))
  [ "$drift" -lt 0 ] && drift=$(( -drift ))
  if [ "$drift" -gt "$DRIFT_MAX" ]; then
    aviso "relógio do WSL está ${drift}s fora da referência externa."
    aviso "isso causa timeouts falsos no provider AWS. Corrija com: sudo hwclock -s"
    confirmar
  else
    ok "Relógio: drift de ${drift}s vs referência"
  fi
else
  aviso "sem rede pra verificar o relógio -- pulando (se o apply travar, rode: sudo hwclock -s)"
fi

mem_disp="$(free -m | awk '/^Mem:/ {print $7}')"
if [ "${mem_disp:-0}" -lt 2048 ]; then
  aviso "só ${mem_disp} MB de RAM disponíveis -- apply seguirá com parallelism=$PARALELISMO; se falhar, feche programas ou ajuste o .wslconfig"
else
  ok "Memória: ${mem_disp} MB disponíveis"
fi

# --- 2. LocalStack -----------------------------------------------------------
etapa 2 "LocalStack"
docker compose up -d >/dev/null 2>&1
ok "docker compose up -d"

# espera TODOS os serviços usados (o 'make up' antigo só esperava o s3)
inicio=$(date +%s)
while :; do
  saude="$(curl -s --max-time 3 "$ENDPOINT/_localstack/health" || true)"
  pronto=1
  for s in $SERVICOS; do
    echo "$saude" | grep -Eq "\"$s\": \"(available|running)\"" || { pronto=0; break; }
  done
  [ "$pronto" = 1 ] && break
  [ $(( $(date +%s) - inicio )) -gt 90 ] && falha "LocalStack não ficou saudável em 90s -- veja: docker compose logs localstack"
  sleep 2
done
ok "Serviços prontos ($SERVICOS)" "$(( $(date +%s) - inicio ))s"

# --- 3. state órfão ----------------------------------------------------------
# O LocalStack community não persiste nada entre reinícios: se o state local
# tem recursos mas o bucket sentinela não existe no emulador, o state é de uma
# sessão morta. Backup datado (nunca delete) e apply do zero.
etapa 3 "State do Terraform"
no_state="$(terraform -chdir="$INFRA_DIR" state list 2>/dev/null | grep -cv '^data\.' || true)"
if [ "${no_state:-0}" -gt 0 ]; then
  if aws --endpoint-url="$ENDPOINT" s3api head-bucket --bucket "$SENTINELA" >/dev/null 2>&1; then
    ok "State com $no_state recursos e LocalStack ainda os conhece -- mantido"
  else
    bak="$INFRA_DIR/terraform.tfstate.bak-$(date +%Y%m%d-%H%M%S)"
    mv "$INFRA_DIR/terraform.tfstate" "$bak"
    aviso "state tinha $no_state recursos mas o LocalStack subiu limpo (sentinela $SENTINELA ausente)"
    ok "Backup do state órfão em $bak"
  fi
else
  ok "Sem state anterior -- apply parte do zero"
fi

# --- 4. terraform init + apply ----------------------------------------------
etapa 4 "terraform init + apply (parallelism=$PARALELISMO)"
: > "$LOG_TF"
terraform -chdir="$INFRA_DIR" init -input=false >>"$LOG_TF" 2>&1 || {
  tail -20 "$LOG_TF"; falha "init falhou -- log completo em $LOG_TF"; }
ok "init"

aplicar() { terraform -chdir="$INFRA_DIR" apply -input=false -auto-approve -parallelism="$PARALELISMO" >>"$LOG_TF" 2>&1; }
inicio=$(date +%s)
if ! aplicar; then
  if grep -q "Plugin did not respond" "$LOG_TF"; then
    aviso 'apply falhou ("Plugin did not respond") -- retentando (1/1); o que já entrou no state não é recriado'
    aplicar || { tail -20 "$LOG_TF"; falha "apply falhou de novo -- log completo em $LOG_TF"; }
  else
    tail -20 "$LOG_TF"; falha "apply falhou -- log completo em $LOG_TF"
  fi
fi
resumo="$(grep -Eo 'Apply complete! Resources: .*' "$LOG_TF" | tail -1)"
ok "${resumo:-apply}" "$(( $(date +%s) - inicio ))s"

# --- 5. verificação ----------------------------------------------------------
etapa 5 "Verificação"
n="$(terraform -chdir="$INFRA_DIR" state list | grep -cv '^data\.')"
if [ "$n" -eq "$ESPERADO" ]; then
  ok "Recursos no state: $n/$ESPERADO"
else
  aviso "state tem $n recursos, esperado $ESPERADO (sobrescreva com ESPERADO=$n se estiver no meio do projeto)"
fi
if command -v aws >/dev/null; then
  b="$(aws --endpoint-url="$ENDPOINT" s3 ls 2>/dev/null | grep -c evt-lakehouse || true)"
  aws --endpoint-url="$ENDPOINT" lambda get-function --function-name evt-lakehouse-pipeline-ops >/dev/null 2>&1 && l="pipeline-ops" || l="AUSENTE"
  aws --endpoint-url="$ENDPOINT" stepfunctions list-state-machines --query 'stateMachines[].name' --output text 2>/dev/null | grep -q daily-pipeline && m="daily-pipeline" || m="AUSENTE"
  ok "s3: $b buckets | lambda: $l | sfn: $m"
else
  aviso "aws cli não encontrado -- smoke test pulado"
fi

# --- 6. dados ----------------------------------------------------------------
# Infra recriada não traz dado de volta: bronze/silver/gold são produto dos
# passos 3-5 e somem com o container. A posição abaixo é inferida da MÁQUINA
# (o que existe nos buckets), não do app -- pode diferir de onde o estudo parou.
etapa 6 "Dados"
tem() { aws --endpoint-url="$ENDPOINT" s3 ls "s3://evt-lakehouse-$1" --recursive 2>/dev/null | grep -q . ; }
if [ "$COM_DADOS" = 1 ]; then
  # os alvos de dados nascem nos Passos 3-5; em máquina que ainda não os
  # implementou, avisa em vez de quebrar no meio do make
  if make -n ingest silver gold >/dev/null 2>&1; then
    ok "Reidratando: make ingest silver gold"
    make ingest silver gold
  else
    aviso "seu Makefile ainda não tem os alvos de dados (ingest/silver/gold) -- eles nascem nos Passos 3-5; implemente esses passos primeiro e a flag --com-dados passa a funcionar"
  fi
elif tem "bronze/events"; then
  tem "silver/events" && tem "gold/merchant_daily" \
    && ok "bronze, silver e gold populados -- cadeia de dados completa" \
    || aviso "bronze tem dados mas silver/gold não -- rode os jobs (make silver gold)"
else
  aviso "buckets vazios (LocalStack não persiste dados). Pra reidratar: make retomar COM_DADOS=1"
fi

printf '\nRetomada concluída. Infra dos Passos 1-2 de pé.\n'
if ! tem "bronze/events"; then
  printf 'Posição inferida da máquina: antes do Passo 3 (geração e ingestão dos eventos).\n'
elif ! tem "silver/events"; then
  printf 'Posição inferida da máquina: antes do Passo 4 (job bronze->silver).\n'
elif ! tem "gold/merchant_daily"; then
  printf 'Posição inferida da máquina: antes do Passo 5 (job silver->gold).\n'
else
  printf 'Posição inferida da máquina: dados completos -- Passo 6 (orquestração) em diante.\n'
fi
