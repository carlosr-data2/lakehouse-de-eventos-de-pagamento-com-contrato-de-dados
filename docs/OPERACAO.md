# Operação — Lakehouse de Eventos de Pagamento com Contrato de Dados, Step Functions e CI/CD

## Retomada automatizada (caminho recomendado)

Um comando substitui o passo a passo manual da seção seguinte:

```bash
make retomar              # sobe LocalStack + Terraform e deixa a infra pronta
make retomar COM_DADOS=1  # além da infra, regera bronze/silver/gold (passos 3-5)
```

O `scripts/retomar.sh` executa, nesta ordem: pré-checagens da máquina (drift
de relógio do WSL2 e memória disponível — os dois problemas reais que já
derrubaram o apply neste projeto), sobe o LocalStack esperando **todos** os
serviços ficarem saudáveis, detecta state órfão (state local com recursos que
o emulador reiniciado não conhece mais — vira backup datado, nunca delete),
roda `terraform init + apply` com `-parallelism=2` (paralelismo alto mata o
plugin do provider por falta de memória no WSL) e uma retentativa automática,
confere a contagem de recursos (17 com as flags padrão) com um smoke test, e
por fim infere da máquina em que passo do projeto você parou. A saída bruta do
Terraform vai para `/tmp/retomar-terraform.log`; no terminal fica só o resumo.

Duas lições de WSL2 codificadas no script:

- **Drift de relógio**: depois de suspend/hibernação o relógio da VM descola
  do host — sintomas: `Creation complete after -1m48s` (tempo negativo) e
  timeouts falsos no provider. Correção: `sudo hwclock -s`.
- **`Plugin did not respond`**: o processo do provider AWS morre por pressão
  de memória com o paralelismo padrão (10). Correção: `-parallelism=2`, que o
  script já aplica sempre.

O script nunca destrói nada: encerrar o dia continua sendo `make destroy`,
decisão sua. Se estiver refazendo o projeto do zero e o state ainda não tiver
os 17 recursos, sobrescreva a contagem esperada: `ESPERADO=11 make retomar`.

## Subir e retomar o ambiente (passo a passo manual)

```bash
# 1. Abrir o projeto no VS Code (Ubuntu) -- pasta raiz criada no Passo 1 (mkdir -p evt-lakehouse/...)
# ajuste o caminho se você criou em outro lugar -- aqui supõe que evt-lakehouse/ está na pasta atual
code evt-lakehouse

# 2. Subir o ambiente -- LocalStack via Docker Compose (docker-compose.yml na raiz, Passo 1)
cd evt-lakehouse
docker compose up -d

# 3. Workflow do Terraform -- os arquivos .tf ficam em infra/
cd infra
terraform init
terraform plan
terraform apply
# ao pausar por hoje, desmonte tudo (LocalStack não persiste nada entre reinícios, mas evita drift no state):
terraform destroy

# 4. Contar e conferir os recursos criados -- 'data.*' aparece no state mas não é recurso AWS de verdade, filtra fora
terraform state list | grep -v '^data\.' | wc -l
# esse total é DINÂMICO -- bate com o total acumulado do campo "Recursos criados" do ÚLTIMO PASSO que
# você já implementou de verdade (nem sempre todos). Quando o projeto estiver 100% concluído, o total
# final é 17.
terraform state list | grep -v '^data\.'
```

## Verificações por etapa

### Provisionamento (parte 1): fundação do lakehouse — storage, IAM e tabela de métricas

Confirme que os cinco buckets existem e que os nomes batem com o esperado:

aws --endpoint-url=http://localhost:4566 s3 ls

A saída deve listar evt-lakehouse-bronze, evt-lakehouse-silver, evt-lakehouse-gold, evt-lakehouse-quarantine e evt-lakehouse-artifacts.

Confirme o versionamento seletivo (bronze deve retornar Enabled, silver deve retornar vazio):

aws --endpoint-url=http://localhost:4566 s3api get-bucket-versioning --bucket evt-lakehouse-bronze
aws --endpoint-url=http://localhost:4566 s3api get-bucket-versioning --bucket evt-lakehouse-silver

Confirme a tabela DynamoDB e a role:

aws --endpoint-url=http://localhost:4566 dynamodb describe-table --table-name evt-lakehouse-run-metrics --query 'Table.KeySchema'
aws --endpoint-url=http://localhost:4566 iam get-role --role-name evt-lakehouse-pipeline-role --query 'Role.Arn'

Confirme o state e os outputs:

terraform -chdir=infra state list
terraform -chdir=infra output

O state deve listar 11 recursos com as flags desligadas — cinco buckets, dois versionamentos, a tabela, a role, a policy e o attachment. Anote esse número: na AWS real, com enable_lifecycle e enable_glue ligados, ele sobe para 16. Saber explicar a diferença vale mais do que fingir que ela não existe.

Se quiser ver o comportamento do ciclo de vida por curiosidade, rode terraform -chdir=infra apply -var="enable_lifecycle=true" e observe o apply estourar por timeout após três minutos. Depois confirme que a regra existe apesar do erro:

aws --endpoint-url=http://localhost:4566 s3api get-bucket-lifecycle-configuration --bucket evt-lakehouse-bronze

Esse é o diagnóstico completo: o PUT funciona, a leitura de confirmação é que não. Volte a flag para false e siga.

Para limpar tudo ao final, rode terraform -chdir=infra destroy (o force_destroy nos buckets garante que funcione mesmo com objetos dentro) e depois docker compose down -v para derrubar o LocalStack.

**Pausar aqui:** Se for parar por hoje logo depois deste passo, pode deixar o container do LocalStack rodando -- amanhã o terraform plan deve dar 'No changes' e você já emenda no Passo 2. Se for desligar a máquina, sem problema: como o docker-compose.yml não tem PERSISTENCE=1, os 11 recursos somem quando o container para de qualquer forma -- é só subir o LocalStack de novo e rodar terraform apply (poucos segundos, sem custo) antes de seguir.

### Provisionamento (parte 2): plano de controle — Lambda, SNS/SQS e a máquina de estados

Confirme que a máquina de estados foi criada:

aws --endpoint-url=http://localhost:4566 stepfunctions list-state-machines --query 'stateMachines[].name'

Teste a Lambda isoladamente antes de orquestrar. A validação de landing deve falhar, porque o bronze ainda está vazio — e falhar aqui é o resultado correto:

aws --endpoint-url=http://localhost:4566 lambda invoke \
  --function-name evt-lakehouse-pipeline-ops \
  --payload '{"action":"validate_landing","run_id":"teste","dt":"2026-07-03"}' \
  --cli-binary-format raw-in-base64-out /tmp/out.json
cat /tmp/out.json

A saída deve conter errorType igual a LandingEmptyError. Esse é o erro que o Catch da máquina de estados vai capturar mais adiante.

Confirme o tópico, a fila e o vínculo entre eles:

aws --endpoint-url=http://localhost:4566 sns list-subscriptions --query 'Subscriptions[].[TopicArn,Protocol]'

Confirme as saídas do Terraform (usadas nos próximos passos):

terraform -chdir=infra output state_machine_arn
terraform -chdir=infra output alerts_queue_url

O state agora deve ter 17 recursos. Se a Lambda falhar ao ser invocada com erro de runtime em vez de LandingEmptyError, o problema costuma ser o empacotamento: confirme que infra/.build/pipeline_ops.zip existe e contém pipeline_ops.py na raiz do zip.

Para limpar tudo ao final do projeto: terraform -chdir=infra destroy remove Lambda, SNS, SQS, log group e máquina de estados junto com a fundação do passo 1.

**Pausar aqui:** Mesma lógica do Passo 1: se o LocalStack continuar rodando, não precisa fazer nada. Se reiniciar o container, reaplique o Terraform (agora incluindo orchestration.tf) antes de seguir pro Passo 3 -- o state volta a ter os 17 recursos e a Lambda/Step Functions ficam prontas de novo em segundos, sem custo.

### Geração e ingestão dos eventos brutos na camada bronze

Liste as partições criadas no bronze e confirme que são três:

aws --endpoint-url=http://localhost:4566 s3 ls s3://evt-lakehouse-bronze/events/ --recursive

Confirme que a dimensão de estabelecimentos chegou no bucket gold:

aws --endpoint-url=http://localhost:4566 s3 ls s3://evt-lakehouse-gold/dim_merchants/

Inspecione o conteúdo bruto de uma partição para ver os defeitos com os próprios olhos:

aws --endpoint-url=http://localhost:4566 s3 cp s3://evt-lakehouse-bronze/events/dt=2026-07-03/events-2026-07-03.jsonl.gz - | gunzip | head -20

Agora repita a invocação da Lambda de validação que falhou no passo 2. Desta vez ela deve retornar sucesso com a contagem de objetos, provando que a pré-condição do pipeline foi satisfeita:

aws --endpoint-url=http://localhost:4566 lambda invoke \
  --function-name evt-lakehouse-pipeline-ops \
  --payload '{"action":"validate_landing","run_id":"teste","dt":"2026-07-03"}' \
  --cli-binary-format raw-in-base64-out /tmp/out.json
cat /tmp/out.json

**Pausar aqui:** Os eventos gerados aqui são dado, não infraestrutura -- se o LocalStack perder o estado, as partições no bronze e a dimensão de estabelecimentos em gold somem junto com os buckets. Reaplicar o Terraform sozinho não traz esse dado de volta: é preciso rodar o gerador de eventos de novo (mesmo comando deste passo) antes de seguir pro job PySpark do Passo 4, que lê exatamente esses arquivos.

### Job PySpark bronze para silver: contrato de dados, quarentena e testes unitários

Os quatro testes unitários devem passar antes de qualquer execução contra dado real. Se algum falhar, corrija a regra antes de rodar o job — é exatamente para isso que os testes existem.

Confirme que as três partições do silver foram escritas em Parquet:

aws --endpoint-url=http://localhost:4566 s3 ls s3://evt-lakehouse-silver/events/ --recursive | head -20

Confirme que a quarentena recebeu os registros reprovados:

aws --endpoint-url=http://localhost:4566 s3 ls s3://evt-lakehouse-quarantine/events/ --recursive | head

Leia o arquivo de métricas do último dia e compare a taxa de rejeição com o que você calculou no passo 3:

aws --endpoint-url=http://localhost:4566 s3 ls s3://evt-lakehouse-artifacts/metrics/silver/dt=2026-07-03/
aws --endpoint-url=http://localhost:4566 s3 cp s3://evt-lakehouse-artifacts/metrics/silver/dt=2026-07-03/ - --recursive | head -1

Espere ver reject_rate em torno de 0,05, duplicates_removed próximo de 3% do volume, e a quebra por motivo em reasons. Se a taxa vier acima do limite de 5% configurado no quality gate, o passo 6 vai reprovar de propósito — e isso também é um resultado válido de observar.

Confirme que a Lambda de checkpoint agora enxerga a saída do estágio silver:

aws --endpoint-url=http://localhost:4566 lambda invoke \
  --function-name evt-lakehouse-pipeline-ops \
  --payload '{"action":"checkpoint_stage","run_id":"teste","dt":"2026-07-03","stage":"silver"}' \
  --cli-binary-format raw-in-base64-out /tmp/out.json
cat /tmp/out.json

**Pausar aqui:** Se pausar aqui, o silver e a quarentena já estão gravados -- só regrave se o LocalStack tiver perdido o estado (nesse caso, reaplique o Terraform, regere os eventos do Passo 3 e rode este job de novo, nessa ordem). Os quatro testes unitários (pytest) não dependem do LocalStack e continuam passando mesmo com o container desligado, então dá pra revisá-los offline antes de continuar.

### Job silver para gold: SQL analítico avançado, broadcast join e exposição para Redshift

Confirme que as três partições da camada gold existem:

aws --endpoint-url=http://localhost:4566 s3 ls s3://evt-lakehouse-gold/merchant_daily/ --recursive | head

Leia as métricas do estágio gold e confira a contagem de estabelecimentos (deve ficar próxima de 300):

aws --endpoint-url=http://localhost:4566 s3 cp s3://evt-lakehouse-artifacts/metrics/gold/dt=2026-07-03/ - --recursive | head -1

Verifique o conteúdo analítico abrindo o Parquet e conferindo três coisas: se variacao_gmv_pct está nula no dia 2026-07-01 (correto, não há dia anterior) e preenchida em 2026-07-03; se rank_categoria começa em 1 dentro de cada categoria; e se taxa_aprovacao fica entre 0 e 1:

docker run --rm --network lakehouse-net --user root \
  -v "$(pwd)/jobs":/opt/jobs -v "$(pwd)/.ivy":/root/.ivy2 \
  bitnamilegacy/spark:3.5.1 bash -c "echo \"spark.read.parquet('s3a://evt-lakehouse-gold/merchant_daily/').orderBy('dt','rank_categoria').show(10, False)\" > /tmp/c.py && spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 --conf spark.hadoop.fs.s3a.access.key=test --conf spark.hadoop.fs.s3a.secret.key=test --conf spark.hadoop.fs.s3a.path.style.access=true /tmp/c.py"

Confirme que o plano de controle enxerga a saída do estágio gold:

aws --endpoint-url=http://localhost:4566 lambda invoke \
  --function-name evt-lakehouse-pipeline-ops \
  --payload '{"action":"checkpoint_stage","run_id":"teste","dt":"2026-07-03","stage":"gold"}' \
  --cli-binary-format raw-in-base64-out /tmp/out.json
cat /tmp/out.json

O arquivo sql/redshift_gold.sql não é executado localmente, porque Redshift não é emulado pelo LocalStack community. Ele existe como artefato de projeto: é o DDL e o COPY que rodariam na AWS real, com as decisões de DISTKEY e SORTKEY justificadas em comentário. Vale ler e entender cada escolha, porque é exatamente esse tipo de raciocínio que separa quem usa Redshift de quem sabe modelá-lo.

**Pausar aqui:** Mesmo raciocínio dos passos anteriores: a camada gold só existe enquanto o LocalStack mantiver o estado. Com o container ainda rodando, não precisa refazer nada. Se reiniciar, a ordem pra reconstruir é sempre a mesma -- Terraform, gerador de eventos, job bronze→silver, job silver→gold -- antes de seguir pro Passo 6, que espera essa cadeia completa já rodada.

### Orquestração ponta a ponta: executar, quebrar de propósito e comparar com Airflow

Liste as execuções e confirme que existem os dois desfechos, FAILED e SUCCEEDED:

aws --endpoint-url=http://localhost:4566 stepfunctions list-executions \
  --state-machine-arn "$SM_ARN" --query 'executions[].[name,status]' --output table

Na execução de erro, confirme que o fluxo passou por NotifyFailure antes de terminar. Procure no histórico o evento de entrada nesse estado:

aws --endpoint-url=http://localhost:4566 stepfunctions get-execution-history \
  --execution-arn "$EXEC_ARN" --query 'events[].type' --output text

Na execução de retry, o histórico deve mostrar TaskFailed seguido de novo TaskScheduled mais de uma vez, com o intervalo entre eles crescendo. Se aparecer apenas uma falha e o desvio imediato para o Catch, o nome da exceção no bloco Retry não está batendo com o errorType real — esse é o erro mais comum ao configurar retry no Step Functions.

Confirme que a mensagem chegou na fila e traz o veredito do quality gate com a taxa de rejeição e o limite.

Confirme no DynamoDB que existem duas linhas (silver e gold) sob o run_id da execução bem-sucedida.

Se o quality gate reprovar, leia a quebra de motivos no campo metrics do DynamoDB e decida conscientemente: ou ajusta max_reject_rate em infra/variables.tf e reaplica o Terraform, ou trata a origem do dado sujo. Anote a decisão e o número.

**Pausar aqui:** As execuções do Step Functions e as linhas gravadas no DynamoDB são histórico -- se o LocalStack perder o estado, esse histórico some, mas não é grave: basta disparar as execuções de novo, leva segundos. Antes de pausar, garanta só que os dados de bronze/silver/gold do dia usado no teste (2026-07-03) ainda existem -- senão regenere a cadeia dos passos 3 a 5 primeiro.

### CI/CD, FinOps e fechamento: transformar o projeto em software entregável

Os quatro alvos do Makefile devem passar localmente na sequência, sem intervenção manual: fmt, validate, test e smoke. Se o smoke test falhar, ele imprime exatamente qual recurso está faltando.

No GitHub, faça um push e confirme que os três jobs rodam em cascata e ficam verdes. Depois teste o CI de propósito: introduza um erro no Terraform (por exemplo, referencie um recurso inexistente na policy) e confirme que o estágio static falha antes de gastar tempo com o resto. Reverta em seguida.

Checklist de fechamento do projeto — percorra e responda cada item por escrito no README:

Consistência de nomes: todo recurso usado nos passos 3 a 6 foi criado nos passos 1 e 2, e nenhum comando recria infraestrutura. Confira grepando pelo prefixo evt-lakehouse no repositório inteiro e verificando que só o Terraform declara recursos.

Reprodutibilidade completa: derrube tudo com make destroy e reconstrua com make pipeline. O lakehouse inteiro deve subir do zero sem nenhum comando manual. Se algum passo exigir intervenção, aquilo é uma lacuna de automação e deve ser corrigida ou registrada.

Contagem de recursos: com as flags padrão, o state tem 17 recursos. Com enable_lifecycle e enable_glue ligados na AWS real, sobe para 22. Saber explicar a diferença e o motivo de cada flag é parte do entregável.

Decisões de arquitetura documentadas: PySpark puro em vez de GlueContext, bucket por camada em vez de prefixo, versionamento seletivo, row_number em vez de dropDuplicates, Step Functions em vez de Airflow, Spectrum versus tabela interna no Redshift. Cada uma com o custo e o ganho escritos.

Limitações assumidas: role IAM única, ausência de formato de tabela transacional, ausência de gatilho por evento, ausência de detecção de deriva de schema, e as duas flags que isolam o que a emulação não cobre.

Números concretos: taxa de rejeição observada, quebra por motivo, volume por camada, tempo de execução de cada job. Projeto sem número medido é projeto sem evidência.

Limpeza: ao final, rode make destroy, que executa terraform destroy e derruba o LocalStack com os volumes.

**Pausar aqui:** Este é o fechamento do projeto -- depois de rodar o checklist e confirmar o CI verde no GitHub, é o momento certo de rodar make destroy de verdade (não só por curiosidade): não há passo seguinte que dependa da infraestrutura continuar de pé. Se ainda for revisar algo do checklist amanhã, pode deixar como está sem custo nenhum (LocalStack é sempre gratuito); só destrua quando o projeto estiver mesmo encerrado.

