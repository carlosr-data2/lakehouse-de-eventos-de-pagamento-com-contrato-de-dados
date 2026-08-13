# Limitações assumidas e caminho de evolução

Limitações **conscientes** do escopo atual — cada uma com o porquê de ter sido aceita e o caminho de evolução. Falar limite com precisão vale mais que fingir completude.

## 1. Role IAM única para todo o pipeline

**Por que foi aceita.** No laboratório, uma role (`evt-lakehouse-pipeline-role`) simplifica o provisionamento e o LocalStack community não aplica IAM de verdade.

**Evolução.** Uma role por componente (Lambda, jobs, orquestrador) com mínimo privilégio por bucket/ação — o desenho já separa os componentes, então a divisão é mecânica.

## 2. Sem formato de tabela transacional (Iceberg/Delta)

**Por que foi aceita.** Parquet particionado + sobrescrita por partição idempotente cobre o escopo — mas sem ACID, sem time travel e com uma janela de leitura parcial durante a sobrescrita.

**Evolução.** Iceberg nas camadas silver/gold: commits atômicos eliminam a janela de dado parcial, e schema evolution resolve parte da deriva (item 4).

## 3. Sem gatilho por evento — pipeline agendado (agora via EventBridge)

**Por que foi aceita.** A execução diária atende o caso de uso e mantém o custo de orquestração previsível. O agendamento é gerenciado (EventBridge → Step Functions, `infra/monitoring.tf`), com `dt` resolvido para D-1 pela Lambda de validação — automatizado, mas ainda por relógio, não por chegada de dado.

**Evolução.** S3 → EventBridge → Step Functions para latência menor; exige idempotência reforçada (chegadas duplicadas) — a base já existe na deduplicação do contrato.

## 4. Sem detecção de deriva de schema

**Por que foi aceita.** O contrato valida os campos esperados; campo novo inesperado hoje é ignorado silenciosamente.

**Evolução.** Comparar o schema observado com o `RAW_SCHEMA` a cada run e alertar diferença (novo campo → aviso; tipo alterado → quarentena), antes de evoluir para um schema registry.

## 5. Limites do emulador, codificados

- `aws_s3_bucket_lifecycle_configuration` atrás da flag `enable_lifecycle` (default `false`): o provider nunca estabiliza a leitura pós-PUT no LocalStack.
- Provider AWS travado abaixo de 5.67 ([ADR-008](DECISOES.md#adr-008--provider-aws-travado-em--5600--5670)).

**Evolução.** Na AWS real, habilitar a flag e destravar o provider — ambos são mudanças de uma linha, comentadas no código.
