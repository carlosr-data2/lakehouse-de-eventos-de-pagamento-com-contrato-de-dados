# Limitações assumidas e caminho de evolução

Limitações **conscientes** do escopo atual — cada uma com o porquê de ter sido aceita e o caminho de evolução. Falar limite com precisão vale mais que fingir completude.

## 1. Role IAM única para todo o pipeline

**Por que foi aceita.** No laboratório, uma role (`evt-lakehouse-pipeline-role`) simplifica o provisionamento e o LocalStack community não aplica IAM de verdade.

**Evolução.** Uma role por componente (Lambda, jobs, orquestrador) com mínimo privilégio por bucket/ação — o desenho já separa os componentes, então a divisão é mecânica.

## 2. Catálogo Iceberg `hadoop` e manutenção de tabelas não agendada

O que era a limitação nº 2 (sem formato transacional) foi resolvido pelo [ADR-010](DECISOES.md#adr-010--open-table-formats-iceberg-no-silver-delta-no-gold): silver é tabela Iceberg, gold é tabela Delta. O que fica no lugar são as duas arestas assumidas dessa adoção.

**Por que foi aceita.** O catálogo `hadoop` guarda o ponteiro de metadados no próprio S3 — zero infraestrutura extra e funciona no LocalStack community, que não emula o Glue Catalog. Com um único writer (a state machine), o commit por rename em object store não vira corrida. E nenhum job de manutenção roda: snapshots do Iceberg e versões do Delta se acumulam sem poda.

**Evolução.** Catálogo de verdade — Glue Data Catalog ou um catálogo REST (Nessie/Polaris) — para commit atômico multi-writer e descoberta via Athena/EMR; manutenção agendada como mais um estágio da state machine: `expire_snapshots` + `rewrite_data_files` (compaction) no Iceberg, `OPTIMIZE` + `VACUUM` no Delta.

## 3. Sem gatilho por evento — pipeline agendado (agora via EventBridge)

**Por que foi aceita.** A execução diária atende o caso de uso e mantém o custo de orquestração previsível. O agendamento é gerenciado (EventBridge → Step Functions, `infra/monitoring.tf`), com `dt` resolvido para D-1 pela Lambda de validação — automatizado, mas ainda por relógio, não por chegada de dado.

**Evolução.** S3 → EventBridge → Step Functions para latência menor; exige idempotência reforçada (chegadas duplicadas) — a base já existe na deduplicação do contrato.

## 4. Sem detecção de deriva de schema

**Por que foi aceita.** O contrato valida os campos esperados; campo novo inesperado hoje é ignorado silenciosamente. O lado *tabela* do problema o Iceberg já resolve — coluna nova entra por `ALTER TABLE ADD COLUMN` sem reescrever dado (provado em [`tests/test_table_formats.py`](../tests/test_table_formats.py)); o que falta é o lado *detecção* na borda do bronze.

**Evolução.** Comparar o schema observado com o `RAW_SCHEMA` a cada run e alertar diferença (novo campo → aviso + `ALTER TABLE` no silver; tipo alterado → quarentena), antes de evoluir para um schema registry.

## 5. Limites do emulador, codificados

- `aws_s3_bucket_lifecycle_configuration` atrás da flag `enable_lifecycle` (default `false`): o provider nunca estabiliza a leitura pós-PUT no LocalStack.
- Provider AWS travado abaixo de 5.67 ([ADR-008](DECISOES.md#adr-008--provider-aws-travado-em--5600--5670)).

**Evolução.** Na AWS real, habilitar a flag e destravar o provider — ambos são mudanças de uma linha, comentadas no código.
