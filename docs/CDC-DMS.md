# CDC com DMS — o desenho, e o que o pipeline já suporta

O LocalStack community não emula o AWS DMS, então este repositório não sobe
um endpoint de replicação de verdade. O que ele faz é mais útil para
entrevista e para produção: **prova que o pipeline já suporta a semântica de
CDC** (upsert por chave + ordenação temporal) e documenta aqui o desenho
completo com DMS para quando a fonte for um banco transacional real.

## A semântica, provada localmente

CDC entrega o mesmo registro mais de uma vez: o insert original e depois
cada update, como eventos separados. O que um pipeline precisa para absorver
isso sem duplicar nem regredir estado:

1. **Chave de negócio estável** — aqui, `event_id`.
2. **Ordenação temporal confiável** — aqui, `occurred_at`.
3. **Deduplicação determinística que fica com o mais recente** — exatamente
   o que `jobs/contract.py` faz: janela por `event_id` ordenada por
   `occurred_at` descendente, `row_number() = 1`.

O script [`ingest/generate_cdc_updates.py`](../ingest/generate_cdc_updates.py)
re-emite eventos existentes de um `dt` com `status=refunded` e
`occurred_at` + 1h — o formato de chegada de um upsert de CDC. Reprocessar o
silver do mesmo dia mostra cada `event_id` atualizado aparecendo **uma** vez,
com o status novo. Nenhuma linha do pipeline muda: a arquitetura já estava
pronta para CDC antes de o CDC existir — é isso que "desenhar para evolução"
significa na prática.

```bash
python ingest/generate_cdc_updates.py --dt 2026-07-02
make silver DATES=2026-07-02
# verificação: os event_ids atualizados devem ter status=refunded, sem duplicata
```

## O desenho com DMS de verdade (produção)

```
SQL Server / Postgres / MySQL (origem, com CDC/binlog habilitado)
        │  full load inicial + change data capture
        ▼
AWS DMS (replication instance ou DMS Serverless)
        │  target endpoint S3: Parquet, particionado por data
        ▼
s3://evt-lakehouse-bronze/cdc/<tabela>/dt=YYYY-MM-DD/
        │  mesmo contrato, mesma dedup (janela por PK, ordem por commit_ts)
        ▼
silver → gold (inalterados)
```

Decisões que importam nesse desenho:

- **Colunas de controle do DMS.** O target S3 do DMS adiciona `Op`
  (I/U/D) e um timestamp de commit. A dedup passa a ordenar pelo
  `commit_ts` da transação — mais confiável que timestamp de aplicação — e
  o `Op = D` vira soft delete na silver (coluna `is_deleted`), nunca
  descarte silencioso: a mesma filosofia da quarentena.
- **Full load × CDC contínuo.** O DMS faz os dois na mesma task; o full
  load inicial cai como partição histórica e o CDC segue incremental. O
  reprocesso continua idempotente por partição.
- **Limites conhecidos do DMS** (para dizer em entrevista, não para
  descobrir em produção): schema drift na origem não é propagado
  automaticamente (mudou coluna, a task precisa de atenção), LOBs têm modo
  próprio com custo, e a replication instance é um recurso a dimensionar —
  o clássico "serverless até a página 2".

## Kinesis × DMS — qual caminho para qual fonte

| Fonte | Caminho | Por quê |
|---|---|---|
| Aplicação emitindo eventos | Kinesis → Firehose → bronze (`infra/nrt.tf`) | o produtor já fala evento; não há banco a capturar |
| Banco transacional legado | DMS → S3 → bronze | captura sem tocar na aplicação, full load + CDC na mesma ferramenta |
| Ambos | os dois, prefixos separados na bronze | o contrato na silver unifica — camadas existem exatamente para isso |
