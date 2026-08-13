-- DDL de sql/redshift_gold.sql ADAPTADA para Postgres - com cada diferenca
-- comentada, porque a adaptacao e a parte que ensina (Redshift descende do
-- Postgres 8; o que sobrou em comum e o que divergiu diz muito sobre MPP).
--
-- O que saiu e por que:
--   DISTSTYLE/DISTKEY  -> nao existem: Postgres e single-node; a "distribuicao"
--                         do Redshift vira aqui escolha de indice.
--   COMPOUND SORTKEY   -> vira um indice B-tree (dt, category). O SORTKEY
--                         ordena FISICAMENTE os blocos (zone maps pulam
--                         leitura); o indice e uma estrutura A PARTE que
--                         aponta pras linhas - mesmo objetivo (pular
--                         leitura por dt), mecanismo diferente.
--   COPY ... PARQUET   -> Postgres nao le Parquet nativo; a carga vira
--                         \copy de CSV exportado do lake.
--   ANALYZE            -> este sobreviveu identico: estatistica para o
--                         planejador e ideia comum aos dois mundos.

CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.merchant_daily;

CREATE TABLE analytics.merchant_daily (
    dt                DATE          NOT NULL,
    merchant_id       VARCHAR(32)   NOT NULL,
    merchant_name     VARCHAR(128),
    category          VARCHAR(64),
    state             CHAR(2),
    tx_total          BIGINT,
    tx_aprovadas      BIGINT,
    clientes_unicos   BIGINT,
    taxa_aprovacao    DECIMAL(9,4),
    gmv_aprovado      DECIMAL(18,2),
    valor_estornado   DECIMAL(18,2),
    ticket_medio      DECIMAL(18,2),
    share_pix         DECIMAL(9,4),
    rank_categoria    INTEGER,
    gmv_dia_anterior  DECIMAL(18,2),
    variacao_gmv_pct  DECIMAL(9,4)
);

-- O equivalente mental do COMPOUND SORTKEY (dt, category): praticamente
-- toda consulta filtra por dt primeiro.
CREATE INDEX idx_merchant_daily_dt_category
    ON analytics.merchant_daily (dt, category);
