{{ config(materialized='incremental', unique_key='chave_merchant_dia') }}
-- Incremental com unique_key: a idempotencia do DELETE+COPY da carga
-- Redshift (sql/redshift_gold.sql), em forma declarativa. Na primeira
-- execucao (ou com --full-refresh) o is_incremental() e falso e o filtro
-- de dt novo nao existe.
select
    chave_merchant_dia,
    dt,
    merchant_id,
    merchant_name,
    category,
    gmv_aprovado,
    taxa_aprovacao,
    ticket_medio,
    share_pix,
    rank_categoria,
    variacao_gmv_pct
from {{ ref('stg_merchant_daily') }}
{% if is_incremental() %}
where dt > (select coalesce(max(dt), date '1900-01-01') from {{ this }})
{% endif %}
