-- Staging nao transforma: a regra de negocio agregada ja foi resolvida no
-- Spark SQL da gold (jobs/silver_to_gold.py) -- duplica-la aqui criaria
-- duas fontes de verdade. So entra o que protege os marts: chave explicita
-- e nomes estaveis por cima da fonte declarada.
select
    -- chave declarada pro teste unique (a "PK logica" dt + merchant_id)
    dt || '|' || merchant_id as chave_merchant_dia,
    dt,
    merchant_id,
    merchant_name,
    category,
    state,
    tx_total,
    tx_aprovadas,
    clientes_unicos,
    taxa_aprovacao,
    gmv_aprovado,
    valor_estornado,
    ticket_medio,
    share_pix,
    rank_categoria,
    gmv_dia_anterior,
    variacao_gmv_pct
from {{ source('gold', 'merchant_daily') }}
