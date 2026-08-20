-- A pergunta que a area de negocio faz primeiro: qual categoria puxou o
-- dia? TABLE recalculada por completo a cada run -- barata, porque agrega
-- a partir da view de staging; o post-hook ANALYZE roda em seguida.
select
    dt,
    category,
    count(*)                                                 as merchants_ativos,
    sum(gmv_aprovado)                                        as gmv_aprovado,
    sum(tx_total)                                            as tx_total,
    sum(tx_aprovadas)                                        as tx_aprovadas,
    case
        when sum(tx_total) > 0
        then sum(tx_aprovadas)::numeric / sum(tx_total)
    end                                                      as taxa_aprovacao,
    sum(gmv_aprovado) / nullif(sum(tx_aprovadas), 0)         as ticket_medio
from {{ ref('stg_merchant_daily') }}
group by dt, category
