-- Teste SINGULAR de reconciliacao mart x fonte: o total de GMV por dt do
-- mart tem que bater com a gold -- divergiu, alguem filtrou ou duplicou
-- linha no caminho. Retornar linhas = falhar.
with fonte as (
    select dt, sum(gmv_aprovado) as gmv
    from {{ source('gold', 'merchant_daily') }}
    group by dt
),

mart as (
    select dt, sum(gmv_aprovado) as gmv
    from {{ ref('mart_categoria_diaria') }}
    group by dt
)

select
    fonte.dt,
    fonte.gmv as gmv_fonte,
    mart.gmv as gmv_mart
from fonte
inner join mart using (dt)
where abs(coalesce(fonte.gmv, 0) - coalesce(mart.gmv, 0)) > 0.01
