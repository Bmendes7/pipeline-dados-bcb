with mensal as (
    select
        serie_id,
        date_trunc(data_id, month) as mes,
        array_agg(valor order by data_id desc limit 1)[offset(0)] as ultimo_valor_mes
    from {{ ref('fct_indicadores') }}
    group by serie_id, mes
)

select
    serie_id,
    mes,
    ultimo_valor_mes,
    safe_divide(
        ultimo_valor_mes - lag(ultimo_valor_mes) over w,
        lag(ultimo_valor_mes) over w
    ) as variacao_mes_pct,
    avg(ultimo_valor_mes) over (partition by serie_id order by mes rows between 2 preceding and current row) as media_movel_3m
from mensal
window w as (partition by serie_id order by mes)