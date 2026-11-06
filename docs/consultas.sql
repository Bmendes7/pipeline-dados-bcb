--  série mensal com variação e média móvel (Selic e dólar, últimos 12 meses)
select
    s.nome,
    m.mes,
    round(m.ultimo_valor_mes, 4) as ultimo_valor_mes,
    round(m.variacao_mes_pct * 100, 2) as variacao_mes_pct,
    round(m.media_movel_3m, 4) as media_movel_3m
from analytics.mart_indicadores_mensais as m
join analytics.dim_serie as s using (serie_id)
where m.serie_id in (432, 1)
  and m.mes >= date_sub(date_trunc(current_date(), month), interval 12 month)
order by s.nome, m.mes;

-- star schema em ação (dólar por trimestre). Une a fato à dimensão de calendário
select
    d.ano,
    d.trimestre,
    round(avg(f.valor), 4) as dolar_medio,
    round(min(f.valor), 4) as minimo,
    round(max(f.valor), 4) as maximo
from analytics.fct_indicadores as f
join analytics.dim_data as d on d.data_id = f.data_id
where f.serie_id = 1
group by d.ano, d.trimestre
order by d.ano, d.trimestre;