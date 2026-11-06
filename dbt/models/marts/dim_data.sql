select
    d as data_id,
    extract(year from d) as ano,
    extract(quarter from d) as trimestre,
    extract(month from d) as mes,
    format_date('%B', d) as nome_mes,
    date_trunc(d, month) as primeiro_dia_mes,
    format_date('%A', d) as dia_semana,
    extract(dayofweek from d) in (1,7) as eh_fim_de_semana
from unnest(generate_date_array('2010-01-01', '2030-12-31')) as d