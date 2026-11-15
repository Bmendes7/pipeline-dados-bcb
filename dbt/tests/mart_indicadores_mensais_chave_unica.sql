select serie_id, mes, count(*) as repeticoes
from {{ ref('mart_indicadores_mensais') }}
group by serie_id, mes
having count(*) > 1
