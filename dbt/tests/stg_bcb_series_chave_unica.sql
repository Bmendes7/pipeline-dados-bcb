select serie_id, data_referencia, count(*) as repeticoes
from {{ ref('stg_bcb_series') }}
group by serie_id, data_referencia
having count(*) > 1