-- dólar e Selic não podem ser zero ou negativos (o IPCA pode, por causa de deflação)
select serie_id, data_referencia, valor
from {{ ref('stg_bcb_series') }}
where serie_id in (1, 432) and valor <= 0
