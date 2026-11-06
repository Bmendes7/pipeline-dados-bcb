select
    cast(serie_id as int64) as serie_id,
    cast(data as date) as data_referencia,
    cast(valor as numeric) as valor,
    data_carga
from {{ source('raw', 'bcb_series')}}