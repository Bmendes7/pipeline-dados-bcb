select
    to_hex(md5(concat(cast(serie_id as string), '|', cast(data_referencia as string)))) as indicador_id,
    serie_id,
    data_referencia as data_id,
    valor
from {{ ref('stg_bcb_series')}}