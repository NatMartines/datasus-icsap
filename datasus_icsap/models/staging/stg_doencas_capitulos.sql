select
    subcat                          as cod_cid,
    try_cast(numcap as integer)     as num_capitulo,
    capitulo                        as nome_capitulo
from {{ source('datasus_raw', 'doencas_capitulos') }}