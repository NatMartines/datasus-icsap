select
    subcat                          as cod_cid,
    try_cast(grupo as integer)      as num_grupo_icsap,
    grupo_nome                      as nome_grupo_icsap,
    diagnostico
from {{ source('datasus_raw', 'internacoes_evitaveis_lista') }}