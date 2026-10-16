select
    subcat          as cod_cid,
    grupo           as nome_grupo,
    grupo_inicio,
    grupo_fim
from {{ source('datasus_raw', 'doencas_grupos') }}