select
    cod_municipio,
    nome_municipio,
    uf_sigla,
    regiao_nome,
    regiao_intermediaria,
    regiao_imediata
from {{ ref('stg_municipios') }}