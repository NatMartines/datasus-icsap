select
    cod_municipio,
    cod_municipio_ibge7,
    nome_municipio,
    uf_sigla,
    regiao_nome,
    regiao_intermediaria,
    regiao_imediata
from {{ source('datasus_raw', 'municipios') }}