with internacoes as (
    select * from {{ ref('int_internacoes_enriquecidas') }}
    where tipo_aih = '1'    -- regra do Dia 3: só AIH normal
)

select
    -- chave da linha
    num_aih,

    -- chaves das dimensões
    cod_municipio_residencia,
    cod_hospital,
    cod_cid_principal,
    data_internacao,

    -- atributos para filtro
    ano,
    mes,
    sexo,
    raca_cor,
    faixa_etaria,
    idade_anos,
    carater_internacao,

    -- medidas
    valor_total,
    valor_uti,
    dias_permanencia,
    dias_uti,
    obito,
    eh_evitavel

from internacoes