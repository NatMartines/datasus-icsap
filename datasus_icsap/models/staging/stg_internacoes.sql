with origem as (

    select * from {{ source('datasus_raw', 'internacoes') }}

)

select
    n_aih                                               as num_aih,
    ident                                               as tipo_aih,
    cnes                                                as cod_hospital,
    munic_res                                           as cod_municipio_residencia,
    munic_mov                                           as cod_municipio_hospital,
    sexo,
    raca_cor,

    case cod_idade
        when '4' then cast(idade as integer)            -- anos
        when '5' then 100 + cast(idade as integer)      -- mais de 100 anos
        else 0                                          -- dias ou meses = menos de 1 ano
    end                                                 as idade_anos,

    cast(date_parse(dt_inter, '%Y%m%d') as date)        as data_internacao,
    cast(date_parse(dt_saida, '%Y%m%d') as date)        as data_saida,
    cast(dias_perm as integer)                          as dias_permanencia,
    cast(uti_mes_to as integer)                         as dias_uti,
    car_int                                             as carater_internacao,
    diag_princ                                          as cod_cid_principal,
    proc_rea                                            as cod_procedimento,
    cast(morte as integer)                              as obito,
    cast(val_tot as decimal(18,2))                      as valor_total,
    cast(val_uti as decimal(18,2))                      as valor_uti,
    ano,
    mes

from origem