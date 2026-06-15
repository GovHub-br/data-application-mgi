with

    ids_from_empenhos as (
        select distinct contrato_id::text as contrato_id, upper(nota_empenho) as ne
        from {{ ref("empenhos") }}
    ),

    ids_from_faturas as (
        select distinct contrato_id::text as contrato_id, upper(numero_empenho) as ne
        from {{ ref("faturas") }}
    ),

    ids_table as (
        select *
        from ids_from_empenhos e
        full join ids_from_faturas f using (contrato_id, ne)
    ),

    contratos as (
        select
            id::text as contrato_id,
            categoria,
            case when length(numero) = 12 then numero end as ne,
            regexp_replace(processo, '[^0-9]', '', 'g') as processo,
            regexp_replace(fornecedor_cnpj_cpf_idgener, '[/.-]', '', 'g') as cnpj_cpf,
            case
                when codigo_modalidade in ('05', '06')
                then
                    concat(
                        contratante__orgao__unidade_gestora__codigo,
                        codigo_modalidade,
                        replace(numero, '/', '')
                    )
                when codigo_modalidade = '07'
                then
                    concat(
                        contratante__orgao__unidade_gestora__codigo,
                        codigo_modalidade,
                        replace(licitacao_numero, '/', '')
                    )
            end as info_complementar
        from {{ ref("contratos") }}
    ),

    identificadores as (
        select
            c.contrato_id,
            c.categoria,
            c.processo,
            c.cnpj_cpf,
            c.info_complementar,
            coalesce(i.ne, c.ne) as ne
        from contratos as c
        full join ids_table as i on c.contrato_id = i.contrato_id
    )

--
select *
from identificadores
