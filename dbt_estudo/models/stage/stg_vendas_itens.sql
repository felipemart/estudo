{{ config(
    materialized = 'view'
) }}

WITH source AS (

    SELECT
        id,
        id_produtos,
        id_vendas,
        valor :: DECIMAL(
            10,
            2
        ) AS valor
    FROM
        {{ source(
            'sources',
            'vendas_itens'
        ) }}
)
SELECT
    id,
    id_produtos,
    id_vendas,
    valor
FROM
    source
