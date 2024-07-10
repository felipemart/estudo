{{ config(
    materialized = 'view'
) }}

WITH source AS (

    SELECT
        id,
        DATA,
        id_cliente,
        id_loja
    FROM
        {{ source(
            'sources',
            'vendas'
        ) }}
)
SELECT
    id,
    DATA,
    id_cliente,
    id_loja
FROM
    source
