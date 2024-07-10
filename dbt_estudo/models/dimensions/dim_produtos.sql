{{ config(
    materialized = 'table'
) }}

SELECT
    id AS id_produto,
    descricao AS descricao_produto,
    valor :: DECIMAL(
        10,
        2
    ) AS valor_base
FROM
    {{ ref('stg_produtos') }}
