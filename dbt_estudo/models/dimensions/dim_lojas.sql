{{ config(
    materialized = 'table'
) }}

SELECT
    id AS id_loja,
    descricao AS descricao_loja
FROM
    {{ ref('stg_lojas') }}
