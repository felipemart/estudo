{{ config(
    materialized = 'table'
) }}

SELECT
    id AS id_cliente,
    nome AS nome_cliente,
    DATA
FROM
    {{ ref('stg_clientes') }}
