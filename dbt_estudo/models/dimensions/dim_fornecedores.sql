{{ config(
    materialized = 'table'
) }}

SELECT
    id AS id_fornecedor,
    endereco,
    email AS email_fornecedor,
    nome AS contato_fornecedor,
    empresa AS fornecedor
FROM
    {{ ref('stg_fornecedores') }}
