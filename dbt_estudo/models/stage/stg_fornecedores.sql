{{ config(
    materialized = 'view'
) }}

WITH source AS (

    SELECT
        id,
        endereco,
        email,
        nome,
        empresa
    FROM
        {{ source(
            'sources',
            'fornecedores'
        ) }}
)
SELECT
    id,
    endereco,
    email,
    nome,
    empresa
FROM
    source
