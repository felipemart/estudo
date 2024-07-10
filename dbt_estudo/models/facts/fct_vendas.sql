{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

WITH vendas AS (

    SELECT
        v.id,
        v.data AS data_venda,
        v.id_cliente AS cliente_id,
        v.id_loja AS loja_id
    FROM
        {{ ref("stg_vendas") }}
        v
        JOIN {{ ref("dim_clientes") }} C
        ON v.id_cliente = C.id_cliente
        JOIN {{ ref("dim_lojas") }}
        l
        ON v.id_loja = l.id_loja
)
SELECT
    id,
    data_venda,
    cliente_id,
    loja_id
FROM
    vendas

{% if is_incremental() %}
WHERE
    id > (
        SELECT
            MAX(id)
        FROM
            {{ this }}
    )
{% endif %}
