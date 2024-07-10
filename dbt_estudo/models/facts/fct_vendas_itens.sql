{{ config(
    materialized = 'incremental',
    unique_key = 'venda_item_id'
) }}

WITH vendas_itens AS (

    SELECT
        vi.id AS venda_item_id,
        vi.id_produtos AS produto_id,
        vi.id_vendas,
        vi.valor :: DECIMAL(
            10,
            2
        ) AS valor_pago
    FROM
        {{ ref("stg_vendas_itens") }}
        vi
        JOIN {{ ref("dim_produtos") }}
        p
        ON vi.id_produtos = p.id_produto
        JOIN {{ ref("fct_vendas") }}
        v
        ON vi.id_vendas = v.id
)
SELECT
    venda_item_id,
    produto_id,
    id_vendas,
    valor_pago
FROM
    vendas_itens

{% if is_incremental() %}
WHERE
    venda_item_id > (
        SELECT
            MAX(venda_item_id)
        FROM
            {{ this }}
    )
{% endif %}
