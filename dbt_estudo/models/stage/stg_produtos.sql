{{ config(materialized='view') }}
with source as (
    select
        id,
        descricao,
        valor
    from {{ source('sources', 'produtos') }}
)

select
    id,
    descricao,
    valor
from source