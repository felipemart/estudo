{{ config(materialized='view') }}
with source as (
    select
        id,
        descricao
    from {{ source('sources', 'lojas') }}
)

select
    id,
    descricao
from source