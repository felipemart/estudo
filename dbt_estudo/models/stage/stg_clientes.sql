{{ config(materialized='view') }}
with source as (
    select
        id,
        initcap(nome) as nome,
        data
    from {{ source('sources', 'clientes') }}
)

select
    id,
    nome,
    data
from source