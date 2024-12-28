
{{ config(materialized='table') }}

with staging as (
    select * from {{ source('stg', 'dates')}}
)

select * from staging