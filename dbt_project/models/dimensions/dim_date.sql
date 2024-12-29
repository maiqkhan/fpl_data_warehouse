
{{  config(
        materialized='incremental'
    ) }}

with staging as (
    select * from {{ source('stg', 'dates')}}
)

select * from 

staging

{% if is_incremental() %}

where date_key >= (select coalesce(max(date_key),19000101) from {{ this }} )

{% endif %}