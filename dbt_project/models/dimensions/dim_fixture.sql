
{{  config(
        materialized='incremental',
        unique_key='fixture_key',
        incremental_strategy='merge',
        merge_exclude_columns = ['fixture_id', 'season'],
        on_schema_change='fail'
    ) }}

with staging as (
    select * from {{ source('stg', 'fixtures')}}
)

select 
fixture_key
,fixture_id
,season
,"event" as gameweek
,finished as finished_ind
,team_h
,team_a
,kickoff_time
,fixture_type
from 

staging

{% if is_incremental() %}

where fixture_key > (select coalesce(max(fixture_key),1900011) from {{ this }} )

{% endif %}