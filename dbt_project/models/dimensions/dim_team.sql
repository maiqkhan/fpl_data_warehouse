
{{  config(
        materialized='incremental',
        unique_key='team_key',
        incremental_strategy='merge',
        merge_exclude_columns = ['team_id', 'season', 'name', 'short_name'],
    ) }}

with staging as (
    select * from {{ source('stg', 'teams')}}
)

select 
team_key
,team_id
,season
,"name" as full_name
,short_name
,strength as strength_overall
,strength_overall_home
,strength_overall_away
,strength_attack_home
,strength_attack_away
,strength_defence_home
,strength_defence_away
from 

staging

{% if is_incremental() %}

where team_key > (select coalesce(max(team_key),1900011) from {{ this }} )

{% endif %}