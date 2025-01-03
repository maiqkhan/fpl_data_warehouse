
{{  config(
        materialized='incremental',
        unique_key='fixture_key',
        incremental_strategy='merge',
        merge_exclude_columns = ['fixture_id', 'season'],
        on_schema_change='fail'
    ) }}

with staging as (
    select 
    fixture_source.fixture_key 
    ,fixture_source.fixture_id 
    ,fixture_source.season 
    ,fixture_source.event as gameweek 
    ,fixture_source.finished as finished_ind
    ,home_team.team_key as home_team_key
    ,away_team.team_key as away_team_key
    ,fixture_source.kickoff_time
    ,fixture_source.fixture_type    
    
    FROM {{ source('stg', 'fixtures')}} as fixture_source
    LEFT JOIN {{ ref('dim_team')}} as home_team on fixture_source.team_h = home_team.team_id and fixture_source.season = home_team.season
    LEFT JOIN {{ ref('dim_team')}} as away_team on fixture_source.team_a = away_team.team_id and fixture_source.season = away_team.season
)

select 
fixture_key
,fixture_id
,season
,gameweek
,finished_ind
,home_team_key
,away_team_key
,kickoff_time
,fixture_type
from 

staging

{% if is_incremental() %}

where fixture_key > (select coalesce(max(fixture_key),1900011) from {{ this }} )

{% endif %}