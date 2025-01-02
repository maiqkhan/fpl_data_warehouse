{{  config(
        materialized='incremental',
        unique_key=['player_id', 'season', 'effective_dt'],
        incremental_strategy='merge',
        merge_update_columns = ['expiry_dt', 'current_ind'],
        on_schema_change='ignore'
    ) }}

WITH source_data as (
    SELECT 
        player_source.player_id
        ,player_source.season
        ,player_source.first_name
        ,player_source.last_name 
        ,player_source.web_name
        ,player_source.position
        ,player_source.price
        ,dim_team.team_key
        ,player_source.extract_dt as effective_dt
        ,'2261-12-31' as expiry_dt
        ,1 as current_ind 


    FROM {{ source('stg', 'players') }} as player_source
    LEFT JOIN {{ ref('dim_team')}} as dim_team on player_source.team_id = dim_team.team_id and player_source.season = dim_team.season
),

current_player_data AS (
    SELECT 
        player_key
        ,player_id
        ,season
        ,first_name
        ,last_name
        ,web_name
        ,position
        ,price
        ,team_key
        ,effective_dt
        ,expiry_dt
        ,current_ind
        FROM {{ this }}
        WHERE current_ind = 1
),

records_to_expire as (
    SELECT
        current_player_data.player_key  
        ,current_player_data.player_id
        ,current_player_data.season
        ,current_player_data.first_name
        ,current_player_data.last_name
        ,current_player_data.web_name
        ,current_player_data.position
        ,current_player_data.price
        ,current_player_data.team_key
        ,current_player_data.effective_dt
        ,source_data.effective_dt - 1 as expiry_dt
        ,0 as current_ind


    FROM current_player_data
    INNER JOIN source_data on current_player_data.player_id = source_data.player_id
        and current_player_data.season = source_data.season
    WHERE current_player_data.position <> source_data.position
    OR current_player_data.price <> source_data.price
    OR current_player_data.team_key <> source_data.team_key
),

new_player_data as (
    SELECT
        nextval(pg_get_serial_sequence('{{ this }}', 'player_key')) as player_key
        ,player_id
        ,season
        ,first_name
        ,last_name
        ,web_name
        ,position
        ,price
        ,team_key
        ,effective_dt
        ,TO_DATE('2261-12-31', 'YYYY-MM-DD') as expiry_dt
        ,1 as current_ind


    FROM source_data WHERE player_id NOT IN (SELECT player_id from current_player_data)
),

existing_player_new_data as (
    SELECT
        nextval(pg_get_serial_sequence('{{ this }}', 'player_key')) as player_key
        ,source_data.player_id
        ,source_data.season
        ,source_data.first_name
        ,source_data.last_name
        ,source_data.web_name
        ,source_data.position
        ,source_data.price
        ,source_data.team_key
        ,source_data.effective_dt
        ,TO_DATE('2261-12-31', 'YYYY-MM-DD') as expiry_dt
        ,1 as current_ind


    FROM source_data
    INNER JOIN current_player_data on source_data.player_id = current_player_data.player_id
        and source_data.season = current_player_data.season
    WHERE source_data.position <> current_player_data.position
    OR source_data.price <> current_player_data.price
    OR source_data.team_key <> current_player_data.team_key
)

SELECT * FROM records_to_expire
UNION ALL 
SELECT * FROM existing_player_new_data
UNION ALL 
SELECT * FROM new_player_data


{% if is_incremental() %}

where effective_dt > (select max(effective_dt) from {{ this }} )

{% endif %}
