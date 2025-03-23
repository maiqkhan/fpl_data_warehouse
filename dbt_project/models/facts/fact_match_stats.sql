{{  config(
        materialized='incremental',
        unique_key=['fixture_key', 'player_key'],
        incremental_strategy='merge',
        on_schema_change='fail'
    ) }}

With staging as (
select  
d_date.date_key as extract_dt_key
,d_player.player_key as player_key 
,d_fixture.fixture_key as fixture_key
,starts
,assists
,penalties_saved
,saves
,ict_index
,expected_goal_involvements
,transfers_balance
,total_points
,clean_sheets
,penalties_missed
,bonus
,expected_goals_conceded
,selected
,goals_conceded
,yellow_cards
,bps
,expected_goals
,transfers_in
,minutes
,own_goals
,red_cards
,influence
,expected_assists
,transfers_out
,extract_dt
,goals_scored
,threat
,creativity
,mng_win
,mng_draw
,mng_loss
,mng_underdog_win
,mng_underdog_draw
,mng_clean_sheets
,mng_goals_scored
FROM {{ source('stg', 'matches')}} as source 
LEFT JOIN {{ ref('dim_date')}} as d_date on source.extract_dt = d_date.date_id
LEFT JOIN {{ ref('dim_fixture')}} as d_fixture on source.fixture_id = d_fixture.fixture_id 
    and source.season = d_fixture.season
LEFT JOIN {{ ref('dim_player')}} as d_player on source.player_id = d_player.player_id
    and source.season = d_player.season 
    and source.extract_dt between d_player.effective_dt and d_player.expiry_dt

)

SELECT 
extract_dt_key
,player_key 
,fixture_key 
,starts as start_ind
,"minutes" as minutes_played
,selected as selected_by
,bps as bonus_points_system_value
,bonus as bonus_points
,total_points
,goals_scored
,penalties_missed
,expected_goals
,assists
,expected_assists
,expected_goal_involvements
,clean_sheets as clean_sheet_ind
,saves
,penalties_saved
,own_goals
,goals_conceded
,expected_goals_conceded
,yellow_cards
,red_cards
,influence
,threat
,CAST(creativity AS FLOAT) as creativity
,ict_index
,transfers_in
,transfers_out
,transfers_balance
,mng_win
,mng_draw
,mng_loss
,mng_underdog_win
,mng_underdog_draw
,mng_clean_sheets
,mng_goals_scored

FROM staging

{% if is_incremental() %}

where extract_dt_key > (select coalesce(max(extract_dt_key),1900011) from {{ this }} )

{% endif %}