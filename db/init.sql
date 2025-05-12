CREATE SCHEMA IF NOT EXISTS fpl;
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE stg.dates (
    date_key INTEGER PRIMARY KEY,
    date_id DATE NOT NULL,
    year INTEGER NOT NULL,
    month_num INTEGER NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(9) NOT NULL
);

CREATE TABLE stg.teams(
    team_key INTEGER PRIMARY KEY,
    team_id INTEGER NOT NULL,
    season VARCHAR(7) NOT NULL,
    name VARCHAR(50) NOT NULL,
    short_name VARCHAR(3) NOT NULL,
    strength INTEGER NOT NULL,
    strength_overall_home INTEGER NOT NULL,
    strength_overall_away INTEGER NOT NULL,
    strength_attack_home INTEGER NOT NULL,
    strength_attack_away INTEGER NOT NULL,
    strength_defence_home INTEGER NOT NULL,
    strength_defence_away INTEGER NOT NULL,
    extract_dt DATE NOT NULL
);
    
CREATE TABLE stg.fixtures (
    fixture_key INTEGER PRIMARY KEY,
    fixture_id INTEGER NOT NULL,
    season VARCHAR(7) NOT NULL,
    event INTEGER NOT NULL,
    finished BOOLEAN NOT NULL,
    team_h INTEGER NOT NULL,
    team_a INTEGER NOT NULL,
    kickoff_time TIMESTAMP NOT NULL,
    fixture_type VARCHAR(50) NOT NULL,
    extract_dt DATE NOT NULL
);

CREATE TABLE stg.players (
    player_id INTEGER PRIMARY KEY,
    season VARCHAR(7) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    web_name VARCHAR(100) NOT NULL,
    position VARCHAR(50) NOT NULL,
    price INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    extract_dt DATE NOT NULL
);

CREATE TABLE stg.matches(
    player_id INTEGER NOT NULL,
    fixture_id INTEGER NOT NULL,
    assists INTEGER NOT NULL,
    penalties_saved INTEGER NOT NULL,
    saves INTEGER NOT NULL,
    ict_index FLOAT NOT NULL,
    expected_goal_involvements FLOAT NOT NULL,
    transfers_balance INTEGER NOT NULL,
    total_points INTEGER NOT NULL,
    clean_sheets INTEGER NOT NULL,
    penalties_missed INTEGER NOT NULL,
    bonus INTEGER NOT NULL,
    starts INTEGER NOT NULL,
    expected_goals_conceded FLOAT NOT NULL,
    selected INTEGER NOT NULL,
    season VARCHAR(7) NOT NULL,
    goals_conceded INTEGER NOT NULL,
    yellow_cards INTEGER NOT NULL,
    bps INTEGER NOT NULL,
    expected_goals FLOAT NOT NULL,
    value INTEGER NOT NULL,
    transfers_in INTEGER NOT NULL,
    minutes INTEGER NOT NULL,
    own_goals INTEGER NOT NULL,
    red_cards INTEGER NOT NULL,
    influence FLOAT NOT NULL,
    expected_assists FLOAT NOT NULL,
    transfers_out INTEGER NOT NULL,
    extract_dt DATE NOT NULL,
    goals_scored INTEGER NOT NULL,
    threat FLOAT NOT NULL,
    creativity INTEGER NOT NULL,
    mng_win INTEGER NOT NULL,
    mng_draw INTEGER NOT NULL,
    mng_loss INTEGER NOT NULL,
    mng_underdog_win INTEGER NOT NULL,
    mng_underdog_draw INTEGER NOT NULL,
    mng_clean_sheets INTEGER NOT NULL,
    mng_goals_scored INTEGER NOT NULL,
    PRIMARY KEY (player_id, fixture_id)
);

CREATE TABLE stg.players_initial(
    player_id INTEGER NOT NULL, 
    season VARCHAR(7) NOT NULL, 
    team_key INTEGER NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL, 
    web_name VARCHAR(100) NOT NULL, 
    position VARCHAR(50) NOT NULL, 
    price INTEGER NOT NULL, 
    effective_dt DATE NOT NULL, 
    expiry_dt DATE NOT NULL,  
    current_ind INTEGER NOT NULL
);

CREATE TABLE fpl.dim_player(
    player_key SERIAL PRIMARY KEY,
    player_Id INTEGER NOT NULL,
    season VARCHAR(7) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL, 
    web_name VARCHAR(100) NOT NULL, 
    position VARCHAR(50) NOT NULL,  
    price INTEGER NOT NULL, 
    team_key INTEGER NOT NULL,
    effective_dt DATE NOT NULL, 
    expiry_dt DATE NOT NULL,  
    current_ind INTEGER NOT NULL    
);


CREATE DATABASE dagster_metadata;