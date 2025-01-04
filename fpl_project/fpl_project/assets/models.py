from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    Float,
    Date,
    ForeignKey,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class fpl_dates(Base):
    __tablename__ = "dates"
    __table_args__ = {"schema": "stg"}

    date_key = Column(Integer, nullable=False, primary_key=True)
    date_id = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    month_num = Column(Integer, nullable=False)
    month_name = Column(String(9), nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    day_name = Column(String(9), nullable=False)


class stg_teams(Base):
    __tablename__ = "teams"
    __table_args__ = {"schema": "stg"}

    team_key = Column(Integer, primary_key=True)
    team_id = Column(Integer, nullable=False)
    season = Column(String(7), nullable=False)
    name = Column(String, nullable=False)
    short_name = Column(String(3), nullable=False)
    strength = Column(Integer, nullable=False)
    strength_overall_home = Column(Integer, nullable=False)
    strength_overall_away = Column(Integer, nullable=False)
    strength_attack_home = Column(Integer, nullable=False)
    strength_attack_away = Column(Integer, nullable=False)
    strength_defence_home = Column(Integer, nullable=False)
    strength_defence_away = Column(Integer, nullable=False)
    extract_dt = Column(Date, nullable=False)


class stg_fixtures(Base):
    __tablename__ = "fixtures"
    __table_args__ = {"schema": "stg"}

    fixture_key = Column(Integer, primary_key=True)
    fixture_id = Column(Integer, nullable=False)
    season = Column(String(7), nullable=False)
    event = Column(Integer, nullable=False)
    finished = Column(Boolean, nullable=False)
    team_h = Column(
        Integer,
        nullable=False,
    )  # All home team IDs must exist in the teams staging table
    team_a = Column(
        Integer,
        nullable=False,
    )  # All away teams IDs must exist in the teams staging table
    kickoff_time = Column(DateTime, nullable=False)
    fixture_type = Column(String(50), nullable=False)
    extract_dt = Column(Date, nullable=False)


class stg_players(Base):
    __tablename__ = "players"
    __table_args__ = {"schema": "stg"}

    player_id = Column(Integer, nullable=False, primary_key=True)
    season = Column(String(7), nullable=False)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    web_name = Column(String(100), nullable=False)
    position = Column(String(10), nullable=False)
    price = Column(Integer, nullable=False)
    team_id = Column(Integer, nullable=False)
    extract_dt = Column(Date, nullable=False)


class stg_matches(Base):
    __tablename__ = "matches"
    __table_args__ = {"schema": "stg"}

    player_id = Column(Integer, nullable=False, primary_key=True)
    fixture_id = Column(Integer, nullable=False, primary_key=True)
    assists = Column(Integer, nullable=False)
    penalties_saved = Column(Integer, nullable=False)
    saves = Column(Integer, nullable=False)
    ict_index = Column(Float, nullable=False)
    expected_goal_involvements = Column(Float, nullable=False)
    transfers_balance = Column(Integer, nullable=False)
    total_points = Column(Integer, nullable=False)
    clean_sheets = Column(Integer, nullable=False)
    penalties_missed = Column(Integer, nullable=False)
    bonus = Column(Integer, nullable=False)
    starts = Column(Integer, nullable=False)
    expected_goals_conceded = Column(Float, nullable=False)
    selected = Column(Integer, nullable=False)
    season = Column(String(7), nullable=False)
    goals_conceded = Column(Integer, nullable=False)
    yellow_cards = Column(Integer, nullable=False)
    bps = Column(Integer, nullable=False)
    expected_goals = Column(Float, nullable=False)
    value = Column(Integer, nullable=False)
    transfers_in = Column(Integer, nullable=False)
    minutes = Column(Integer, nullable=False)
    own_goals = Column(Integer, nullable=False)
    red_cards = Column(Integer, nullable=False)
    influence = Column(Float, nullable=False)
    expected_assists = Column(Float, nullable=False)
    transfers_out = Column(Integer, nullable=False)
    extract_dt = Column(Date, nullable=False)
    goals_scored = Column(Integer, nullable=False)
    threat = Column(Float, nullable=False)
    creativity = Column(Float, nullable=False)


class dim_date(Base):
    __tablename__ = "dim_date"
    __table_args__ = {"schema": "fpl"}

    date_key = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    month_num = Column(Integer, nullable=False)
    month_name = Column(String, nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    day_name = Column(String, nullable=False)


class dim_team(Base):
    __tablename__ = "dim_team"
    __table_args__ = {"schema": "fpl"}

    team_key = Column(Integer, primary_key=True, autoincrement=True)
    team_id = Column(Integer, nullable=False)
    full_name = Column(String, nullable=False)
    short_name = Column(String, nullable=False)
    overall_strength = Column(Integer, nullable=False)
    home_overall_strength = Column(Integer, nullable=False)
    home_attack_strength = Column(Integer, nullable=False)
    home_defence_strength = Column(Integer, nullable=False)
    away_overall_strength = Column(Integer, nullable=False)
    away_attack_strength = Column(Integer, nullable=False)
    away_defence_strength = Column(Integer, nullable=False)
    effective_dt = Column(Date, nullable=False)
    expiry_dt = Column(Date, nullable=False)
    current_ind = Column(Boolean, nullable=False)


class dim_fixture(Base):
    __tablename__ = "dim_fixture"
    __table_args__ = {"schema": "fpl"}

    fixture_key = Column(Integer, primary_key=True, autoincrement=True)
    fixture_id = Column(Integer, nullable=False)
    season = Column(String, nullable=False)
    gameweek = Column(Integer, nullable=False)
    finished_ind = Column(Boolean, nullable=False)
    home_team_key = Column(
        Integer, ForeignKey(dim_team.team_key, ondelete="CASCADE"), nullable=False
    )
    away_team_key = Column(
        Integer, ForeignKey(dim_team.team_key, ondelete="CASCADE"), nullable=False
    )
    kickoff_time = Column(DateTime, nullable=False)

    fixture_type = Column(String, nullable=False)


class dim_player(Base):
    __tablename__ = "dim_player"
    __table_args__ = {"schema": "fpl"}

    player_key = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, nullable=False)
    season = Column(String, nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    web_name = Column(String, nullable=False)
    position = Column(String, nullable=False)
    price = Column(Integer, nullable=False)
    team_key = Column(
        Integer, ForeignKey(dim_team.team_key, ondelete="CASCADE"), nullable=False
    )
    effective_dt = Column(Date, nullable=False)
    expiry_dt = Column(Date, nullable=False)
    current_ind = Column(Integer, nullable=False)


class fact_match_stats(Base):
    __tablename__ = "fact_match_stats"
    __table_args__ = {"schema": "fpl"}

    fact_match_stat_key = Column(Integer, primary_key=True, autoincrement=True)
    extract_dt_key = Column(
        Integer, ForeignKey(dim_date.date_key, ondelete="CASCADE"), nullable=False
    )
    player_key = Column(
        Integer, ForeignKey(dim_player.player_key, ondelete="CASCADE"), nullable=False
    )
    fixture_key = Column(
        Integer, ForeignKey(dim_fixture.fixture_key, ondelete="CASCADE"), nullable=False
    )
    minutes = Column(Integer, nullable=False)
    starts = Column(Integer, nullable=False)
    total_points = Column(Integer, nullable=False)
    bonus_points = Column(Integer, nullable=False)
    bonus_point_system_score = Column(Integer, nullable=False)
    goals_scored = Column(Integer, nullable=False)
    penalties_missed = Column(Integer, nullable=False)
    expected_goals = Column(Float, nullable=False)
    expected_goals_diff = Column(Float, nullable=False)
    assists = Column(Integer, nullable=False)
    expected_assists = Column(Float, nullable=False)
    expected_assists_diff = Column(Float, nullable=False)
    expected_goal_involvements = Column(Float, nullable=False)
    expected_goal_involvements_diff = Column(Float, nullable=False)
    goals_conceded = Column(Integer, nullable=False)
    expected_goals_conceded = Column(Float, nullable=False)
    own_goals = Column(Integer, nullable=False)
    clean_sheets = Column(Integer, nullable=False)
    saves = Column(Integer, nullable=False)
    penalties_saved = Column(Integer, nullable=False)
    yellow_cards = Column(Integer, nullable=False)
    red_cards = Column(Integer, nullable=False)
    influence = Column(Float, nullable=False)
    creativity = Column(Float, nullable=False)
    threat = Column(Float, nullable=False)
    ict_index = Column(Float, nullable=False)
