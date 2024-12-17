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

from database import Base


class stg_teams(Base):
    __tablename__ = "teams"
    __table_args__ = {"schema": "stg"}

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    shortName = Column(String, nullable=False)
    strength = Column(Integer, nullable=False)
    strengthOverallHome = Column(Integer, nullable=False)
    strengthOverallAway = Column(Integer, nullable=False)
    strengthAttackHome = Column(Integer, nullable=False)
    strengthAttackAway = Column(Integer, nullable=False)
    strengthDefenceHome = Column(Integer, nullable=False)
    strengthDefenceAway = Column(Integer, nullable=False)
    extract_dt = Column(Date, nullable=False)


class stg_fixtures(Base):
    __tablename__ = "fixtures"
    __table_args__ = {"schema": "stg"}

    id = Column(Integer, primary_key=True)
    season = Column(String, nullable=False)
    event = Column(
        Integer, nullable=True
    )  # Certain fixtures may be cancelled and not yet moved to another gameweek, so null must be allowed
    finished = Column(
        Boolean, nullable=False
    )  # A fixture is either finished = True, or currently being played / not played yet = False
    teamH = Column(
        Integer,
        nullable=False,
    )  # All home team IDs must exist in the teams staging table
    teamA = Column(
        Integer,
        nullable=False,
    )  # All away teams IDs must exist in the teams staging table
    fixtureType = Column(String, nullable=False)
    localKickoffTime = Column(
        DateTime, nullable=True
    )  # Certain fixtures may be cancelled and not yet rescheduled, so null must be allowed
    localKickoffMonth = Column(String, nullable=True)
    extract_dt = Column(Date, nullable=False)


class stg_player_data(Base):
    __tablename__ = "player_data"
    __table_args__ = {"schema": "stg"}

    id = Column(Integer, primary_key=True)
    firstName = Column(String, nullable=False)
    secondName = Column(String, nullable=False)
    webName = Column(String, nullable=False)
    elementType = Column(Integer, nullable=False)
    fixture = Column(Integer, primary_key=True)
    opponentTeam = Column(Integer, nullable=False)
    totalPoints = Column(Integer, nullable=False)
    wasHome = Column(Boolean, nullable=False)
    teamHScore = Column(Integer, nullable=False)
    teamAScore = Column(Integer, nullable=False)
    round = Column(Integer, nullable=False)
    minutes = Column(Integer, nullable=False)
    goalsScored = Column(Integer, nullable=False)
    assists = Column(Integer, nullable=False)
    cleanSheets = Column(Integer, nullable=False)
    goalsConceded = Column(Integer, nullable=False)
    ownGoals = Column(Integer, nullable=False)
    penaltiesSaved = Column(Integer, nullable=False)
    penaltiesMissed = Column(Integer, nullable=False)
    yellowCards = Column(Integer, nullable=False)
    redCards = Column(Integer, nullable=False)
    saves = Column(Integer, nullable=False)
    bonus = Column(Integer, nullable=False)
    bps = Column(Integer, nullable=False)
    influence = Column(Float, nullable=False)
    creativity = Column(Float, nullable=False)
    threat = Column(Float, nullable=False)
    ictIndex = Column(Float, nullable=False)
    starts = Column(Integer, nullable=False)
    expectedGoals = Column(Float, nullable=False)
    expectedAssists = Column(Float, nullable=False)
    expectedGoalInvolvements = Column(Float, nullable=False)
    expectedGoalsConceded = Column(Float, nullable=False)
    value = Column(Float, nullable=False)
    selected = Column(Integer, nullable=False)
    transfersIn = Column(Integer, nullable=False)
    transfersOut = Column(Integer, nullable=False)
    extract_dt = Column(Date, nullable=False)


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
    gameweek = Column(
        Integer
    )  # Certain fixtures may be cancelled and not yet moved to another gameweek, so null must be allowed
    fixture_type = Column(String, nullable=False)
    finished_ind = Column(Boolean, nullable=False)
    local_kickoff_datetime = Column(
        DateTime, nullable=True
    )  # Certain fixtures may be cancelled and not yet rescheduled, so null must be allowed
    local_kickoff_month = Column(
        String, nullable=True
    )  # Certain fixtures may be cancelled and not yet rescheduled, so null must be allowed
    team_key = Column(
        Integer, ForeignKey(dim_team.team_key, ondelete="CASCADE"), nullable=False
    )
    opponent_key = Column(
        Integer, ForeignKey(dim_team.team_key, ondelete="CASCADE"), nullable=False
    )
    was_home_ind = Column(Boolean, nullable=False)


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
    price = Column(Float, nullable=False)
    team_key = Column(
        Integer, ForeignKey(dim_team.team_key, ondelete="CASCADE"), nullable=False
    )
    effective_dt = Column(Date, nullable=False)
    expiry_dt = Column(Date, nullable=False)
    current_ind = Column(Boolean, nullable=False)


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
