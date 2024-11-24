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


class Stg_Teams(Base):
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


class Stg_Fixtures(Base):
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


class Stg_Player_Data(Base):
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


class DIM_TEAM(Base):
    __tablename__ = "DIM_TEAM"
    __table_args__ = {"schema": "FPL"}

    TEAM_KEY = Column(Integer, primary_key=True, autoincrement=True)
    TEAM_ID = Column(Integer, nullable=False)
    FULL_NAME = Column(String, nullable=False)
    SHORT_NAME = Column(String, nullable=False)
    OVERALL_STRENGTH = Column(Integer, nullable=False)
    HOME_OVERALL_STRENGTH = Column(Integer, nullable=False)
    HOME_ATTACK_STRENGTH = Column(Integer, nullable=False)
    HOME_DEFENCE_STRENGTH = Column(Integer, nullable=False)
    AWAY_OVERALL_STRENGTH = Column(Integer, nullable=False)
    AWAY_ATTACK_STRENGTH = Column(Integer, nullable=False)
    AWAY_DEFENCE_STRENGTH = Column(Integer, nullable=False)
    EFFECTIVE_DT = Column(Date, nullable=False)
    EXPIRY_DT = Column(Date, nullable=False)
    CURRENT_IND = Column(Boolean, nullable=False)


class DIM_FIXTURE(Base):
    __tablename__ = "DIM_FIXTURE"
    __table_args__ = {"schema": "FPL"}

    FIXTURE_KEY = Column(Integer, primary_key=True, autoincrement=True)
    FIXTURE_ID = Column(Integer, nullable=False)
    SEASON = Column(String, nullable=False)
    GAMEWEEK = Column(
        Integer
    )  # Certain fixtures may be cancelled and not yet moved to another gameweek, so null must be allowed
    FIXTURE_TYPE = Column(String, nullable=False)
    FINISHED_IND = Column(Boolean, nullable=False)
    LOCAL_KICKOFF_DATETIME = Column(
        DateTime, nullable=True
    )  # Certain fixtures may be cancelled and not yet rescheduled, so null must be allowed
    LOCAL_KICKOFF_MONTH = Column(
        String, nullable=True
    )  # Certain fixtures may be cancelled and not yet rescheduled, so null must be allowed
    TEAM_KEY = Column(
        Integer, ForeignKey(DIM_TEAM.TEAM_KEY, ondelete="CASCADE"), nullable=False
    )
    OPPONENT_KEY = Column(
        Integer, ForeignKey(DIM_TEAM.TEAM_KEY, ondelete="CASCADE"), nullable=False
    )
    WAS_HOME = Column(Boolean, nullable=False)


class DIM_PLAYER(Base):
    __tablename__ = "DIM_PLAYER"
    __table_args__ = {"schema": "FPL"}

    PLAYER_KEY = Column(Integer, primary_key=True, autoincrement=True)
    PLAYER_ID = Column(Integer, nullable=False)
    FIRST_NAME = Column(String, nullable=False)
    LAST_NAME = Column(String, nullable=False)
    WEB_NAME = Column(String, nullable=False)
    POSITION = Column(String, nullable=False)
    PRICE = Column(Float, nullable=False)
    TEAM_KEY = Column(
        Integer, ForeignKey(DIM_TEAM.TEAM_KEY, ondelete="CASCADE"), nullable=False
    )
    EFFECTIVE_DT = Column(Date, nullable=False)
    EXPIRY_DT = Column(Date, nullable=False)
    CURRENT_IND = Column(Boolean, nullable=False)


class FACT_MATCH_STATS(Base):
    __tablename__ = "FACT_MATCH_STATS"
    __table_args__ = {"schema": "FPL"}

    FACT_MATCH_STAT_KEY = Column(Integer, primary_key=True, autoincrement=True)
    PLAYER_KEY = Column(
        Integer, ForeignKey(DIM_PLAYER.PLAYER_KEY, ondelete="CASCADE"), nullable=False
    )
    FIXTURE_KEY = Column(
        Integer, ForeignKey(DIM_FIXTURE.FIXTURE_KEY, ondelete="CASCADE"), nullable=False
    )
    MINUTES = Column(Integer, nullable=False)
    STARTS = Column(Integer, nullable=False)
    TOTAL_POINTS = Column(Integer, nullable=False)
    BONUS_POINTS = Column(Integer, nullable=False)
    BONUS_POINTS_SYSTEM_SCORE = Column(Integer, nullable=False)
    GOALS_SCORED = Column(Integer, nullable=False)
    PENALTIES_MISSED = Column(Integer, nullable=False)
    EXPECTED_GOALS = Column(Float, nullable=False)
    EXPECTED_GOAL_DIFF = Column(Float, nullable=False)
    ASSISTS = Column(Integer, nullable=False)
    EXPECTED_ASSISTS = Column(Float, nullable=False)
    EXPECTED_ASSIST_DIFF = Column(Float, nullable=False)
    EXPECTED_GOAL_INVOLVEMENTS = Column(Float, nullable=False)
    EXPECTED_GOAL_INVOLVEMENT_DIFF = Column(Float, nullable=False)
    GOALS_CONCEDED = Column(Integer, nullable=False)
    EXPECTED_GOALS_CONCEDED = Column(Float, nullable=False)
    OWN_GOALS = Column(Integer, nullable=False)
    CLEAN_SHEETS = Column(Integer, nullable=False)
    SAVES = Column(Integer, nullable=False)
    PENALTIES_SAVED = Column(Integer, nullable=False)
    YELLOW_CARDS = Column(Integer, nullable=False)
    RED_CARDS = Column(Integer, nullable=False)
    INFLUENCE = Column(Float, nullable=False)
    CREATIVITY = Column(Float, nullable=False)
    THREAT = Column(Float, nullable=False)
    ICT_INDEX = Column(Float, nullable=False)
