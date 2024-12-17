from sqlalchemy.orm.session import Session
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy import func
from models import (
    stg_teams,
    stg_fixtures,
    stg_player_data,
    dim_date,
    dim_team,
    dim_fixture,
    dim_player,
    fact_match_stats,
)
from sqlalchemy import func, literal
from datetime import datetime as dt
from typing import List, Tuple, LiteralString
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta


class table_ingestor:
    def __init__(
        self,
        db_session: Session,
    ) -> None:
        self.db_session = db_session

    def generate_position(self, element_type: str) -> str:
        player_dict = {1: "Goalkeeper", 2: "Defender", 3: "Midfielder", 4: "Forward"}

        try:
            return player_dict[element_type]
        except KeyError:
            raise Exception(
                f"The following element type does not have a mapped position associated with it: {element_type}"
            )

    def initial_player_scd_table(
        self,
        player_fixtures: List[Tuple],
        single_player_fixtures: List[int] | None = [],
    ) -> List[Tuple]:
        player_id = 0
        season = ""
        first_name = ""
        second_name = ""
        web_name = ""
        position = ""
        price = 0.0
        fixture_dt = ""
        team_key = 0

        scd_record_lst = []

        single_fixtures = [
            match for match in player_fixtures if match[0] in single_player_fixtures
        ]
        multi_fixtures = [
            match for match in player_fixtures if match[0] not in single_player_fixtures
        ]
        total_player_fixtures = len(multi_fixtures)

        stg_player_data.id,
        dim_fixture.season,
        stg_player_data.firstName,
        stg_player_data.secondName,
        stg_player_data.webName,
        stg_player_data.elementType,
        stg_player_data.value,
        dim_fixture.local_kickoff_datetime,
        dim_fixture.team_key,

        for match_stats in single_fixtures:
            scd_record = [
                match_stats[0],
                match_stats[1],
                match_stats[2],
                match_stats[3],
                match_stats[4],
                self.generate_position(match_stats[5]),
                match_stats[6],
                match_stats[8],
                match_stats[7],
                datetime(year=2261, month=12, day=31),
                1,
            ]

            scd_record_lst.append(scd_record)

        for idx, match_stats in enumerate(multi_fixtures):
            if match_stats[0] != player_id:
                if player_id != 0:
                    scd_record = [
                        player_id,
                        season,
                        first_name,
                        second_name,
                        web_name,
                        position,
                        price,
                        team_key,
                        fixture_dt,
                        datetime(year=2261, month=12, day=31),
                        1,
                    ]

                    scd_record_lst.append(scd_record)

                player_id = match_stats[0]
                season = match_stats[1]
                first_name = match_stats[2]
                second_name = match_stats[3]
                web_name = match_stats[4]
                position = self.generate_position(match_stats[5])
                price = match_stats[6]
                fixture_dt = match_stats[7]
                team_key = match_stats[8]

                continue

            elif (
                match_stats[2] != first_name
                or match_stats[3] != second_name
                or match_stats[4] != web_name
                or self.generate_position(match_stats[5]) != position
                or match_stats[6] != price
                or match_stats[8] != team_key
            ):
                eff_dt = fixture_dt
                exp_dt = match_stats[7] + timedelta(days=-1)

                scd_record = [
                    player_id,
                    season,
                    first_name,
                    second_name,
                    web_name,
                    position,
                    price,
                    team_key,
                    eff_dt,
                    exp_dt,
                    0,
                ]
                scd_record_lst.append(scd_record)

                if idx == total_player_fixtures - 1:
                    scd_record = [
                        match_stats[0],
                        match_stats[1],
                        match_stats[2],
                        match_stats[3],
                        match_stats[4],
                        self.generate_position(match_stats[5]),
                        match_stats[6],
                        match_stats[8],
                        match_stats[7],
                        datetime(year=2261, month=12, day=31),
                        1,
                    ]
                    scd_record_lst.append(scd_record)

                season = match_stats[1]
                first_name = match_stats[2]
                second_name = match_stats[3]
                web_name = match_stats[4]
                position = self.generate_position(match_stats[5])
                price = match_stats[6]
                fixture_dt = match_stats[7]
                team_key = match_stats[8]

            elif idx == total_player_fixtures - 1:
                scd_record = [
                    player_id,
                    season,
                    first_name,
                    second_name,
                    web_name,
                    position,
                    price,
                    team_key,
                    fixture_dt,
                    datetime(year=2261, month=12, day=31),
                    1,
                ]
                scd_record_lst.append(scd_record)

            else:
                continue

        return sorted(scd_record_lst, key=lambda player_fixture: player_fixture[0])

    def load_table(
        self, table: DeclarativeMeta, data: List[Tuple], identity_col: str
    ) -> None:

        table_cols = [c.name for c in table.__table__.columns if c.name != identity_col]

        table_insts = [table(**dict(zip(table_cols, inst))) for inst in data]

        self.db_session.add_all(table_insts)
        self.db_session.commit()

    def load_date_table(self) -> None:

        month_dict = {
            1: "January",
            2: "February",
            3: "March",
            4: "April",
            5: "May",
            6: "June",
            7: "July",
            8: "August",
            9: "September",
            10: "October",
            11: "November",
            12: "December",
        }

        day_dict = {
            1: "Monday",
            2: "Tuesday",
            3: "Wednesday",
            4: "Thursday",
            5: "Friday",
            6: "Saturday",
            7: "Sunday",
        }

        today = datetime.now().date()
        five_years_later = today + relativedelta(years=5)

        date_list = []
        current_date = today

        while current_date <= five_years_later:

            date = current_date
            date_key = int(datetime.strftime(current_date, "%Y%m%d"))
            year = current_date.year
            month_num = current_date.month
            month_name = month_dict.get(month_num)
            day_of_month = current_date.day
            day_of_week = current_date.weekday() + 1
            day_name = day_dict.get(day_of_week)

            date_list.append(
                (
                    date_key,
                    date,
                    year,
                    month_num,
                    month_name,
                    day_of_month,
                    day_of_week,
                    day_name,
                )
            )

            current_date += timedelta(days=1)

        self.load_table(
            table=dim_date,
            data=date_list,
            identity_col="",
        )

    def load_team_table(self) -> None:

        teams_in_season = (
            self.db_session.query(
                stg_teams.id,
                stg_teams.name,
                stg_teams.shortName,
                stg_teams.strength,
                stg_teams.strengthOverallHome,
                stg_teams.strengthAttackHome,
                stg_teams.strengthDefenceHome,
                stg_teams.strengthOverallAway,
                stg_teams.strengthAttackAway,
                stg_teams.strengthDefenceAway,
                stg_teams.extract_dt,
                literal(dt(2261, 12, 31)),
                literal(1),
            )
            .order_by(stg_teams.id)
            .all()
        )

        if len(teams_in_season) != 20:
            raise Exception(
                f"Expected 20 teams from the staging table, received {len(teams_in_season)}"
            )

        self.load_table(
            table=dim_team,
            data=teams_in_season,
            identity_col="team_key",
        )

        # dim_team_cols = [
        #     c.name for c in dim_team.__table__.columns if c.name != "team_key"
        # ]

        # dim_team_insts = [
        #     dim_team(**dict(zip(dim_team_cols, team))) for team in teams_in_season
        # ]

        # self.db_session.add_all(dim_team_insts)
        # self.db_session.commit()

    def load_fixtures_table(self) -> None:
        season_home_fixtures = (
            self.db_session.query(
                stg_fixtures.id,
                stg_fixtures.season,
                stg_fixtures.event,
                stg_fixtures.fixtureType,
                stg_fixtures.finished,
                stg_fixtures.localKickoffTime,
                stg_fixtures.localKickoffMonth,
                stg_fixtures.teamH,
                stg_fixtures.teamA,
                literal(1),
            )
            .order_by(stg_fixtures.id)
            .all()
        )

        season_away_fixtures = (
            self.db_session.query(
                stg_fixtures.id,
                stg_fixtures.season,
                stg_fixtures.event,
                stg_fixtures.fixtureType,
                stg_fixtures.finished,
                stg_fixtures.localKickoffTime,
                stg_fixtures.localKickoffMonth,
                stg_fixtures.teamA,
                stg_fixtures.teamH,
                literal(0),
            )
            .order_by(stg_fixtures.id)
            .all()
        )

        season_fixtures = season_home_fixtures + season_away_fixtures

        self.load_table(
            table=dim_fixture,
            data=season_fixtures,
            identity_col="fixture_key",
        )

        # dim_fixture_cols = [
        #     c.name for c in dim_fixture.__table__.columns if c.name != "fixture_key"
        # ]

        # dim_fixture_insts = [
        #     dim_fixture(**dict(zip(dim_fixture_cols, fixture)))
        #     for fixture in season_fixtures
        # ]

        # self.db_session.add_all(dim_fixture_insts)
        # self.db_session.commit()

    def load_players_table(self) -> None:

        season_home_fixtures = (
            self.db_session.query(
                stg_player_data.id,
                dim_fixture.season,
                stg_player_data.firstName,
                stg_player_data.secondName,
                stg_player_data.webName,
                stg_player_data.elementType,
                stg_player_data.value,
                dim_fixture.local_kickoff_datetime,
                dim_fixture.team_key,
            )
            .filter(
                stg_player_data.fixture == dim_fixture.fixture_id,
                stg_player_data.opponentTeam == dim_fixture.opponent_key,
            )
            .order_by(stg_player_data.id, dim_fixture.local_kickoff_datetime)
            .all()
        )

        single_match_players = (
            self.db_session.query(stg_player_data.id)
            # .filter(stg_player_data.id == 681)
            .group_by(stg_player_data.id)
            .having(func.count(stg_player_data.id) == 1)
            .all()
        )

        single_match_player_lst = (
            [player[0] for player in single_match_players]
            if single_match_players
            else []
        )

        season_cd_fixtures = self.initial_player_scd_table(
            season_home_fixtures, single_match_player_lst
        )

        self.load_table(
            table=dim_player, data=season_cd_fixtures, identity_col="player_key"
        )

        # dim_player_cols = [
        #     c.name for c in dim_player.__table__.columns if c.name != "player_key"
        # ]

        # dim_player_insts = [
        #     dim_player(**dict(zip(dim_player_cols, player_match)))
        #     for player_match in season_cd_fixtures
        # ]

        # self.db_session.add_all(dim_player_insts)
        # self.db_session.commit()

    def load_match_stats_table(self) -> None:

        today_dt_key = int(datetime.today().strftime("%Y%m%d"))

        match_stats_fixtures = (
            self.db_session.query(
                literal(today_dt_key),
                dim_player.player_key,
                dim_fixture.fixture_key,
                stg_player_data.minutes,
                stg_player_data.starts,
                stg_player_data.totalPoints,
                stg_player_data.bonus,
                stg_player_data.bps,
                stg_player_data.goalsScored,
                stg_player_data.penaltiesMissed,
                stg_player_data.expectedGoals,
                (stg_player_data.goalsScored - stg_player_data.expectedGoals),
                stg_player_data.assists,
                stg_player_data.expectedAssists,
                (stg_player_data.assists - stg_player_data.expectedAssists),
                stg_player_data.expectedGoalInvolvements,
                (
                    stg_player_data.goalsScored
                    + stg_player_data.assists
                    - stg_player_data.expectedGoalInvolvements
                ),
                stg_player_data.goalsConceded,
                stg_player_data.expectedGoalsConceded,
                stg_player_data.ownGoals,
                stg_player_data.cleanSheets,
                stg_player_data.saves,
                stg_player_data.penaltiesSaved,
                stg_player_data.yellowCards,
                stg_player_data.redCards,
                stg_player_data.influence,
                stg_player_data.creativity,
                stg_player_data.threat,
                stg_player_data.ictIndex,
            )
            .filter(
                stg_player_data.fixture == dim_fixture.fixture_id,
                stg_player_data.opponentTeam == dim_fixture.opponent_key,
                stg_player_data.id == dim_player.player_id,
                dim_fixture.local_kickoff_datetime >= dim_player.effective_dt,
                dim_fixture.local_kickoff_datetime <= dim_player.expiry_dt,
            )
            .order_by(dim_fixture.local_kickoff_datetime, stg_player_data.id)
            .all()
        )

        self.load_table(
            table=fact_match_stats,
            data=match_stats_fixtures,
            identity_col="fact_match_stat_key",
        )

        # fact_match_cols = [
        #     c.name
        #     for c in fact_match_stats.__table__.columns
        #     if c.name != "fact_match_stat_key"
        # ]

        # fact_match_insts = [
        #     fact_match_stats(**dict(zip(fact_match_cols, player_match_stats)))
        #     for player_match_stats in match_stats_fixtures
        # ]

        # self.db_session.add_all(fact_match_insts)
        # self.db_session.commit()
