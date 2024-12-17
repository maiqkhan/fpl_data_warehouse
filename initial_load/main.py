from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker
from database import get_conn, engine
from api_handler import requestHandler
from utils import dict_to_model_inst
from load_tables import table_ingestor
import models
import yaml
import os


with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)


db_conn = get_conn()

with db_conn as connection:
    for db_schema in ["fpl", "stg"]:
        connection.execute(CreateSchema(db_schema, if_not_exists=True))
        connection.commit()

models.Base.metadata.create_all(bind=engine)

Session = sessionmaker(bind=engine)
session = Session()


with Session() as session:

    for data_model in [models.stg_teams, models.stg_fixtures, models.stg_player_data]:

        data_segment = config["TABLE_ROUTE_DICT"][data_model.__tablename__]
        print(data_segment)

        data_processor = requestHandler(
            base_url=config["BASE_URL"],
            endpoint=(
                data_segment
                if data_segment in config["DEDICATED_ENDPOINTS"]
                else "bootstrap-static/"
            ),
            keep_keys=config["KEEP_KEY_DICT"].get(data_segment),
            subset_dict_key=(
                data_segment if data_segment in config["BOOTSTRAP_SUBSETS"] else None
            ),
        )

        if data_segment == "elements":
            raw_data = data_processor.extract_transform_data()
            raw_data_to_load = []
            for player in raw_data:
                print(player["webName"])
                player_id = player["id"]

                element_summary_processor = requestHandler(
                    base_url=config["BASE_URL"],
                    endpoint=f"element-summary/{player_id}/",
                    keep_keys=config["KEEP_KEY_DICT"].get("element-summary"),
                    subset_dict_key="history",
                )

                raw_summary_data = element_summary_processor.extract_transform_data()

                for player_fixture in raw_summary_data:
                    player_fixture |= player
                    raw_data_to_load.append(player_fixture)
        else:
            raw_data_to_load = data_processor.extract_transform_data()

        data_insts = dict_to_model_inst(raw_data_to_load, data_model)

        session.add_all(data_insts)

    session.commit()

    dwh_ingestor = table_ingestor(db_session=session)

    dwh_ingestor.load_date_table()

    dwh_ingestor.load_team_table()

    dwh_ingestor.load_fixtures_table()

    dwh_ingestor.load_players_table()

    dwh_ingestor.load_match_stats_table()
