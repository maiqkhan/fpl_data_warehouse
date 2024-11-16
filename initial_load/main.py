from sqlalchemy.schema import CreateSchema
from database import get_conn, engine
import models
import yaml
import os


with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

print(config)

# db_conn = get_conn()

# with db_conn as connection:
#     for db_schema in ["FPL", "stg"]:
#         connection.execute(CreateSchema(db_schema, if_not_exists=True))
#         connection.commit()

# models.Base.metadata.create_all(bind=engine)

for model in [models.Stg_Teams, models.Stg_Fixtures, models.Stg_Player_Data]:

    pass
