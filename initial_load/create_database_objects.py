from database import get_conn
from sqlalchemy.schema import CreateSchema

db_conn = get_conn()

with db_conn as connection:
    for db_schema in ["FPL", "stg"]:
        connection.execute(CreateSchema(db_schema, if_not_exists=True))
        connection.commit()
