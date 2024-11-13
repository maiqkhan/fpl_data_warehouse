import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

load_dotenv("..\.env", override=True)

server = os.getenv("SERVER")
server_port = os.getenv("SERVER_PORT")
database = os.getenv("DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")


engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{server}:{server_port}/{database}"
)

# SessionalLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_conn():
    return engine.connect()
