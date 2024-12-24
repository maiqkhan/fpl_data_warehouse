from dagster import (
    ConfigurableResource,
    ResourceDependency,
)
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager


class CredentialsResource(ConfigurableResource):
    """Stores credentials for accessing resources that require authentication"""

    username: str
    password: str
    server: str
    database: str
    server_port: str


class PostgresResource(ConfigurableResource):
    """Engine for connecting SQL Server databases"""

    credentials: ResourceDependency[CredentialsResource]

    def connect_to_engine(self) -> Engine:
        engine: Engine = create_engine(
            f"postgresql+psycopg2://{self.credentials.username}:{self.credentials.password}@{self.credentials.server}:{self.credentials.server_port}/{self.credentials.database}"
        )

        return engine

    @contextmanager
    def get_session(self) -> Session:

        engine = self.connect_to_engine()
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()

        try:
            yield session
        finally:
            session.close()
