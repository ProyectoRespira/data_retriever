import pytest
from src.database import create_postgres, create_postgres_session
from src.models import BasePostgres

# Create a PostgreSQL engine for testing
postgres_engine = create_postgres()

@pytest.fixture(scope='session')
def setup_postgres_database():
    # Create the database schema
    BasePostgres.metadata.create_all(bind=postgres_engine)
    yield
    # Drop the database schema
    BasePostgres.metadata.drop_all(bind=postgres_engine)

@pytest.fixture(scope='function')
def db_session_postgres():
    session = create_postgres_session(postgres_engine)
    yield session
    session.close()