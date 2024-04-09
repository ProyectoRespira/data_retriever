from src.models import BasePostgres
from src.database import create_postgres

def create_postgres_tables():
    engine = create_postgres()

    BasePostgres.metadata.create_all(engine)