from src.models import BasePostgres
from sqlalchemy import create_engine

def create_postgres_tables(postgres_engine):
    BasePostgres.metadata.create_all(postgres_engine)