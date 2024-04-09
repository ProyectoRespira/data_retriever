from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import TableTracking

import os
from dotenv import load_dotenv

try:
    load_dotenv()
except:
    raise "Error loading .env file"


def create_postgres():
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    
    postgres_engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DATABASE}')

    return postgres_engine


def create_mysql():
    MYSQL_HOST = os.getenv('MYSQL_HOST')
    MYSQL_USER = os.getenv('MYSQL_USER')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
    MYSQL_TABLES = os.getenv('MYSQL_TABLES')

    mysql_engine = create_engine(f'mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}')

    return mysql_engine


def create_postgres_session(postgres_engine):
    Session = sessionmaker(bind=postgres_engine)
    return Session()


def create_mysql_session(mysql_engine):
    Session = sessionmaker(bind=mysql_engine)
    return Session()


def get_last_mirrored_id(table_name):
    # Create a session for PostgreSQL
    session = create_postgres_session()

    try:
        # Query the TableTracking table for the last mirrored ID of the specified table
        record = session.query(TableTracking).filter_by(table_name=table_name).first()
        if record:
            return record.last_mirrored_id
        else:
            return 0  # Default to 0 if no record found (assuming ID starts from 1)
    finally:
        session.close()

def update_last_mirrored_id(table_name, last_mirrored_id):
    # Create a session for PostgreSQL
    session = create_postgres_session()

    try:
        # Query the TableTracking table for the record of the specified table
        record = session.query(TableTracking).filter_by(table_name=table_name).first()
        if record:
            # Update the last mirrored ID
            record.last_mirrored_id = last_mirrored_id
        else:
            # If no record exists, create a new one
            new_record = TableTracking(table_name=table_name, last_mirrored_id=last_mirrored_id)
            session.add(new_record)

        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def get_tables(tables = MYSQL_TABLES):
    return tables