from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import TableTracking

from src.config import config

# PostgreSQL connection details
POSTGRES_CONFIG = config(section='postgresql')

# MySQL connection details
MYSQL_CONFIG = config(section='mysql')

# Access individual parameters
POSTGRES_HOST = POSTGRES_CONFIG['host']
POSTGRES_USER = POSTGRES_CONFIG['user']
POSTGRES_PASSWORD = POSTGRES_CONFIG['password']
POSTGRES_DATABASE = POSTGRES_CONFIG['database']

MYSQL_HOST = MYSQL_CONFIG['host']
MYSQL_USER = MYSQL_CONFIG['user']
MYSQL_PASSWORD = MYSQL_CONFIG['password']
MYSQL_DATABASE = MYSQL_CONFIG['database']
MYSQL_TABLES = MYSQL_CONFIG['tables']

# Create SQLAlchemy engines for PostgreSQL and MySQL
postgres_engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DATABASE}')
mysql_engine = create_engine(f'mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}')

# Create session makers for PostgreSQL and MySQL
Session = sessionmaker(bind=postgres_engine)

def create_postgres_session():
    Session.configure(bind=postgres_engine)
    return Session()

def create_mysql_session():
    Session.configure(bind=mysql_engine)
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