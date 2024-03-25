from sqlalchemy import create_engine, MetaData, Table
from src.database import config
####### NOT WORKING - CAN'T CONNECT TO REMOTE SERVER 4 TESTING ########

def create_missing_tables():
    # Load database configuration
    postgres_config = config(section='postgresql')
    mysql_config = config(section='mysql')

    # Create SQLAlchemy engines for PostgreSQL and MySQL
    postgres_engine = create_engine(f"postgresql+psycopg2://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}/{postgres_config['database']}")
    mysql_engine = create_engine(f"mysql+mysqlconnector://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}/{mysql_config['database']}")

    # Reflect existing tables in PostgreSQL
    meta = MetaData()
    meta.reflect(bind=postgres_engine)

    existing_postgres_tables = meta.tables.keys()

    # Get table names from MySQL
    postgres_table_names = postgres_config['tables']

    # Check for missing tables and create them in PostgreSQL
    for table_name in postgres_table_names:
        if table_name not in existing_postgres_tables:
            # Reflect the table structure from MySQL
            table = Table(table_name, meta, autoload_with=mysql_engine)
            # Create the table in PostgreSQL
            table.create(bind=postgres_engine)

    

if __name__ == "__main__":
    create_missing_tables()