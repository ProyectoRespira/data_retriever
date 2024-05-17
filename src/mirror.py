from sqlalchemy import MetaData, Table, desc, select
from sqlalchemy.exc import SQLAlchemyError
from src.models import StationsReadingsRaw
from src.database import create_postgres_session, create_postgres, create_mysql
from src.initialize_db import create_postgres_tables
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_last_measurement_id(postgres_session, station_id):
    logging.info('Starting get_last_measurement_id...')
    last_measurement = (postgres_session.query(StationsReadingsRaw)
                        .filter(StationsReadingsRaw.station_id == station_id)
                        .order_by(desc(StationsReadingsRaw.measurement_id))
                        .first())
    if last_measurement:
        logging.info(f'Last measurement ID for station {station_id}: {last_measurement.measurement_id}')
        return last_measurement.measurement_id
    else:
        logging.info(f'No previous measurements for station {station_id}')
        return 0

def select_new_records_from_origin_table(mysql_engine, table_name, last_measurement_id):
    logging.info(f'Starting select_new_records_from_origin_table where table_name = {table_name} and last_measurement_id = {last_measurement_id}')
    metadata = MetaData()
    metadata.reflect(bind=mysql_engine)
    table = Table(table_name, metadata, autoload_with=mysql_engine)
    
    column_names = [column.name.lower() for column in table.columns]
    column_expressions = [column for column in table.columns]
    query = select(*column_expressions).where(table.c.ID > last_measurement_id)

    with mysql_engine.connect() as connection:
        result = connection.execute(query)
        records_as_dicts = [dict(zip(column_names, row)) for row in result.fetchall()]

    logging.info(f'Selected {len(records_as_dicts)} new records from table {table_name}')
    return records_as_dicts

def prepare_records_for_insertion(station_id, new_records):
    logging.info('Starting prepare_records_for_insertion...')
    prepared_records = []
    for record in new_records:
        modified_record = {'measurement_id': record['id'], 'station_id': station_id, **record}
        del modified_record['id']
        prepared_records.append(StationsReadingsRaw(**modified_record))
    return prepared_records

def insert_new_data_to_target_table(postgres_session, mysql_engine):
    logging.info('Starting insert_new_data_to_target_table...')
    try:
        for station_id in range(1, 11):
            table_name = f'Estacion{station_id}'
            last_measurement_id = get_last_measurement_id(postgres_session, station_id)
            new_records = select_new_records_from_origin_table(mysql_engine, table_name, last_measurement_id)
            prepared_records = prepare_records_for_insertion(station_id, new_records)
            postgres_session.add_all(prepared_records)
            logging.info(f'Inserted {len(prepared_records)} new records for station {station_id}')
        postgres_session.commit()
        return True
    except SQLAlchemyError as e:
        logging.error(f"Error occurred: {e}")
        postgres_session.rollback()
        return False

def retrieve_data():
    logging.info('Starting retrieve_data...')
    try:
        postgres_engine = create_postgres()
        mysql_engine = create_mysql()
        create_postgres_tables(postgres_engine)

        with create_postgres_session(postgres_engine) as postgres_session:
            success = insert_new_data_to_target_table(postgres_session, mysql_engine)
            if success:
                return "Data retrieved and inserted successfully"
            else:
                return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None
    finally:
        if 'postgres_session' in locals() and postgres_session is not None:
            postgres_session.close()
        if 'mysql_engine' in locals() and mysql_engine is not None:
            mysql_engine.dispose()
