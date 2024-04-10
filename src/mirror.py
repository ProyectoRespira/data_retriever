from sqlalchemy import MetaData, Table, desc, select, create_engine
from sqlalchemy.exc import SQLAlchemyError
from src.models import StationsReadingsRaw
from src.database import create_postgres_session, create_postgres, create_mysql
from src.initialize_db import create_postgres_tables

def get_last_measurement_id(postgres_session, station_id):
    last_measurement = postgres_session.query(StationsReadingsRaw).filter(StationsReadingsRaw.station_id == station_id).order_by(desc(StationsReadingsRaw.measurement_id)).first()
    if last_measurement:
        print('last measurement id gotten!')
        return last_measurement.measurement_id
    else:
        print('no last measurement yet')
        return 0 # if no records, starts from 1

def select_new_records_from_origin_table(mysql_engine, table_name, last_measurement_id):
    metadata = MetaData()
    metadata.reflect(bind=mysql_engine)
    table = Table(table_name, metadata, autoload_with=mysql_engine)
    
    column_names = [column.name.lower() for column in table.columns]
    column_expressions = [column for column in table.columns]

    #query = select(*column_expressions).select_from(table).where(table.c.ID > last_measurement_id)
    query = select(*column_expressions).where(table.c.ID > last_measurement_id)

    with mysql_engine.connect() as connection:
        result = connection.execute(query) 

        records_as_dicts = [dict(zip(column_names, row)) for row in result.fetchall()]

    print(f'new data selected from table {table_name}')
    
    return records_as_dicts

def insert_new_data_to_target_table(postgres_session, mysql_engine):
    try:
        for station_id in range(1,11):
            table_name = f'Estacion{station_id}'
            last_measurement_id = get_last_measurement_id(postgres_session, station_id)
            new_records = select_new_records_from_origin_table(mysql_engine, table_name, last_measurement_id)
            print(f'getting new records from table {table_name}...')
            for record in new_records:
                modified_record = {'measurement_id':record['id'], 'station_id': station_id, **record}
                del modified_record['id']

                postgres_session.add(StationsReadingsRaw(**modified_record))

        postgres_session.commit()

    except SQLAlchemyError as e:
        print('Error occurred: ', e)
        postgres_session.rollback()


def retrieve_data():
    try:
        postgres_engine = create_postgres()
        with create_postgres_session(postgres_engine) as postgres_session:
            mysql_engine = create_mysql()

            create_postgres_tables(postgres_engine=postgres_engine)
            insert_new_data_to_target_table(mysql_engine=mysql_engine, postgres_session=postgres_session)
    except Exception as e:
        print("An error occurred:", e)
    
