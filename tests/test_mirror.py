import pytest
from unittest.mock import MagicMock, patch
from src.mirror import retrieve_data, get_last_measurement_id, select_new_records_from_origin_table, prepare_records_for_insertion, insert_new_data_to_target_table
from src.models import StationsReadingsRaw
from sqlalchemy.exc import SQLAlchemyError

@pytest.fixture
def mock_postgres_session(mocker):
    mock_session = mocker.patch('src.mirror.create_postgres_session')
    session_instance = MagicMock()
    mock_session.return_value.__enter__.return_value = session_instance
    return session_instance

@pytest.fixture
def mock_mysql_engine(mocker):
    return mocker.patch('src.mirror.create_mysql')

@pytest.fixture
def mock_new_mysql_records():
    return [
        {'id': 1, 'fecha': '01-01-2023', 'hora': '0:00', 'mp1': '10', 'mp2_5': '25', 'mp10': '50', 'temperatura': '25', 'humedad': '80', 'presion': '1010', 'bateria': '96'},
        {'id': 2, 'fecha': '01-01-2023', 'hora': '1:00', 'mp1': '11', 'mp2_5': '26', 'mp10': '51', 'temperatura': '26 ', 'humedad': '82', 'presion': '1009', 'bateria': '96'}
    ]

@pytest.fixture
def mock_new_records():
    return [
        {'id': 1, 'measurement_id': 1, 'station_id' : 1, 'fecha': '01-01-2023', 'hora': '00:00', 'mp2_5': '25', 'mp1': '10', 'mp10': '50', 'temperatura': '25', 'humedad': '80', 'presion': '1010', 'bateria': '96'},
        {'id': 2, 'measurement_id': 1, 'station_id' : 1, 'fecha': '01-01-2023', 'hora': '01:00', 'mp2_5': '26', 'mp1': '11', 'mp10': '51', 'temperatura': '26', 'humedad': '82', 'presion': '1009', 'bateria': '96'}
    ]

@pytest.fixture
def mock_prepared_records():
    return [
        StationsReadingsRaw(measurement_id=1, station_id=1, fecha='01-01-2023', hora='00:00', mp2_5='25', mp1='10', mp10='50', temperatura='25', humedad='80', presion='1010', bateria='100'),
        StationsReadingsRaw(measurement_id=2, station_id=1, fecha='01-01-2023', hora='01:00', mp2_5='26', mp1='11', mp10='51', temperatura='26', humedad='82', presion='1009', bateria='100')
    ]

def test_get_last_measurement_id(mock_postgres_session):
    mock_postgres_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = MagicMock(measurement_id=123)
    result = get_last_measurement_id(mock_postgres_session, 1)
    assert result == 123

def test_select_new_records_from_origin_table(mock_mysql_engine, mock_new_mysql_records, mock_new_records):
    mock_mysql_engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = mock_new_mysql_records
    result = select_new_records_from_origin_table(mock_mysql_engine, 'Estacion1', 0)
    assert len(result) == len(mock_new_records)
    assert result[0]['mp2_5'] == '25'

def test_prepare_records_for_insertion(mock_new_records):
    result = prepare_records_for_insertion(1, mock_new_records)
    assert len(result) == 2
    assert result[0].measurement_id == 1
    assert result[0].station_id == 1
    assert result[0].mp2_5 == '25'

def test_insert_new_data_to_target_table_success(mock_postgres_session, mock_mysql_engine, mock_new_records, mock_prepared_records, mocker):
    mocker.patch('src.mirror.get_last_measurement_id', return_value=0)
    mocker.patch('src.mirror.select_new_records_from_origin_table', return_value=mock_new_records)
    mocker.patch('src.mirror.prepare_records_for_insertion', return_value=mock_prepared_records)
    result = insert_new_data_to_target_table(mock_postgres_session, mock_mysql_engine)
    assert result
    assert mock_postgres_session.commit.called

def test_insert_new_data_to_target_table_failure(mock_postgres_session, mock_mysql_engine, mocker):
    mocker.patch('src.mirror.get_last_measurement_id', return_value=0)
    mocker.patch('src.mirror.select_new_records_from_origin_table', side_effect=SQLAlchemyError("DB Error"))
    result = insert_new_data_to_target_table(mock_postgres_session, mock_mysql_engine)
    assert not result
    assert mock_postgres_session.rollback.called

def test_retrieve_data_success(mock_postgres_session, mock_mysql_engine, mock_postgres_engine, mocker):
    mocker.patch('src.mirror.create_postgres_tables')
    mocker.patch('src.mirror.insert_new_data_to_target_table', return_value=True)
    result = retrieve_data()
    assert result == "Data retrieved and inserted successfully"

def test_retrieve_data_failure(mock_postgres_session, mock_mysql_engine, mock_postgres_engine, mocker):
    mocker.patch('src.mirror.create_postgres_tables')
    mocker.patch('src.mirror.insert_new_data_to_target_table', return_value=False)
    result = retrieve_data()
    assert not result
