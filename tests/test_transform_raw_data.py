import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from src.transform_raw_data import (
    apply_correction_factor,
    validate_date_hour,
    get_last_transformation_timestamp,
    fetch_raw_readings,
    transform_raw_readings_to_df,
    fetch_meteostat_humidity,
    update_or_insert_station_readings,
    get_aqi_25,
    get_aqi_10,
    calculate_aqi_for_station,
    transform_raw_data,
    calculate_aqi,
    fill_station_readings
)
from src.models import StationsReadingsRaw, StationReadings, ExternalData
import pandas as pd

@pytest.fixture
def mock_postgres_session(mocker):
    mock_session = mocker.patch('src.transform_raw_data.create_postgres_session')
    mock_engine = mocker.patch('src.transform_raw_data.create_postgres')
    
    session_instance = MagicMock()
    mock_session.return_value.__enter__.return_value = session_instance
    
    return session_instance

@pytest.fixture
def mock_humidity_data():
    data = {
        'date': [datetime(2024, 1, 1, i) for i in range(24)],
        'humidity': [60 + i for i in range(24)]
    }
    return pd.DataFrame(data)

@pytest.fixture
def mock_raw_readings():
    readings = [
        StationsReadingsRaw(id=1, measurement_id=1, station_id=1, fecha='01-01-2024', hora='00:00', mp2_5='25', mp1='10', mp10='50', temperatura='25', humedad='80', presion='1010', bateria='100'),
        StationsReadingsRaw(id=2, measurement_id=2, station_id=1, fecha='01-01-2024', hora='01:00', mp2_5='26', mp1='11', mp10='51', temperatura='26', humedad='82', presion='1009', bateria='100')
    ]
    return readings

def test_apply_correction_factor(mock_humidity_data):
    df = pd.DataFrame({
        'hour': [datetime(2024, 1, 1, i) for i in range(24)],
        'mp2_5': [20 + i for i in range(24)]
    })
    corrected = apply_correction_factor(df, 1, 'mp2_5', mock_humidity_data)
    assert corrected is not None
    assert len(corrected) == 24

def test_validate_date_hour():
    assert validate_date_hour('01-01-2024', '00:00') == True
    assert validate_date_hour('01-01-2024', '24:00') == False
    assert validate_date_hour(None, '00:00') == False
    assert validate_date_hour('01-01-2024', None) == False

def test_get_last_transformation_timestamp(mock_postgres_session):
    mock_postgres_session.query.return_value.filter_by.return_value.scalar.return_value = datetime(2024, 1, 1)
    result = get_last_transformation_timestamp(mock_postgres_session, 1)
    assert result == datetime(2024, 1, 1)

def test_fetch_raw_readings(mock_postgres_session, mock_raw_readings):
    mock_postgres_session.query.return_value.filter.return_value.all.return_value = mock_raw_readings
    result = fetch_raw_readings(mock_postgres_session, 1, datetime(2024, 1, 1))
    assert len(result) == len(mock_raw_readings)

def test_transform_raw_readings_to_df(mock_raw_readings):
    df = transform_raw_readings_to_df(mock_raw_readings)
    assert not df.empty
    assert 'mp2_5' in df.columns

def test_fetch_meteostat_humidity(mock_postgres_session):
    mock_postgres_session.query.return_value.filter.return_value.all.return_value = [
        ExternalData(date=datetime(2024, 1, 1, 0), humidity=60),
        ExternalData(date=datetime(2024, 1, 1, 1), humidity=62)
    ]
    result = fetch_meteostat_humidity(mock_postgres_session, datetime(2024, 1, 1))
    assert not result.empty
    assert 'humidity' in result.columns

def test_update_or_insert_station_readings(mock_postgres_session):
    hourly_readings = pd.DataFrame({
        'hour': [datetime(2024, 1, 1, i) for i in range(24)],
        'mp1': [10 + i for i in range(24)],
        'mp2_5': [20 + i for i in range(24)],
        'mp10': [30 + i for i in range(24)],
        'temperatura': [25 for _ in range(24)],
        'humedad': [50 for _ in range(24)],
        'presion': [1010 for _ in range(24)]
    })
    update_or_insert_station_readings(mock_postgres_session, 1, hourly_readings)
    assert mock_postgres_session.commit.called

def test_get_aqi_25():
    assert get_aqi_25(10) == 42
    assert get_aqi_25(50) == 104
    assert get_aqi_25(200) == 245

def test_get_aqi_10():
    assert get_aqi_10(10) == 9
    assert get_aqi_10(100) == 83
    assert get_aqi_10(300) == 198

def test_calculate_aqi_for_station(mock_postgres_session):
    mock_postgres_session.query.return_value.filter.return_value.all.return_value = [
        StationReadings(station=1, date=datetime(2024, 1, 1, i), pm2_5=30, pm10=40) for i in range(24)
    ]
    calculate_aqi_for_station(mock_postgres_session, 1)
    assert mock_postgres_session.commit.called

def test_transform_raw_data(mock_postgres_session, mock_raw_readings, mock_humidity_data, mocker):
    mocker.patch('src.transform_raw_data.get_last_transformation_timestamp', return_value=datetime(2024, 1, 1))
    mocker.patch('src.transform_raw_data.fetch_raw_readings', return_value=mock_raw_readings)
    mocker.patch('src.transform_raw_data.fetch_meteostat_humidity', return_value=mock_humidity_data)
    transform_raw_data(mock_postgres_session)
    assert mock_postgres_session.commit.called

def test_calculate_aqi(mock_postgres_session, mocker):
    mocker.patch('src.transform_raw_data.calculate_aqi_for_station')
    mock_postgres_session.query.return_value.filter.return_value.all.return_value = [
        StationReadings(station=1, date=datetime(2024, 1, 1, i), pm2_5=30, pm10=40) for i in range(24)
    ]
    calculate_aqi(mock_postgres_session)
    assert mock_postgres_session.commit.called

def test_fill_station_readings(mocker):
    mocker.patch('src.transform_raw_data.create_postgres')
    mocker.patch('src.transform_raw_data.create_postgres_session')
    mocker.patch('src.transform_raw_data.transform_raw_data')
    mocker.patch('src.transform_raw_data.calculate_aqi')
    
    result = fill_station_readings()
    assert result == True
