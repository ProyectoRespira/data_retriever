import pytest
from datetime import datetime
from unittest.mock import MagicMock
from src.meteostat_data import fill_meteostat_data
from sqlalchemy.orm import Session
import pandas as pd

@pytest.fixture
def mock_postgres_session(mocker):
    mock_session = mocker.patch('src.meteostat_data.create_postgres_session')
    
    session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = session_instance
    
    return session_instance

@pytest.fixture
def mock_meteostat_data(mocker):
    mock_hourly = mocker.patch('src.meteostat_data.Hourly')
    
    mock_data = pd.DataFrame({
        'temp': [20, 21, 19],
        'rhum': [85, 80, 75],
        'pres': [1010, 1005, 1000],
        'wspd': [5, 10, 15],
        'wdir': [90, 180, 270],
    }, index=pd.date_range('2023-05-01', periods=3, freq='H'))
    
    mock_hourly.return_value.fetch.return_value = mock_data
    return mock_data

def test_fill_meteostat_data_success(mock_postgres_session, mock_meteostat_data):
    mock_postgres_session.query.return_value.count.return_value = 0
    mock_postgres_session.query.return_value.scalar.side_effect = [
        datetime(2023, 5, 1, 0, 0, 0),
        None  # get_last_meteostat_timestamp returns None since there's no data
    ]
    
    result = fill_meteostat_data()
    assert result == True
    assert mock_postgres_session.commit.called

def test_fill_meteostat_data_no_new_data(mock_postgres_session, mock_meteostat_data):
    mock_postgres_session.query.return_value.count.return_value = 1
    mock_postgres_session.query.return_value.scalar.side_effect = [
        datetime(2023, 5, 1, 2, 0, 0)  # Last timestamp already up to date
    ]
    
    result = fill_meteostat_data()
    assert result == True
    assert not mock_postgres_session.commit.called

def test_fill_meteostat_data_failure(mock_postgres_session, mock_meteostat_data):
    mock_postgres_session.query.side_effect = Exception("DB Error")
    
    result = fill_meteostat_data()
    assert result == False
    assert not mock_postgres_session.commit.called