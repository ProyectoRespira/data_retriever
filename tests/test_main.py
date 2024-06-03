import pytest
from unittest.mock import patch
from src.old_code.main_old import main

def test_main_success():
    with patch('main.retrieve_data', return_value=True) as mock_retrieve_data, \
         patch('main.fill_meteostat_data', return_value=True) as mock_fill_meteostat_data, \
         patch('main.fill_station_readings', return_value=True) as mock_fill_station_readings:
        
        result = main()
        
        mock_retrieve_data.assert_called_once()
        mock_fill_meteostat_data.assert_called_once()
        mock_fill_station_readings.assert_called_once()
        assert result == "Station readings filled successfully"

def test_main_retrieve_data_failure():
    with patch('main.retrieve_data', return_value=False) as mock_retrieve_data, \
         patch('main.fill_meteostat_data') as mock_fill_meteostat_data, \
         patch('main.fill_station_readings') as mock_fill_station_readings:
        
        result = main()
        
        mock_retrieve_data.assert_called_once()
        mock_fill_meteostat_data.assert_not_called()
        mock_fill_station_readings.assert_not_called()
        assert result == "Error: Retrieving data from FIUNA failed"

def test_main_fill_meteostat_data_failure():
    with patch('main.retrieve_data', return_value=True) as mock_retrieve_data, \
         patch('main.fill_meteostat_data', return_value=False) as mock_fill_meteostat_data, \
         patch('main.fill_station_readings') as mock_fill_station_readings:
        
        result = main()
        
        mock_retrieve_data.assert_called_once()
        mock_fill_meteostat_data.assert_called_once()
        mock_fill_station_readings.assert_not_called()
        assert result == "Error: Filling meteostat data failed"

def test_main_fill_station_readings_failure():
    with patch('main.retrieve_data', return_value=True) as mock_retrieve_data, \
         patch('main.fill_meteostat_data', return_value=True) as mock_fill_meteostat_data, \
         patch('main.fill_station_readings', return_value=False) as mock_fill_station_readings:
        
        result = main()
        
        mock_retrieve_data.assert_called_once()
        mock_fill_meteostat_data.assert_called_once()
        mock_fill_station_readings.assert_called_once()
        assert result == "Error: Filling station readings failed"

