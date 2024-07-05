from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, VARCHAR, Boolean
from datetime import datetime

BasePostgres = declarative_base()

class Regions(BasePostgres):
    __tablename__ = 'regions'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    region_code = Column(String, unique=True)
    # area
    bbox = Column(String)
    # status
    has_weather_data = Column(Boolean)
    has_pattern_station = Column(Boolean)

    def get_region_bbox(self, session, region_code):
        bbox = session.query(self.bbox).filter(
            self.region_code == region_code
        ).scalar()
        return bbox


class Stations(BasePostgres): # Same as Django
    __tablename__ = 'stations'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    region = Column(String, ForeignKey('regions.region_code'), name = 'region') 
    region_rel = relationship('Regions')
    is_station_on = Column(Boolean)
    is_pattern_station = Column(Boolean)

    @property
    def get_pattern_station_ids(self):
        pass
    
    @property
    def get_fiuna_station_ids(self):
        pass

    @property
    def get_region_code(self):
        pass
    
    # agregar is_station_on (variable de status)
    
    # About region: in Django it looks like this 
    # region = models.CharField(max_length = 100, 
    #                           choices = Region.choices, 
    #                           default = Region.ASUNCION)
    # where there's a class Region with TextChoices

class WeatherStations(BasePostgres):
    __tablename__ = 'weather_stations'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    region = Column(String, ForeignKey('regions.region_code'), name = 'region')
    region_rel = relationship('Regions')


#### Classes for readings from external DBs ######

class StationsReadingsRaw(BasePostgres): # Copy of Raw Data from Origin DB 
    '''
    Raw readings from FIUNA
    '''
    __tablename__ = 'station_readings_raw'

    id = Column(Integer, primary_key=True)
    measurement_id = Column(Integer)
    station_id = Column(Integer, ForeignKey('stations.id'))
    fecha = Column(VARCHAR) # All data from Origin DB comes as VARCHAR
    hora = Column(VARCHAR)
    mp2_5 = Column(VARCHAR)
    mp1 = Column(VARCHAR)
    mp10 = Column(VARCHAR)
    temperatura = Column(VARCHAR)
    humedad = Column(VARCHAR)
    presion = Column(VARCHAR)
    bateria = Column(VARCHAR)
    # filled_data flag

    @property
    def date(self):
        try: 
            return datetime.strptime(self.fecha + ' ' + self.hora, '%d-%m-%Y %H:%M')
        except:
            return None
        
class WeatherReadings(BasePostgres):
    '''
    Readings from Weather Stations. Used for inference and calibrating PM sensors
    Source - Meteostat: https://dev.meteostat.net/python/hourly.html#api
    For Asunción: Silvio Pettirossi Airport
    '''
    __tablename__ = 'weather_readings'
    id = Column(Integer, primary_key=True)
    weather_station = Column(Integer, ForeignKey('weather_stations.id'), name = 'weather_station')
    weather_station_rel = relationship('WeatherStations')
    
    date = Column(DateTime)
    temperature = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)
    wind_speed = Column(Float)
    wind_dir_cos = Column(Float)
    wind_dir_sin = Column(Float)



####### Classes for transformed readings for usage ##########

class StationReadings(BasePostgres): # Same as Django
    '''
    # Clean readings from FIUNA and Pattern Stations. Used for inference.

    # Pattern Station readings for a specific region. Used to calculate calibration factors.
      Pattern Station in Asunción: US Embassy: https://www.airnow.gov/international/us-embassies-and-consulates/#Paraguay$Asuncion
    '''
    __tablename__ ='station_readings'

    id = Column(Integer, primary_key=True)
    station = Column(Integer, ForeignKey('stations.id'), name = 'station')
    station_rel = relationship('Stations')
    date = Column(DateTime)

    # PM
    pm1 = Column(Float)
    pm2_5 = Column(Float)
    pm10 = Column(Float)

    pm2_5_avg_6h = Column(Float)
    pm2_5_max_6h = Column(Float)
    pm2_5_skew_6h = Column(Float)
    pm2_5_std_6h = Column(Float)

    # AQI
    aqi_pm2_5 = Column(Float)
    aqi_pm10 = Column(Float)

    level = Column(Integer) # 1 = good, 2 = moderate, 3 = unhealth for sensitive groups, and so on

    aqi_pm2_5_max_24h = Column(Float)
    aqi_pm2_5_skew_24h = Column(Float)
    aqi_pm2_5_std_24h = Column(Float)

    # climate readings
    temperature = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)

    @property
    def get_station_readings_count(self):
        pass

class RegionReadings(BasePostgres):
    '''
    average PM and AQI values for a specific region. Used for inference
    '''

    __tablename__ = 'region_readings'
    id = Column(Integer, primary_key=True)
    region = Column(Integer, ForeignKey('regions.id'), name = 'region')
    region_rel = relationship('Regions')
    date = Column(DateTime)
    pm2_5_region_avg = Column(Float)
    pm2_5_region_max = Column(Float)
    pm2_5_region_skew = Column(Float)
    pm2_5_region_std = Column(Float)
    
    aqi_region_avg = Column(Float)
    aqi_region_max = Column(Float)
    aqi_region_skew = Column(Float)
    aqi_region_std = Column(Float)

    level_region_max = Column(Integer)


class CalibrationFactors(BasePostgres):
    '''
    Signal treatment factor for each station, calculated by comparing each pollution station 3 month average 
    readings with average readings from a nearby pattern station. Factors should be calculated monthly.
    '''
    __tablename__ = 'calibration_factors'
    id = Column(Integer, primary_key=True)
    region = Column(String, ForeignKey('regions.region_code'), name = 'region')
    station_id = Column(Integer, ForeignKey('stations.id'))

    # data used to calculate calibration patterns
    date_start_cal = Column(DateTime) # start of calculation period
    date_end_cal = Column(DateTime) # should always be 90 days larger than date_start_cal
    station_mean = Column(Float) # mean of the station
    pattern_mean = Column(Float) # mean of our pattern station 

    # period of calibration data usage
    date_start = Column(DateTime)
    date_end = Column(DateTime)
    
    calibration_factor = Column(Float)
