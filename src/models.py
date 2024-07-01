from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, VARCHAR, Boolean
from datetime import datetime

BasePostgres = declarative_base()

class Regions(BasePostgres):
    __tablename__ = 'regions'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    region_code = Column(String, unique=True)


class Stations(BasePostgres): # Same as Django
    __tablename__ = 'stations'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    region = Column(String, ForeignKey('regions.region_code'), name = 'region') 
    region_rel = relationship('Regions')
    is_station_on = Column(Boolean)
    # agregar is_station_on (variable de status)
    
    # About region: in Django it looks like this 
    # region = models.CharField(max_length = 100, 
    #                           choices = Region.choices, 
    #                           default = Region.ASUNCION)
    # where there's a class Region with TextChoices


class StationReadings(BasePostgres): # Same as Django
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

class RegionReadings(BasePostgres):

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
    
    level_region_max = Column(Float)


class StationsReadingsRaw(BasePostgres): # Copy of Raw Data from Origin DB 
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

class WeatherData(BasePostgres):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    temperature = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)
    wind_speed = Column(Float)
    wind_dir_cos = Column(Float)
    wind_dir_sin = Column(Float)

class USAirQualityReadings(BasePostgres):
    __tablename__ = 'airnow_data'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    pm2_5 = Column(Float)
    latitude = Column(Float)
    longitude = Column(Float)

