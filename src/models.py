from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, VARCHAR

BasePostgres = declarative_base()


class Stations(BasePostgres): # Same as Django
    __tablename__ = 'stations'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    region = Column(String) 
    
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
    pm1 = Column(Float)
    pm2_5 = Column(Float)
    pm10 = Column(Float)
    temperature = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)

    aqi_pm2_5 = Column(Float)
    aqi_pm10 = Column(Float)


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