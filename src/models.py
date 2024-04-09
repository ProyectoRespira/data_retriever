from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, VARCHAR

BasePostgres = declarative_base()
BaseMySQL = declarative_base()

# class TableTracking(Base):
#     __tablename__ = 'table_tracking'

#     id = Column(Integer, primary_key=True)
#     table_name = Column(String, unique=True)
#     last_mirrored_id = Column(Integer)


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
    station = Column(Integer, ForeignKey('stations.id'))
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
    fecha = Column(String)
    hora = Column(String)
    mp2_5 = Column(Float)
    mp1 = Column(Float)
    mp10 = Column(Float)
    temperatura = Column(Float)
    humedad = Column(Float)
    presion = Column(Float)
    bateria = Column(Float)

class EstacionX(BaseMySQL): # Origin DB
    __abstract__ = True

    id = Column(Integer, primary_key=True)
    fecha = Column(VARCHAR)
    hora = Column(VARCHAR)
    mp1 = Column(VARCHAR)
    mp2_5 = Column(VARCHAR)
    mp10 = Column(VARCHAR)
    temperatura = Column(VARCHAR)
    humedad = Column(VARCHAR)
    presion = Column(VARCHAR)
    bateria = Column(VARCHAR)



