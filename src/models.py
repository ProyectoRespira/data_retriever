from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float

Base = declarative_base()

class TableTracking(Base):
    __tablename__ = 'table_tracking'

    id = Column(Integer, primary_key=True)
    table_name = Column(String, unique=True)
    last_mirrored_id = Column(Integer)