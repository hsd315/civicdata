
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, Boolean
from sqlalchemy import Float as Real,  Text, ForeignKey
from sqlalchemy.orm import relationship, backref, deferred
from sqlalchemy.types import TypeDecorator, TEXT, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import Mutable

Base = declarative_base()


class Incidents(Base):
    __tablename__ = 'Incidents'

    incidents_id = SAColumn('incidents_id',sqlalchemy.types.Integer,primary_key=True)
    type = SAColumn('type',sqlalchemy.types.Text)
    agency = SAColumn('agency',sqlalchemy.types.Text)
    datetime = SAColumn('datetime',sqlalchemy.types.DateTime)
    legend = SAColumn('legend',sqlalchemy.types.Text)
    description = SAColumn('description',sqlalchemy.types.Text)
    addresses_id = SAColumn('addresses_id',sqlalchemy.types.Text)
    city = SAColumn('city',sqlalchemy.types.Text)
    neighborhood = SAColumn('neighborhood',sqlalchemy.types.Text)
    place = SAColumn('place',sqlalchemy.types.Text)
    x = SAColumn('x',sqlalchemy.types.Integer)
    y = SAColumn('y',sqlalchemy.types.Integer)
    lon = SAColumn('lon',sqlalchemy.types.Float)
    lat = SAColumn('lat',sqlalchemy.types.Float)
    cellx = SAColumn('cellx',sqlalchemy.types.Integer)
    celly = SAColumn('celly',sqlalchemy.types.Integer)

    def __init__(self,**kwargs):
        self.incidents_id = kwargs.get("incidents_id",None)
        self.type = kwargs.get("type",None)
        self.agency = kwargs.get("agency",None)
        self.datetime = kwargs.get("datetime",None)
        self.legend = kwargs.get("legend",None)
        self.description = kwargs.get("description",None)
        self.addresses_id = kwargs.get("addresses_id",None)
        self.city = kwargs.get("city",None)
        self.neighborhood = kwargs.get("neighborhood",None)
        self.place = kwargs.get("place",None)
        self.x = kwargs.get("x",None)
        self.y = kwargs.get("y",None)
        self.lon = kwargs.get("lon",None)
        self.lat = kwargs.get("lat",None)
        self.cellx = kwargs.get("cellx",None)
        self.celly = kwargs.get("celly",None)


