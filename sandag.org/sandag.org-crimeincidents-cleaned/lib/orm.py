
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
    segment_id = SAColumn('segment_id',sqlalchemy.types.Integer)
    segment_source_id = SAColumn('segment_source_id',sqlalchemy.types.Integer)
    road_source_id = SAColumn('road_source_id',sqlalchemy.types.Integer)
    addresses_id = SAColumn('addresses_id',sqlalchemy.types.Text)
    address = SAColumn('address',sqlalchemy.types.Text)
    city = SAColumn('city',sqlalchemy.types.Text)
    neighborhood = SAColumn('neighborhood',sqlalchemy.types.Text)
    place = SAColumn('place',sqlalchemy.types.Text)
    x = SAColumn('x',sqlalchemy.types.Integer)
    y = SAColumn('y',sqlalchemy.types.Integer)
    lon = SAColumn('lon',sqlalchemy.types.Float)
    lat = SAColumn('lat',sqlalchemy.types.Float)
    gcquality = SAColumn('gcquality',sqlalchemy.types.Integer)

    def __init__(self,**kwargs):
        self.incidents_id = kwargs.get("incidents_id",None)
        self.type = kwargs.get("type",None)
        self.agency = kwargs.get("agency",None)
        self.datetime = kwargs.get("datetime",None)
        self.legend = kwargs.get("legend",None)
        self.description = kwargs.get("description",None)
        self.segment_id = kwargs.get("segment_id",None)
        self.segment_source_id = kwargs.get("segment_source_id",None)
        self.road_source_id = kwargs.get("road_source_id",None)
        self.addresses_id = kwargs.get("addresses_id",None)
        self.address = kwargs.get("address",None)
        self.city = kwargs.get("city",None)
        self.neighborhood = kwargs.get("neighborhood",None)
        self.place = kwargs.get("place",None)
        self.x = kwargs.get("x",None)
        self.y = kwargs.get("y",None)
        self.lon = kwargs.get("lon",None)
        self.lat = kwargs.get("lat",None)
        self.gcquality = kwargs.get("gcquality",None)



class Segincident(Base):
    __tablename__ = 'Segincident'

    segincident_id = SAColumn('segincident_id',sqlalchemy.types.Integer,primary_key=True)
    count = SAColumn('count',sqlalchemy.types.Integer)
    type = SAColumn('type',sqlalchemy.types.Text)
    agency = SAColumn('agency',sqlalchemy.types.Text)
    datetime = SAColumn('datetime',sqlalchemy.types.DateTime)
    legend = SAColumn('legend',sqlalchemy.types.Text)
    description = SAColumn('description',sqlalchemy.types.Text)
    segment_id = SAColumn('segment_id',sqlalchemy.types.Integer)
    segment_source_id = SAColumn('segment_source_id',sqlalchemy.types.Integer)
    road_source_id = SAColumn('road_source_id',sqlalchemy.types.Integer)
    addresses_id = SAColumn('addresses_id',sqlalchemy.types.Integer)
    address = SAColumn('address',sqlalchemy.types.Text)
    city = SAColumn('city',sqlalchemy.types.Text)
    neighborhood = SAColumn('neighborhood',sqlalchemy.types.Text)
    place = SAColumn('place',sqlalchemy.types.Text)
    gcquality = SAColumn('gcquality',sqlalchemy.types.Integer)
    density = SAColumn('density',sqlalchemy.types.Float)
    geometry = SAColumn('geometry',sqlalchemy.types.LargeBinary)

    def __init__(self,**kwargs):
        self.segincident_id = kwargs.get("segincident_id",None)
        self.count = kwargs.get("count",None)
        self.type = kwargs.get("type",None)
        self.agency = kwargs.get("agency",None)
        self.datetime = kwargs.get("datetime",None)
        self.legend = kwargs.get("legend",None)
        self.description = kwargs.get("description",None)
        self.segment_id = kwargs.get("segment_id",None)
        self.segment_source_id = kwargs.get("segment_source_id",None)
        self.road_source_id = kwargs.get("road_source_id",None)
        self.addresses_id = kwargs.get("addresses_id",None)
        self.address = kwargs.get("address",None)
        self.city = kwargs.get("city",None)
        self.neighborhood = kwargs.get("neighborhood",None)
        self.place = kwargs.get("place",None)
        self.gcquality = kwargs.get("gcquality",None)
        self.density = kwargs.get("density",None)
        self.geometry = kwargs.get("geometry",None)


