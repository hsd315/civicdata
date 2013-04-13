
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, Boolean
from sqlalchemy import Float as Real,  Text, ForeignKey
from sqlalchemy.orm import relationship, deferred
from sqlalchemy.types import TypeDecorator, TEXT, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import Mutable

Base = declarative_base()

class Incidents(Base):

    incidents_id = SAColumn('incidents_id',sqlalchemy.types.Integer,primary_key=True)
    type = SAColumn('type',sqlalchemy.types.Text)
    agency = SAColumn('agency',sqlalchemy.types.Text)
    datetime = SAColumn('datetime',sqlalchemy.types.DateTime)
    legend = SAColumn('legend',sqlalchemy.types.Text)
    description = SAColumn('description',sqlalchemy.types.Text)
    block_address = SAColumn('block_address',sqlalchemy.types.Text,ForeignKey('address.address_id')))
    syn_address = SAColumn('syn_address',sqlalchemy.types.Text,ForeignKey('address.address_id')))

    def __init__(self,**kwargs):
        self.incidents_id = kwargs.get("incidents_id",None)
        self.type = kwargs.get("type",None)
        self.agency = kwargs.get("agency",None)
        self.datetime = kwargs.get("datetime",None)
        self.legend = kwargs.get("legend",None)
        self.description = kwargs.get("description",None)
        self.block_address = kwargs.get("block_address",None)
        self.syn_address = kwargs.get("syn_address",None)


class Address(Base):

    address_id = SAColumn('address_id',sqlalchemy.types.Integer,primary_key=True)
    type = SAColumn('type',sqlalchemy.types.Text)
    number = SAColumn('number',sqlalchemy.types.Text)
    street = SAColumn('street',sqlalchemy.types.Text)
    city = SAColumn('city',sqlalchemy.types.Text)
    state = SAColumn('state',sqlalchemy.types.Text)
    zip = SAColumn('zip',sqlalchemy.types.Text)
    lat = SAColumn('lat',sqlalchemy.types.Integer)
    lon = SAColumn('lon',sqlalchemy.types.Integer)

    def __init__(self,**kwargs):
        self.address_id = kwargs.get("address_id",None)
        self.type = kwargs.get("type",None)
        self.number = kwargs.get("number",None)
        self.street = kwargs.get("street",None)
        self.city = kwargs.get("city",None)
        self.state = kwargs.get("state",None)
        self.zip = kwargs.get("zip",None)
        self.lat = kwargs.get("lat",None)
        self.lon = kwargs.get("lon",None)


