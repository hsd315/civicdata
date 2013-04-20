
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


class Addresses(Base):
    __tablename__ = 'Addresses'

    addresses_id = SAColumn('addresses_id',sqlalchemy.types.Integer,primary_key=True)
    hash = SAColumn('hash',sqlalchemy.types.Text)
    parent = SAColumn('parent',sqlalchemy.types.Integer)
    type = SAColumn('type',sqlalchemy.types.Text)
    number = SAColumn('number',sqlalchemy.types.Text)
    street = SAColumn('street',sqlalchemy.types.Text)
    street2 = SAColumn('street2',sqlalchemy.types.Text)
    city = SAColumn('city',sqlalchemy.types.Text)
    state = SAColumn('state',sqlalchemy.types.Text)
    zip = SAColumn('zip',sqlalchemy.types.Integer)
    lat = SAColumn('lat',sqlalchemy.types.Integer)
    lon = SAColumn('lon',sqlalchemy.types.Integer)
    geocoder = SAColumn('geocoder',sqlalchemy.types.Text)
    gc_place = SAColumn('gc_place',sqlalchemy.types.Text)

    def __init__(self,**kwargs):
        self.addresses_id = kwargs.get("addresses_id",None)
        self.hash = kwargs.get("hash",None)
        self.parent = kwargs.get("parent",None)
        self.type = kwargs.get("type",None)
        self.number = kwargs.get("number",None)
        self.street = kwargs.get("street",None)
        self.street2 = kwargs.get("street2",None)
        self.city = kwargs.get("city",None)
        self.state = kwargs.get("state",None)
        self.zip = kwargs.get("zip",None)
        self.lat = kwargs.get("lat",None)
        self.lon = kwargs.get("lon",None)
        self.geocoder = kwargs.get("geocoder",None)
        self.gc_place = kwargs.get("gc_place",None)



class Incidents(Base):
    __tablename__ = 'Incidents'

    incidents_id = SAColumn('incidents_id',sqlalchemy.types.Integer,primary_key=True)
    type = SAColumn('type',sqlalchemy.types.Text)
    agency = SAColumn('agency',sqlalchemy.types.Text)
    datetime = SAColumn('datetime',sqlalchemy.types.DateTime)
    legend = SAColumn('legend',sqlalchemy.types.Text)
    description = SAColumn('description',sqlalchemy.types.Text)
    block_address_id = SAColumn('block_address_id',sqlalchemy.types.Integer,ForeignKey('Addresses.addresses_id'))
    syn_address_id = SAColumn('syn_address_id',sqlalchemy.types.Integer,ForeignKey('Addresses.addresses_id'))

    block_address=relationship("Addresses",
       foreign_keys=[block_address_id],
       backref=backref('incidents_block_address', 
                       order_by='Addresses.addresses_id'))

    syn_address=relationship("Addresses",
       foreign_keys=[syn_address_id],
       backref=backref('incidents_syn_address', 
                       order_by='Addresses.addresses_id'))

    def __init__(self,**kwargs):
        self.incidents_id = kwargs.get("incidents_id",None)
        self.type = kwargs.get("type",None)
        self.agency = kwargs.get("agency",None)
        self.datetime = kwargs.get("datetime",None)
        self.legend = kwargs.get("legend",None)
        self.description = kwargs.get("description",None)
        self.block_address_id = kwargs.get("block_address_id",None)
        self.syn_address_id = kwargs.get("syn_address_id",None)


