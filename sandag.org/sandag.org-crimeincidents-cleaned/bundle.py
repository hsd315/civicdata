'''

'''
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy import event
from sqlalchemy import Column as SAColumn, Integer, Boolean
from sqlalchemy import Float as Real,  Text, ForeignKey
from sqlalchemy.orm import relationship, deferred
from sqlalchemy.types import TypeDecorator, TEXT, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import Mutable

from sqlalchemy.sql import text
from databundles.identity import  DatasetNumber, ColumnNumber
from databundles.identity import TableNumber, PartitionNumber, ObjectNumber

import json

Base = declarative_base()

from  databundles.bundle import BuildBundle
 
class SavableMixin(object):
    
    def save(self):
        self.session.commit()


class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    def prepare(self):
        '''Create the datbase and load the schema from a file, if the file exists. '''
        from databundles.partition import PartitionIdentity
      
        if not self.database.exists():
            self.database.create()

        if self.config.build.get('schema_file', False):
            with open(self.filesystem.path(self.config.build.schema_file), 'rbU') as f:
                self.schema.schema_from_file(f)      
                self.schema.create_tables()     

        self.schema.write_orm()

        return True

    def hash_address(self, row):
        import hashlib 
        
        m = hashlib.md5()
        m.update(str(row['number']) if row.get('number',False) else '' )
        m.update(row['street'].lower()  if row.get('street',False) else '' )
        m.update(row['street2'].lower()  if row.get('street2',False) else '' )
        m.update(row['city'].lower()  if row['city'] else '' )
        m.update(row['state'].lower()  if row.get('state',False) else '' ) 
        m.update(str(row['zip'])  if row['zip'] else '' ) 
        m.update(str(row['zip'])  if row['zip'] else '' ) 
        return m.hexdigest()
 

    def build(self):

        import dateutil.parser
        from databundles.geo.geocoder import Geocoder
        
        g = Geocoder(self.library, addresses_ds='geoaddresses')
                
        log_rate = self.init_log_rate(5000)
     
        _,incidents = self.library.dep('incidents')
    
        self.database.query("DELETE FROM incidents")
 
        with self.database.inserter('incidents') as ins:
            for i, inct in enumerate(incidents.query("SELECT * FROM incidents")):
                row = dict(inct)
                
                log_rate('Load: ')
    
                row = self.prep_row_address(g,row)
        
                if not row: continue
            
                ins.insert(row)
                

    def prep_row_address(self,geocoder,row):
        import dateutil.parser
        from random import choice
        
        candidates = geocoder.geocode_semiblock(row['blockaddress'], row['city'], 'CA')
            
        if  len(candidates) != 1:
            #self.error(row['blockaddress'])
            return None

        s =  candidates.popitem()[1]
        
        address = choice(s)
        address['datetime'] = dateutil.parser.parse(row['datetime'])

        for k,v in address.items():
            row[k] = v

        return row



import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    