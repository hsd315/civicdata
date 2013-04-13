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

        print self.schema.as_orm()
        self.schema.write_orm()

        return True
    
    ### Build the final package
     
    def build(self):
        import tokenize as tk
        from StringIO import StringIO
        _,incidents = self.library.dep('incidents')
        
        i = 0
        for inct in incidents.query("select * from incidents"):
            i = i+1
            if i > 5:
                break
            
            number = None
            street = []
            
            for  i, (type, val, start, end, _)  in enumerate(tk.generate_tokens(StringIO(inct['blockaddress']).readline)):
                if i == 0:
                    if type != tk.NUMBER:
                        raise ValueError("Should have gotten a number first in : {} ".format(inct['blockaddress']))
                    number = int(val)
                elif val.lower() != 'block':
                    street.append(val.capitalize())

            row = dict(inct)
            
            row['housenumber'] = number
            row['street']  =' '.join(street)
            
            print  "{housenumber} {street}, {city}, CA {zip}".format(**row)
        
        return True


    def test_orm(self):
        from orm import Incidents, Addresses
        
        s = self.database.session
        
        for i in range(5):
            inct = Incident(type=str(i))
            s.add(inct)
            
        s.commit()
        

    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    