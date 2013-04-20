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
        self.load_addresses()

    def load_incidents(self):
        from orm import Incidents, Addresses
        import dateutil.parser
        from databundles.geo.address import Parser
        
        log_rate = self.init_log_rate(5000)
        w_log_rate = self.init_log_rate(5000)
        br_log_rate = self.init_log_rate(100)
        e_log_rate = self.init_log_rate(1000)
         
        p = Parser()
        
        s = self.database.session
        
        _,incidents = self.library.dep('incidents')
    
        o_adr = {} # original addresses, indexed by hash
        g_adr = {} #geocoded addreses, indexed by parent id
        self.log("Caching addresses")
        for row in self.database.query("SELECT * FROM addresses"):
            
            if row['parent']:
                g_adr[row['parent']] = row
            else:
                o_adr[row['hash']] = row
    
        self.log("Cached {} orig and {} geocoded addresses".format(len(o_adr), len(g_adr)))
        self.log("Loading incidents")
 
        log_rate = self.init_log_rate()
        ins = self.database.inserter('incidents')
        for i, inct in enumerate(incidents.query("select * from incidents")):
            row = dict(inct)
            
            log_rate('Load: ')

            a_row = self.prep_row_address(p,row)

            if not a_row:
                #self.error("Bad row")
                br_log_rate("Bad row: ")
                continue
          
            try:
                orig = o_adr[a_row['hash']]
            except:
                #self.error("Failed to get orig records for hash: "+a_row['hash'])
                e_log_rate("Link Error: ")
                continue
            
            try:
                geo = g_adr[orig['addresses_id']]
            except:
                #self.error("Failed to get geo record for id: "+str(orig['addresses_id']))
                e_log_rate("Link Error: ")
                continue                
        
            row['block_address_id'] = orig['addresses_id']
            row['syn_address_id'] = geo['addresses_id']
            
            w_log_rate("Write Incident: ")
        
            ins.insert(row)
        
        ins.close()
        
        return True

    def prep_row_address(self,p,row):
        import dateutil.parser
        
        try:
            ps = p.parse(row['blockaddress'])
        except Exception as e:
            ps = False
            
        if not ps:
            #self.error(row['blockaddress'])
            return None
        
        row['datetime'] = dateutil.parser.parse(row['datetime'])
        row['state'] = 'CA'  
        row['type'] = 'block' if ps.is_block else 'address'
        row['number'] = ps.number
        row['street'] = ps.street
        row['street2'] = ps.cross_street.street if ps.cross_street else None
        row['city'] = row['city'].title()        
        row['hash'] = self.hash_address(row)
        
        return row

    def load_addresses(self):
        from orm import Addresses
        import dateutil.parser
        from databundles.geo.address import Parser
        
        p = Parser()
        
        log_rate = self.init_log_rate(5000)
        log_rate_saved = self.init_log_rate(5000)
        addresses = set()
        
        s = self.database.session
        
        _,incidents = self.library.dep('incidents')
        
        p = Parser()
        
        for i, inct in enumerate(incidents.query("select * from incidents")):
            row = dict(inct)
            log_rate('Address: ')   
            
            if not row['blockaddress'].strip(): continue

            row = self.prep_row_address(p,row)

            if row['hash'] not in addresses:
                log_rate_saved('Stored: ')
                s.add(Addresses(**row))
                addresses.add(row['hash'])
                
            if i % 50000 == 0:
                s.commit()
            
        s.commit()



                

    def test_geocode(self):
        import requests
        from orm import Addresses
        
        from geopy import geocoders  
        
        id = 'iTYPICzV34Ho5KVzUPapVjzNhDviyGyj3X0b2Fc3jTwF85ISRlhsXb_23vZfwSKjF84-'

        gc = geocoders.OpenMapQuest()
      
        for row in self.database.query("SELECT * from addresses where street2 is NULL and number is not NULL"):
            s = "{number} {street}, {city}, {state} {zip}".format(**dict(row))

            try:
                place, (lat, lng) = gc.geocode(s)
                
                print "%s: %.5f, %.5f" % (place, lat, lng)
            except Exception as e:
                self.error(s+": "+str(e))
                
  
    def _make_smarty_list(self):
        
        
        # Get all of the addresses records that have not already been geocoded. 
        q = """
        SELECT a1.* FROM addresses as a1
        LEFT JOIN addresses AS a2 ON a2.parent = a1.addresses_id
        WHERE a1.street2 IS NULL AND a1.number IS NOT NULL AND a1.parent IS NULL
        AND a2.addresses_id IS NULL;
        """

        import tempfile
        import csv

        f = tempfile.TemporaryFile()
      
        cw = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        cw.writerow(['ID','street1','city','state','zipcode'])
        add = 1
        for i, row in enumerate(self.database.query(q)):    
            row = dict(row)        

            number = int(row['number'])
            number = number + add
            cw.writerow([
                         str(row['addresses_id']),
                         str(number)+" "+row['street'],
                         row['city'],
                         row['state'],
                         row['zip']
                         ]
                        )
                
        f.seek(0)
                
        return f   
                
    def _unzip_smarty_file(self, url):
        import re

        self.log("Downloading {}".format(url))

        df = self.filesystem.download(url)
        
        self.log("Downloaded to: {}".format(df))

        ef = self.filesystem.unzip(df,re.compile('everything.csv'))
        
        self.log("Unzipped to: {}".format(ef))
        
        return ef
        
    def _process_smarty_file(self,file_name=None):
        import csv
        from databundles.geo.address import Parser
        
        if file_name is None:
            file_name = '/Volumes/DataLibrary/cache/extracts/_lists_3b4a6f29-2853-4459-8ca9-b48a31dd6da6_download/everything.csv'
                 
        self.log("Processing {}".format(file_name))
        log_rate = self.init_log_rate(1000)
        p = Parser()   
        ins = self.database.inserter('addresses', cache_size = 100)
        with open(file_name) as f:
            reader = csv.DictReader(f)
            
            for row in reader:

                if 'F#' in row['Footnotes']:
                    continue
                
                if not int(float(row['Latitude'])):
                    continue
                
                log_rate("Adding addresses: ")
                
                id_ = row['[ID]']
                ps = p.parse(row['DeliveryLine1'])

                a = {'parent' : id_,
                       'type' : 'block',
                       'number' : ps.number, 
                       'street' : ps.street, 
                       'city' : row['City'],
                       'state' : row['State'],
                       'zip' : row['ZIPCode'],
                       'lat' : row['Latitude'],
                       'lon' : row['Longitude'],
                       'geocoder' : 'S'
                       }
            
                a['hash'] = self.hash_address(a)
            
                ins.insert(a)
                
        ins.close()
         
    def smarty_batch_geocode(self):
        import requests
        from orm import Addresses
        from geopy import geocoders
        import csv  
        import time
    
        id = self.config.accounts.smarty.id
        token = self.config.accounts.smarty.token

        t = time.time()

        url = ('https://api.smartystreets.com/lists?filename={filename}&auth-id={id}&auth-token={token}'
               .format(filename='smarty-{}.csv'.format(t),id=id,token=token))
   
        
        self.log("Make SmartyStreets list")
        f = self._make_smarty_list()

        self.log("Send list to  SmartyStreets")
        r = requests.post(url, data=f)
        
        f.close() # Deletes the file
      
            
        j = r.json()
        list_id = j['list_id']
        
        poll = 10 # ONly wait 10s the first time we poll
        poll2 =  j['polling_frequency_in_seconds']
        # Poll to get the completed file.       
        url = ('https://api.smartystreets.com/lists/{list_id}?auth-id={id}&auth-token={token}'
               .format(list_id=list_id,id=id,token=token))

        self.log("Poll for completion")
        while True:
            time.sleep(poll)
            r = requests.get(url)
            j = r.json()
            
            if j['current_step'] == 'Succeeded':
                break
            else:
                self.log('Waiting: '+j['current_step'])
                
            poll = poll2
       
        url = ('https://api.smartystreets.com/lists/{list_id}/download?auth-id={id}&auth-token={token}'
              .format(list_id=list_id,id=id,token=token)) 
        

        f = self._unzip_smarty_file(url)
        
        self._process_smarty_file(f)
        

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    