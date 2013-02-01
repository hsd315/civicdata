'''

'''


from  databundles.bundle import BuildBundle
from pyparsing import *


class Bundle(BuildBundle):
    ''' '''
 
    header = ['Date', 'Type', 'Address', 'Latitude', 'Longitude', 'Link', 'Description', 'time','state']
    types  = [str,      str,    str,     float,      float,        str,    str,           str, str]

    def __init__(self,directory=None):
        import yaml
        
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

        states_file =  self.filesystem.path(self.config.build.statesFile)
    
        with open(states_file, 'r') as f:
            self.states =  yaml.load(f) 

    ### Prepare is run before building, part of the devel process.  

    def prepare(self):
        from databundles.orm import Column
        from databundles.partition import PartitionIdentity

        if not self.database.exists():
            self.database.create()    
   
        #
        # Create schema
        #
   
        type_map = {
            str:Column.DATATYPE_TEXT,
            float: Column.DATATYPE_REAL
             }
            
        cols = dict(zip(self.header,  self.types))
       
        t = self.schema.add_table('incidents')
        #self.schema.add_column(t,'id',datatype = Column.DATATYPE_INTEGER,is_primary_key=True )
         
        for field in self.header:
            # Dont iterate dict; columns must be in sam order as header
            self.schema.add_column(t,field,datatype = type_map[ cols[field]])
           
        self.database.session.commit()
        self.schema.create_tables()
     
    
        return True
        
    def generate_urls(self):
        
        template = self.config.build.urlTemplate
        
        for year in range(2007,2010):
            for month in range(1,12):
                url = template.format(year=year,month=month)
                yield url
        
    def import_csv(self, url):
        ''''''
        import time, dateutil.parser
        import petl.fluent as petlf
      
        zip_file = self.filesystem.download(url, 'zip')
        csv_file = self.filesystem.unzip(zip_file)


        def parse_time(v):
            if len(v) < 11:
                return None
            
            try:
                return dateutil.parser.parse(v).time().isoformat()
            except:
                return None
        
        print csv_file
        t = ( petlf.fromcsv(csv_file)
                .addfield('time', lambda rec: parse_time(rec['Date']))
                .convert('Date', lambda v: dateutil.parser.parse(v).date().isoformat())
                .addfield('state',lambda rec: rec['Address'].split(',')[-1].strip())
                .convert(dict(zip(self.header, self.types)))
                .convert( ('Type', 'Address', 'Description'), lambda v: v.strip().decode('latin1').encode('ascii','xmlcharrefreplace') )
                .setheader(map(str.lower,self.header))
                )

        return t
        
    def build(self):
        import petl
        from databundles.partition import PartitionIdentity
        
        keys = set()
        for url in self.generate_urls():
            t = self.import_csv(url)
            self.log("Loading {}".format(url))
            t.appendsqlite3(self.database.path, 'incidents')
    
        return True
  
    def x_submit(self):
        
        import petl.fluent as petlf
        import tempfile
            
        bb = self.sd_bb
        
        q = (()
            .format(bb[0][0],bb[1][0],bb[0][1],bb[1][1])
            )
   
        print q
        t = (petlf
            .fromsqlite3(self.database.path, q)
            .tocsv('/tmp/sd-crime.csv')
            )

        return True
    
    ### Submit the package to the repository
    def submit(self):
        import os
        import databundles.client.ckan
        import time, datetime

        for config in self.generate_extracts():
            print config
            
        return True

        ck = databundles.client.ckan.get_client()
    
        ckb = ck.update_or_new_bundle_extract(self)
        
        # Clear out existing resources. 
        ckb['resources'] = []      
        ck.put_package(ckb)
        
        for config, partition in self.generate_extracts():
            self.do_extract(ckb, config, partition)
        
        return True
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    