'''
Minor conversion  of the SpotCrime free crime database that re-codes text
and loads the records into a database. 
'''

from  databundles.bundle import BuildBundle

class Bundle(BuildBundle):
    ''' '''
 

    def __init__(self,directory=None):
        import yaml
        
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

        states_file =  self.filesystem.path(self.config.build.statesFile)
    
        with open(states_file, 'r') as f:
            self.states =  yaml.load(f) 

    ### Prepare is run before building, part of the devel process.  

    def list_states(self):
        bundle = self.library.dep('geodim')
        
        for row in bundle.query("SELECT * from states"):
            print row
        

    def prepare(self):
        from databundles.orm import Column
        from databundles.partition import PartitionIdentity

        if not self.database.exists():
            self.database.create()    
   
        #
        # Create schema
        #
   
        with open(self.filesystem.path(self.config.build.schemaFile), 'rbU') as f:
               self.schema.schema_from_file(f)     
   
        self.database.session.commit()
        self.schema.create_tables()

        return True
        
    @property
    def header(self):
        header = []
        types = []
        for c in self.schema.table('incidents').columns:
            header.append(str(c.name))
            types.append(c.python_type)
            
        return header, types
        
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
        
        header, types = self.header 

        t = ( petlf.fromcsv(csv_file)
                .addfield('Time', lambda rec: parse_time(rec['Date']))
                .convert('Date', lambda v: dateutil.parser.parse(v).date().isoformat())
                .addfield('State',lambda rec: rec['Address'].split(',')[-1].strip())
                .convert( ('Type', 'Address', 'Description'), lambda v: v.strip().decode('latin1').encode('ascii','xmlcharrefreplace') )
                .setheader(map(str.lower,header))
                .convert(dict(zip(header, types)))
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
    

    def test_addresses(self):
        from random import randint
        import re

        from databundles.geo.address import Address
        
        ap = Address()

        
        for row in self.database.query('select * from incidents'):
            r = randint(0,99)
            
            a1 =  re.sub('XX',"{:02d}".format(r), row['address'])

            print "{0:40s} --------".format(a1)
            print '123456789_' *8
            
            try:
                a2 = ap.parse(a1)
                print a2.dump()
            except:
                print "Failed: ", a1
            
    

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    