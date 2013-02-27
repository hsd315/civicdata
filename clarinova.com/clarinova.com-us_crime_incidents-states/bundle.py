'''

'''


from  databundles.bundle import BuildBundle
from pyparsing import *


class Bundle(BuildBundle):
    ''' '''
 
    header = ['Date', 'Type', 'Address', 'Latitude', 'Longitude', 'Link', 'Description', 'state', 'city']
    types  = [str,      str,    str,     float,      float,        str,    str,           str,     str]
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    ### Prepare is run before building, part of the devel process.  

    def prepare(self):
        from databundles.orm import Column
        from databundles.partition import PartitionIdentity
        import yaml
        
        if not self.database.exists():
            self.database.create()    
   
        #
        # Create schema
        #
   
        type_map = {
            str:Column.DATATYPE_TEXT,
            float: Column.DATATYPE_REAL }
            
   
        cols = dict(zip(self.header,  self.types))
       
        t = self.schema.add_table('incidents')
        
        for name,type in cols.items():
            self.schema.add_column(t,name,datatype = type_map[type])
           
        self.database.session.commit()
        self.schema.create_tables()
     
        #
        # Create Partitions
        #
     
        states_file =  self.filesystem.path(self.config.build.statesFile)
    
        with open(states_file, 'r') as f:
            states =  yaml.load(f) 
        
        for stab,state in states.items():
            pid = PartitionIdentity(self.identity, table='incidents', space=stab)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                partition = self.partitions.new_partition(pid)
                partition.create_with_tables('incidents')    
                self.log("Created partition: {}".format(partition.identity.name))      
        
        self.database.session.commit()
    
        return True
        
    def generate_urls(self):
        
        template = self.config.build.urlTemplate
        
        for year in range(2007,2010):
            for month in range(1,12):
                url = template.format(year=year,month=month)
                yield url, month, year
        
    def import_csv(self, url):
        ''''''
        import petl.fluent as petlf
      
        zip_file = self.filesystem.download(url, 'zip')
        csv_file = self.filesystem.unzip(zip_file)

        t = ( petlf.fromcsv(csv_file)
                 .addfield('state',lambda rec: rec['Address'].split(',')[-1].strip())
                 .addfield('city',lambda rec: rec['Address'].split(',')[-2].strip().lower().capitalize())
                 .convert(dict(zip(self.header, self.types)))
                 .setheader(map(str.lower,self.header))
                )
        
        return t
        
    def build(self):
        
        street_address = self.street_address()
        
        for url, month,year in self.generate_urls():
            t = self.import_csv(url)

            for row in t.records():
                print row
               
                   
        return True
    
        
  
 
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    