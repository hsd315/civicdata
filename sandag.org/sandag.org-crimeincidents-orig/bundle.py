'''

'''

from  databundles.bundle import BuildBundle
import csv
import datetime

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        self.part_cache = {}

    def prepare(self):
        '''Create the datbase and load the schema from a file, if the file exists. '''
        from databundles.partition import PartitionIdentity
      
        if not self.database.exists():
            self.database.create()

        if self.config.build.get('schema_file', False):
            with open(self.filesystem.path(self.config.build.schema_file), 'rbU') as f:
                self.schema.schema_from_file(f)      
                self.schema.create_tables()     

        return True

               

    def generate_incidents(self, table):
        from databundles.client.ckan import Ckan
      
        repo = Ckan(self.config.build.repo.url, self.config.build.repo.key)
   
        pkg = repo.get_package(self.config.build.repo.package)
        
        for resource in pkg['resources']:    
                  
            f = self.filesystem.download(resource['url'])
            uz = self.filesystem.unzip(f)
                   
            self.log("Reading: {}".format(uz))

            with open(uz, 'rbU') as csvfile:
                reader = csv.reader(csvfile)
                header = reader.next()
                
                fh = [ c.data['fileheader'] for c in table.columns]
                if  fh != header:
                    raise Exception("Header mismatch: {} != {} ".format(fh, header))
                
                for row in reader:
                    yield  list(row)

    def get_partition(self,year):
        
        if year not in self.part_cache:
            p = self.partitions.find_or_new(time=year, table='incidents');
            self.part_cache[year] = p.database.inserter('incidents')
            
        return self.part_cache[year]

    def build(self):
        from dateutil.parser import parse
        # All incidents
        allp = self.partitions.find_or_new(table='incidents');
        allins = allp.database.inserter()
        
        table = allp.table
        
        lr = self.init_log_rate(10000)
        
        for row in self.generate_incidents(table):
            
            
            lr()

            dt = parse(row[2])
            row[2] = dt
            row[5] = unicode(row[5],errors='ignore').strip()
            
            ins = self.get_partition(dt.year)

            drow = [ v if v else None for v in row ]

            if not drow[6]:
                drow[6] = -1 # Zips

            try:
                ins.insert(drow)
                allins.insert(drow)
            except:
                print row
                raise

        for ins in self.part_cache.values():
            ins.close()

        allins.close()

        return True

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    