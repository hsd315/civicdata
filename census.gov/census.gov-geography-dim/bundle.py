'''

@author: eric
'''
from  databundles.bundle import BuildBundle
import os.path
import yaml
from databundles.library import get_library
 
class Bundle(BuildBundle):
    '''
    Bundle code for US 2010 Census geo files. 
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        bg = self.config.build
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.states_file =  self.filesystem.path(bg.statesFile)

    def prepare(self):
        '''Scrape the URLS into the urls.yaml file and load all of the geo data
        into partitions, without transformation'''
        from databundles.partition import PartitionIdentity
        import re, csv, collections
      
        geo_file = '/Users/eric/proj/github.com/civicdata/census.gov/census.gov-2000_population-sf1-geo/meta/geoschema.csv'
        d = collections.OrderedDict()
        with open(geo_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if  row['description']:
                    print row['column'], row['description']
                    d[row['column']] = row['description']
                    
        geo_file = '/Users/eric/proj/github.com/civicdata/census.gov/census.gov-2010_population-geo-orig/meta/geoschema.csv'

        with open(geo_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if  row['description']:
                    print row['column'], row['description']
                    d[row['column']] = row['description']
            
        geo_file = self.filesystem.path(self.config.build.geoschemaFile)
        with open(geo_file) as f:
            reader = csv.reader(f)
            reader.next
            for row in reader:
                print row
                row[4] = d[row[1]]
                print row
      
        return;
      
        if not self.database.exists():
            self.database.create()

        geo_file = self.filesystem.path(self.config.build.geoschemaFile)
     
        if len(self.schema.tables) == 0 and len(self.schema.columns) == 0:
            self.log("Loading schema from file")
            with open(geo_file, 'rbU') as f:
               self.schema.schema_from_file(f)           
        else:
            self.log("Reusing schema")

        self.schema.create_tables()

        self.database.session.commit()
        
        return True

    def make_generated_geo(self):
        geo_file = self.filesystem.path(self.config.build.gengeoFile)
        with open(geo_file, 'w') as f:
            import csv
            writer = csv.writer(f)
            writer.writerow(['table','column','is_pk','type'])
            for year, cfg in  self.config.queries.items():
                self.make_generated_geo(writer, cfg.source, cfg.template, cfg.data, year)
             

    def _make_generated_geo(self, writer, dataset_name, template, data, year):
        """Create a schema file for the queries to extract data from the 
        source partition. 
        
        This method executed the queries defined in the 'meta' configuration, 
        then creates a schema for that query, based on the first row. 
        """
        from databundles.identity import PartitionIdentity
        
        ds = get_library().get(dataset_name) 
        year_suffix = str(year)[2:]
        
        for qd in data:
            
            table_name = str(qd[1])+year_suffix
            writer.writerow([table_name,qd[1]+'_id', 1, 'INTEGER'])
            writer.writerow([table_name,'geoid', None, 'TEXT'])
             
            # Source partition          
            partition = ds.bundle.partitions.find(grain=qd[0])
            
            if not partition: 
                raise Exception("Failed to get partition for grain {} from dataset {}"
                                .format(qd[0], dataset_name))
            
            row =  partition.database.session.execute(template.format(*qd)).first()
            
            for k,v in zip(row.keys(), row):
                
                try:
                    int(v)
                    type = "INTEGER"
                except:
                    type = "TEXT"
                    
                if k in ('name'):
                    type = 'INTEGER'
                    
                writer.writerow([table_name, str(k),None,type])
                        
    def build(self):
        
        for year, cfg in  self.config.queries.items():
           self.load(year, cfg)
           
        return True

    def load(self, year, config ):
        from databundles.identity import PartitionIdentity
        
        year_suffix = str(year)[2:]
        
        ds = get_library().get(config.source) 
        qt = config.template
          
        for qd in config.data:
            table_name = str(qd[1])+year_suffix
            
            pid = PartitionIdentity(self.identity, grain=qd[1])
            dp = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not dp:
                dp = self.partitions.new_partition(pid)
                dp.database.create(copy_tables = False)
                #dp.create_with_tables() 
            
            sp = ds.bundle.partitions.find(grain=qd[0])
            print sp.identity.name, sp.database.path
            
            with dp.database.inserter(table_name) as ins:
                q = qt.format(*qd)
                print q
                for row in  sp.database.session.execute(q):
                    geo_id = 'foo'
                    ins.insert((None, geo_id)+tuple(row))
    
import sys

if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
