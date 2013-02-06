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
      
        if not self.database.exists():
            self.database.create()
     
        self.make_generated_geo()
     
        geo_file = self.filesystem.path(self.config.build.gengeoFile)
     
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
        """Create a schema file for the queries to extract data from the 
        source partition. 
        
        This method executed the queries defined in the 'meta' configuration, 
        then creates a schema for that query, based on the first row. 
        """
        
        ds = get_library().get('a1O5Tm') 
     
        qt = self.config.meta.query2000.template

        geo_file = self.filesystem.path(self.config.build.gengeoFile)

        import csv
        with open(geo_file, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['table','column','is_pk','type'])
            
            for qd in self.config.meta.query2000.data:
                
                writer.writerow([qd[1],qd[1]+'_id', 1, 'INTEGER'])
                writer.writerow([qd[1],'geoid', None, 'TEXT'])
                                
                partition = ds.bundle.partitions.find(grain=qd[0])
                row =  partition.database.session.execute(qt.format(*qd)).first()
                
                for k,v in zip(row.keys(), row):
                    
                    try:
                        int(v)
                        type = "INTEGER"
                    except:
                        type = "TEXT"
                        
                    if k in ('name'):
                        type = 'INTEGER'
                        
                    writer.writerow([str(qd[1]), str(k),None,type])
                        
    def build(self):
           
        ds = get_library().get('a1O5Tm') 
        qt = self.config.meta.query2000.template
          
        for qd in self.config.meta.query2000.data:
            
            partition = ds.bundle.partitions.find(grain=qd[0])
            print partition.identity.name, partition.database.path
            
            with self.database.inserter(qd[1]) as ins:
                for row in  partition.database.session.execute(qt.format(*qd)):
                    geo_id = 'foo'
                    ins.insert((None, geo_id)+tuple(row))
    
     

    
import sys

if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
