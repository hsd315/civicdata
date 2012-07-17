'''

'''

from  sourcesupport.uscensus import UsCensusBundle

class Bundle(UsCensusBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
    def prepare(self):
        '''Create the prototype database'''
        from databundles.partition import PartitionIdentity 
        import yaml
        
        self.database.create()

        self.scrape_files()
        self.generate_geo_schema()
        
        
        # Generate all of the partitions
        urls = yaml.load(file(self.urls_file, 'r')) 
        for state, source in urls['geos'].items(): #@UnusedVariable
            self.log("Create partition for "+state)
            # Create the partition
            partition = self.partitions.new_partition(
                           PartitionIdentity(self.identity, space=state))

            partition.database.delete()
            partition.database.create()
            partition.database.load_sql( self.filesystem.path('meta/sf1geo.sql'))
            
        # First install the bundle main database into the library
        # so all of the tables will be there for installing the
        # partitions. 
        self.library.put(self)
        
        return True
 
    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        
        from databundles.partition import PartitionIdentity 
        import yaml 
      
        urls = yaml.load(file(self.urls_file, 'r')) 
        
       
            
        # Process the geo files. 
        for state, source in urls['geos'].items():
            self.load_geo(state, source)
     
    
        return True
    
    def load_geo(self, state, source):
        from databundles.partition import PartitionIdentity 
       
        import petl.fluent as petl
        header, regex = self.get_geo_regex()
  

        
        retry = 4
        while retry > 0:
            retry -= 1
            #try:  
            with self.filesystem.download(source) as zip_file:
                with self.filesystem.unzip(zip_file) as rf:
                    self.log("Processing GEO file: "+rf)
 
                    partition = self.partitions.find(PartitionIdentity(self.identity, space=state))
 
                    # Load the data
                              
                    db_path = partition.database.path
                    self.log("  Partion: "+str(partition))
                    self.log("  To Database: "+db_path)
                     
                    t = petl.fromregex(rf, regex=regex, header=header) #@UndefinedVariable
 
                    if state == 'pr':
                        t = t.convert('NAME', unicode)
 
                    t.progress(100000).appendsqlite3(db_path,'sf1geo')
                          
                    dest = self.library.put(partition)
                    self.log("Install in library: "+dest)
                    partition.database.delete()
                                  
                    return True
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    