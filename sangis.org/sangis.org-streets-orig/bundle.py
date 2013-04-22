'''

'''


from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)


    ### Prepare is run before building, part of the devel process.  

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
    
    ### Build the final package

    def load_shapefile(self, url, table_name):
        from databundles.identity import PartitionIdentity
 
        pid = PartitionIdentity(self.identity, table=table_name)
        shape_partition = None
        
        try: shape_partition = self.partitions.find(pid)
        except: pass # Fails with ValueError because table does not exist. 
        
        if not shape_partition:
            shp_file= self.filesystem.download_shapefile(url)
            shape_partition = self.partitions.new_geo_partition( pid, shp_file)


    def build(self):

        def progress_f(i):
            if i%10000 == 0:
                self.log("Converted {} records".format(i))  

        self.load_shapefile(self.config.build.sources.addresses, 'addresses')
        self.load_shapefile(self.config.build.sources.roads, 'roads')
        self.load_shapefile(self.config.build.sources.intersections, 'intersections')
                
        return True
    
    
    def test_geo(self):
        
        p = self.partitions.find(table='addresses')
        
        for row in p.query("select AsText(geometry) from addresses limit 5"):
            print row
    

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    