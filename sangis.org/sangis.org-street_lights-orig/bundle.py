'''

'''


from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    ### Meta is run before prepare, to load or configure meta information

    def meta(self):
        return True
 
    ### Prepare is run before building, part of the devel process.  

    def prepare(self):
        
        if not self.database.exists():
            self.database.create()
        
        return True
        
    ### Build the final package

    def build(self):
        import gdal, ogr
        from databundles.identity import PartitionIdentity
        
        url = self.config.build.url

        zip_file = self.filesystem.download(url)
        
        pid = PartitionIdentity(self.identity, table='street_lights_g')
        partition = self.partitions.find(pid)

        if not partition:
        
            for f in self.filesystem.unzip_dir(zip_file):
                if not f.endswith('.shp'):
                    continue
                
               
                partition = self.partitions.new_geo_partition( pid, f)
            
        def progress_f(i):
            if i%10000 == 0:
                self.log("Converted {} records".format(i))
                
        partition.convert('street_lights', progress_f = progress_f )
        
        return True


    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    