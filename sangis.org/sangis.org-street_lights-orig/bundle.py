'''

'''


from  databundles.bundle import BuildBundle


class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

        
    ### Build the final package


    def build(self):
        """Perform the initial import, then convert to imported shapefile 
        partition to one that has lat and lon columns. """
        from databundles.identity import PartitionIdentity
        import re
        
        url = self.config.build.url

        zip_file = self.filesystem.download(url)
        
        # This is the geo partition, which holds a Spatialite database, 
        # a sqlitedatabase with some special features, which aren't available unless
        # you have Spatialite installed. 
        pid = PartitionIdentity(self.identity, table='street_lights_g')
        shape_partition = self.partitions.find(pid)

        if not shape_partition:  
            # Need to use unzip_dir, and not break when we find the file, for Shapefiles. 
            # The .shpfile opten cant be loaded without the ocrresponding .shx file in the
            # same directory. 
            for f in self.filesystem.unzip_dir(zip_file, regex=re.compile('.*\.shp$')):
                shape_partition = self.partitions.new_geo_partition( pid, f)
          
        pid = PartitionIdentity(self.identity, table='street_lights')
        partition = self.partitions.find(pid)  
        
        if not partition:  
            def progress_f(i):
                if i%10000 == 0:
                    self.log("Converted {} records".format(i))
                    
            shape_partition.convert('street_lights', progress_f = progress_f )
        
            partition = self.partitions.find(pid)  
        
        return partition

        

  

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    