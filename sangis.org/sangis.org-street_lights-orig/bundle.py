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

        # This is the geo partition, which holds a Spatialite database, 
        # a sqlitedatabase with some special features, which aren't available unless
        # you have Spatialite installed. 
        
        pid = PartitionIdentity(self.identity, table='street_lights')
        
        try: shape_partition = self.partitions.find(pid)
        except: shape_partition = None
        
        if not shape_partition:  

            # Use download_shape file instead of download an unzip to ensure that the whole
            # shapefile is extracted before using the .shp file, which is often useless without the
            # corresponding .shx. 
            shp_file= self.filesystem.download_shapefile(url)
 
            shape_partition = self.partitions.new_geo_partition( pid, shp_file)
          
        return True

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    