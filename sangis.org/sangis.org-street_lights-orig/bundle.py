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

    def extract_image(self, data):
        
        import databundles.geo as dg
        from databundles.geo.analysisarea import get_analysis_area
        from osgeo.gdalconst import GDT_Float32
        
        aa = get_analysis_area(self.library, geoid='CG0666000')
        trans = aa.get_translator()

        a = aa.new_array()
        
        k = dg.GaussianKernel(33,11)
        
        p = self.partitions.find(table='street_lights')
        
        for row in p.query("""
            SELECT 
                X(Transform(geometry, 4326)) AS lon, 
                Y(Transform(geometry, 4326)) AS lat 
            FROM street_lights"""):
            
            p = trans(row['lon'], row['lat'])
       
            k.apply_add(a, p)  

        file_name = self.filesystem.path('extracts','{}'.format(data['name']))

        aa.write_geotiff(file_name, 
                    a[...], #std_norm(ma.masked_equal(i,0)),  
                    data_type=GDT_Float32)
        
        return file_name

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    