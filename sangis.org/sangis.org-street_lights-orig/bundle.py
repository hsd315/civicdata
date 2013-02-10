'''

'''


from  databundles.bundle import BuildBundle
from numpy  import *

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

        partition = self.build_import()

        self.extract_density_tiff()

        return True
    
    
    
    def build_import(self):
        """Perform the initial import, then convert to imported shapefile 
        partition to one that has lat and lon columns. """
        from databundles.identity import PartitionIdentity
        
        url = self.config.build.url

        zip_file = self.filesystem.download(url)
        
        pid = PartitionIdentity(self.identity, table='street_lights_g')
        shape_partition = self.partitions.find(pid)

        if not shape_partition:
        
            for f in self.filesystem.unzip_dir(zip_file):
                if not f.endswith('.shp'):
                    continue
               
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


    def extract_density_tiff(self):
        '''Collect the street_lights into a heat map. '''
        from databundles.identity import PartitionIdentity
        from osgeo import gdal, gdal_array, osr
        from osgeo.gdalconst import GDT_Float32, GDT_Byte, GDT_Int16

     
        pid = PartitionIdentity(self.identity, table='street_lights')
        partition = self.partitions.find(pid) 

        e=partition.database.connection.execute
        
        # Find the extents of the data and figure out the offsets for the array. 
        r = e("""SELECT min(_db_lat) as min_y, min(_db_lon) as min_x, 
                max(_db_lat) as max_y, max(_db_lon) as max_x from street_lights """).first()
 
        bin_scale = 1000.
        i_bin_scale = 1 / float(bin_scale)


        x_offset_c = int(r['min_x']*bin_scale)-2
        y_offset_c = int(r['min_y']*bin_scale)-2  

        x_max_c = int(r['max_x']*bin_scale)
        y_max_c = int(r['max_y']*bin_scale)    

        x_offset_d = x_offset_c/bin_scale
        y_offset_d = y_offset_c/bin_scale
       
        # Size of the output array
        x_size = x_max_c - x_offset_c + 4
        y_size = y_max_c - y_offset_c + 4
   
        print 'OFFSETS', x_offset_d, y_offset_d
        print "size",  x_size, y_size
        print 'UL Corner (x,y)',r['min_x'], r['max_y']
        print 'LR Corner (x,y)',r['max_x'], r['min_y']
        
        a =  zeros( (  y_size, x_size),  dtype=float )
    
        print 'SHape',a.shape
    
        m =  outer(array([1.,2.,1.]),array([1.,2.,1.]))
        m /= sum(m)

        # Draw registration marks at the corners. 
        if False:
            for i in range(0,30,2):
                a[i,0] = 1
                a[0,i] = 1
                
                a[y_size-i-1,x_size-1] = 1
                a[y_size-1,x_size-i-1] = 1
                
                a[y_size-i-1,0] = 1
                a[0,x_size-i-1] = 1
                
                a[i,x_size-1] = 1
                a[y_size-1,i] = 1
        
        for i,row in enumerate(partition.database.connection.execute("select * from street_lights")):
            
            x = int(row['_db_lon']*bin_scale) - x_offset_c 
            y = int(row['_db_lat']*bin_scale) - y_offset_c 
      
            x -= 1
      
            if row['ogc_fid'] in [34307, 8040]:
                print row['ogc_fid'],  x, y
             
            
            if False:   
                # Sum number of lamps
                a[y,x] += 1
            else:
                # Add in smoothing matrix
                for (x_m,y_m), value in ndenumerate(m):
                    #print x,x_m,x+x_m,' : ',y,y_m,y+y_m
                    a[y+y_m-1][x+x_m-1] += value
        
     
        driver = gdal.GetDriverByName('GTiff') 
        file_ = self.filesystem.path('extracts','street_lamp_density.tiff')
        out = driver.Create(file_, a.shape[1], a.shape[0], 1, GDT_Float32)  
        
        transform = [ x_offset_d ,  # Upper Left X postion
                     i_bin_scale ,  # Pixel Width 
                     0 ,     # rotation, 0 if image is "north up" 
                     y_offset_d ,  # Upper Left Y Position
                     0 ,     # rotation, 0 if image is "north up"
                     i_bin_scale # Pixel Height
                     ]

        out.SetGeoTransform(transform)  
        
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        out.SetProjection( srs.ExportToWkt() )
     
        out.GetRasterBand(1).SetNoDataValue(0)
        out.GetRasterBand(1).WriteArray(a)
      
        return file_
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    