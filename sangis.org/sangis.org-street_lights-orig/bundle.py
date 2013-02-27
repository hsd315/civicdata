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

        partition = self.build_import()

        self.extract_density_tiff()

        return True
    
    def build_import(self):
        """Perform the initial import, then convert to imported shapefile 
        partition to one that has lat and lon columns. """
        from databundles.identity import PartitionIdentity
        import re
        
        url = self.config.build.url

        zip_file = self.filesystem.download(url)
        
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

        
    def extract_density(self, data, file_name=None):
  
        '''Collect the street_lights into a heat map. '''
        from databundles.identity import PartitionIdentity
        from databundles.geo.density import DensityImage, LinearMatrix,GaussianMatrix
        from numpy import histogram, ndenumerate, set_printoptions

        
        pid = PartitionIdentity(self.identity, table='street_lights')
        partition = self.partitions.find(pid) 

        bin_scale = 5000 # cells per degree
        matrix_size = int((bin_scale / 500) / 2) * 2 + 1 # Maxtrix size must be odd
        matrix_dia = matrix_size / 3 # Controls spread of matrix

        di = DensityImage(partition.extents, bin_scale, GaussianMatrix(matrix_size,matrix_dia))
   
        #limit_where = 'where _db_lon < -117 and _db_lat < 32.8 and _db_lat >32.6'
   
        for i,row in enumerate(partition.database.connection.execute("select * from street_lights ")):
            di.add_matrix(row['_db_lon'], row['_db_lat'])
          
        if not file_name:  
            file_name = self.filesystem.path('extracts',partition.table.name+".tiff")

        di.mask()
        di.std_norm()
        print di.info()
                
        print di.write(file_name)
        
        return file_name
  

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    