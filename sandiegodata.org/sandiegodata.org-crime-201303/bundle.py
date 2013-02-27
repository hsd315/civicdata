'''

'''


from  databundles.bundle import BuildBundle
import os

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    def build(self):
        return True
      
    def extract_array(self, data, file_name=None):
        """Extract data from the database and store it in an HDF5 file. This file
        is then used for extracting other images. """
        import h5py

        from databundles.identity import PartitionIdentity
        from databundles.geo.density import DensityImage, LinearMatrix,GaussianMatrix
        from numpy import histogram, ndenumerate, set_printoptions
        from databundles.geo.util  import BoundingBox
        import os
        
        file_name = self.identity.name+".h5"
    
        file_ = self.filesystem.path('extracts', file_name)
        
        #Write to the userblock
        f = h5py.File(file_)
      
        counts = f.require_group("counts")
        dataset_name = data['filetitle']

        # Exit if we have already aded the data. 
        if  dataset_name in counts:
            return file_
        
        r = self.library.dep('crime')
        query = data['query']
        extents = BoundingBox(min_x=data['min_lon'],
                              min_y=data['min_lat'], 
                              max_x=data['max_lon'], 
                              max_y=data['max_lat'])
        
        self.log("Extracting array: {} into {}".format(data['filetitle'], file_name))
    
        bin_scale = data['bin_scale'] # cells per degree
        di = DensityImage(extents, bin_scale)

        for i,row in enumerate(r.bundle.database.connection.execute(query)):
            if i%1000 == 0:
                self.log("Added {} records".format(i))

            lon = row['longitude']
            lat = row['latitude']
            di.add_count(lon, lat )
        
            
        ds = counts.create_dataset(dataset_name, data=di.a, compression=9)
        
        ds.attrs['bin_scale'] = data['bin_scale']
        ds.attrs['min_lat'] = data['min_lat']
        ds.attrs['min_lon'] = data['min_lon']
        ds.attrs['max_lat'] = data['max_lat']
        ds.attrs['max_lon'] = data['max_lon']
        
        f.close()
        
        return file_
        

    def extract_image(self, data, file_name=None):
        from databundles.geo.util  import BoundingBox
        import pprint
     
        r = self.library.dep('crime')
      
        file_ = self.filesystem.path('extracts', data['filetitle']+".tiff")
        
        if os.path.exists(file_):
            return file_
      
        extents = BoundingBox(min_x=data['min_lon'],
                              min_y=data['min_lat'], 
                              max_x=data['max_lon'], 
                              max_y=data['max_lat'])

        file_ =  self.make_image(r.bundle.database, data['query'],  extents,file_)
    
        return file_ 

    def make_image(self, database, query, extents, file_):
        '''Collect the street_lights into a heat map. '''
        from databundles.identity import PartitionIdentity
        from databundles.geo.density import DensityImage, LinearMatrix,GaussianMatrix
        from numpy import histogram, ndenumerate, set_printoptions
        import random
        
        self.log("Extracting image: {}".format(file_))
    
        bin_scale = 5000 # cells per degree
        rs = 5 # Random spread, in number of cells
        matrix_size = int((bin_scale / 200) / 2) * 2 + 1 # Maxtrix size must be odd
        matrix_dia = matrix_size / 3 # Controls spread of matrix

        m = GaussianMatrix(matrix_size,matrix_dia)
        di = DensityImage(extents, bin_scale, m)

        for i,row in enumerate(database.connection.execute(query)):
            if i%1000 == 0:self.log("Added {} records".format(i))

            try:
                # Randomness can push points outside matrix
                lon = row['longitude'] + (1./bin_scale) * random.randint(-rs, rs)
                lat = row['latitude']  + (1./bin_scale) * random.randint(-rs,rs)
                di.add_matrix(lon, lat )
            except IndexError:
                # Just put those points in the normal position
                di.add_matrix(row['longitude'], row['latitude']  )

        #di.mask()
        di.unity_norm()
        print di.info()
                
        print di.write(file_)
        
        return file_    
    
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    