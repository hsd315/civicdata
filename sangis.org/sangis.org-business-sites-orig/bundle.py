'''

'''

from  databundles.bundle import BuildBundle
from databundles.util import AttrDict

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    def build(self):
        from databundles.identity import PartitionIdentity

        pid = PartitionIdentity(self.identity, table='businesses')
        gp = self.partitions.new_geo_partition(pid, self.config.build.sources.businesses)

        return True
        
    def extract_image(self, data):

        import databundles.geo as dg
        from databundles.geo.analysisarea import get_analysis_area
        from osgeo.gdalconst import GDT_Float32

        aa = get_analysis_area(self.library, geoid='CG0666000')
        trans = aa.get_translator()

        a = aa.new_array()

        k = dg.GaussianKernel(33,11)

        p = self.partitions.find(table='businesses')

        for row in p.query("""
        SELECT *, X(Transform(geometry, 4326)) AS lon, Y(Transform(geometry, 4326)) AS lat  
        FROM businesses"""):

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
     
    
    