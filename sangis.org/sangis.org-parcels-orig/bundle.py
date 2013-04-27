'''
'''

from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
 
    def build(self):
        from databundles.identity import PartitionIdentity

        pid = PartitionIdentity(self.identity, table='parcels')
        gp = self.partitions.new_geo_partition(pid, self.config.build.sources.parcels)

        return True
        
    def add_indexes(self):

        p = self.partitions.find(table='parcels')
        p.database.query('CREATE INDEX IF NOT EXISTS apn_idx ON parcels (apn, apn_8);')
        p.database.query('CREATE INDEX IF NOT EXISTS parcel_idx ON parcels (parcelid);')
        p.database.query('CREATE INDEX IF NOT EXISTS road_idx ON parcels (addrno, roadname);')
        p.database.query('CREATE INDEX IF NOT EXISTS sroad_idx ON parcels (situs_addr, situs_stre);')
        p.database.query('CREATE INDEX IF NOT EXISTS zip_idx ON parcels (zip);')
        p.database.query('CREATE INDEX IF NOT EXISTS szip_idx ON parcels (situs_zip);')
        p.database.query('CREATE INDEX IF NOT EXISTS jur_idx ON parcels (situs_juri);')
     
# SELECT addrno, roadpdir, roadname, roadsfx, zip, situs_addr, situs_pre_, situs_stre, situs_post, situs_juri, situs_suff,  situs_zip
         
import sys

if __name__ == '__main__':
    import databundles.run

    databundles.run.run(sys.argv[1:], Bundle)    
    