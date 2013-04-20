'''

'''

from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    def prepare(self):
        from databundles.identity import PartitionIdentity
        from databundles.geo.analysisarea import get_analysis_area
        
        if not self.database.exists():
            self.database.create()
        
        if len(self.schema.tables) == 0 and len(self.schema.columns) == 0:
            self.log("Loading schema from file")
            with open(self.config.build.schema_file, 'rbU') as f:
                self.schema.schema_from_file(f)        
     
        self.database.commit()
     
        for aa_id, aa_name in self.config.build.analysisareas.items():
            aa = get_analysis_area(self.library, geoid=aa_id)
            
            if aa:
                pid = PartitionIdentity(self.identity, table='incidents', space=aa_id)
                p = self.partitions.find_or_new(pid)
                self.log("Created partition {} ".format(p.name))

        return True

    def build(self):
        from databundles.geo.analysisarea import get_analysis_area
        from databundles.identity import PartitionIdentity
        
        #
        # Create an array of all of the AAs, and compute the combined bounding box, 
        # so we can restrict the query
        #
        aas = []
        lonmin = 180
        lonmax = -180
        latmin = 180
        latmax = -180
        
        for aa_id, aa_name in self.config.build.analysisareas.items():
            aa = get_analysis_area(self.library, geoid=aa_id)
            part = self.partitions.find(PartitionIdentity(self.identity, table='incidents', space=aa_id))
            aas.append((aa, 
                        part, 
                        part.database.inserter(), 
                        aa.get_translator()))

            lonmax  = max(lonmax, aa.lonmax )
            lonmin  = min(lonmin, aa.lonmin )
            latmax = max(latmax, aa.latmax )
            latmin = min(latmin, aa.latmin )

        #
        # Select only the points in the combined analysis area, then partition the points to each of
        # the AA sets. 
        #
        q = self.config.build.incident_query.format(lonmax=lonmax, lonmin=lonmin, latmax=latmax, latmin=latmin)
        
        cr = self.library.dep('crime')
        
        type_map = self.config.build.type_map
        
        rows = 0
        for row in cr.bundle.query(q):
            rows += 1
            if rows % 10000 == 0:
                self.log("Processed {} Rows".format(rows))

            row = dict(row)
            nr = dict(row)
            nr['lat'] = row['lat']
            nr['lon'] = row['lon']
            nr['type'] = type_map[row['legend']]
            
            if row['number']:
                row['number'] = str(int(row['number']) / 100)+'xx'
            nr['address'] = "{number} {street}, {city}, {zip}".format(**row)
            
            for aa, part, ins, trans in aas:
                if aa.is_in_ll(row['lon'],row['lat']):
                    nr['analysisarea'] = aa.geoid
                    p = trans(row['lon'], row['lat'])
                    nr['cellx'] = p.x
                    nr['celly'] = p.y
                   
                    ins.insert(nr)
        
        for aa, part, ins, trans in aas:
            ins.close()
        
        return True
   
    def demo(self):
        from databundles.library import QueryCommand as q
        from databundles.geo.analysisarea import get_analysis_area
        from databundles.geo.kernel import GaussianKernel
        import numpy as np
        
        aa = get_analysis_area(self.library, geoid='CG0666000')
        
        r =  self.library.find(q().identity(id='a2z2HM').partition(table='incidents',space=aa.geoid)).pop()
    
        p = self.library.get(r.partition).partition
         
        a = aa.new_array()
         
        k = GaussianKernel(33,11)
         
        for row in p.query("select date, time, cellx, celly from incidents"):
            k.apply_add(a, row['cellx'],row['celly'] )

import sys

if __name__ == '__main__':
    import databundles.run
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    