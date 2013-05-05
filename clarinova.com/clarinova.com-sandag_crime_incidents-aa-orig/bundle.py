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

            row = dict(row)
            nr = dict(row)
            nr['lat'] = row['lat']
            nr['lon'] = row['lon']
            nr['type'] = type_map[row['legend']]
            
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
   

import sys

if __name__ == '__main__':
    import databundles.run
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    