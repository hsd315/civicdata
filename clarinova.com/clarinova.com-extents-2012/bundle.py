'''

'''


from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        self._sp_zones = None

    def prepare(self):
        '''Scrape the URLS into the urls.yaml file and load all of the geo data
        into partitions, without transformation'''
        from databundles.partition import PartitionIdentity
      
        if not self.database.exists():
            self.database.create()
     
        if len(self.schema.tables) == 0 and len(self.schema.columns) == 0:
            self.log("Loading schema from file")
            with open(self.filesystem.path(self.config.build.schema_file), 'rbU') as f:
               self.schema.schema_from_file(f)      
               self.schema.create_tables()     
        else:
            self.log("Reusing schema")

        return True

    def get_zones(self):
        """Get the GDAL geometry object for each of the StatePlane zones, so 
        we can determine which state plane zone each city, county, etc is in"""
        
        import ogr 
        
        zone_file = self.filesystem.download_shapefile(self.config.build.stateplane_url)

        shapefile = ogr.Open(zone_file)
        layer = shapefile.GetLayer(0)
    
        zones = {}
        
        for i in range(layer.GetFeatureCount()):
            feature = layer.GetFeature(i)
            fips = feature.GetField("FIPSZONE83") # FIPS Code of the stateplane zone
            geometry = feature.GetGeometryRef()
            #print i, name, geometry.GetEnvelope()
        
            # Must clone, or will segfault b/c of links to shapefile, layer, feature, 
            # which are deleted when they go out of scope. 
            zones[fips] =  geometry.Clone() 
            
        return  zones            
           
    @property       
    def sp_zones(self):
        if not self._sp_zones:
            self._sp_zones = self.get_zones()
            
        return self._sp_zones
        
    def find_sp_zone(self, geometry):
        in_zone = None
        dist_zone = None
        min_dist = None
        
        centroid = geometry.Centroid()

        for fips,  zone in self.sp_zones.items():   
            
            zcentroid = zone.Centroid()
            
            if zone.Contains(centroid) or zone.Intersect(geometry):
                in_zone = fips

            # The zone file is very coarse, so not all of the places are actually in
            # any zone, so try to find the nearest zone. 
            dist = zone.Distance(centroid)
            if min_dist is None or dist < min_dist:
                dist_zone = fips
                min_dist = dist
        
        if in_zone is None and dist_zone is not None:    
            in_zone = dist_zone
            
        return in_zone
                    
    def load_spcs(self):
        '''Load the zone.csv file into a table'''
        import csv
        
        with open(self.filesystem.path(self.config.build.zone_file)) as f:
            reader = csv.reader(f)
            
            self.database.clean_table('spcs')
            with self.database.inserter('spcs') as ins:
                reader.next() # Skip header
                for row in reader:
                    ins.insert( [None]+row)
            
    def get_zone(self, fips):
        """Return a SPCS zone record given the zone's FIPS code"""
        t = self.database.table('spcs')
        
        query = t.select().where(t.columns.fips == fips)
              
        return self.database.session.execute(query).first()
        
    def build(self):
        """Download a TIGER shapefile for the places in each state, then
        extract the shape of each  place """
        import re 
        import ogr
        from databundles.geo.analysisarea import  create_bb
        
        self.load_spcs()
        
        ogr.UseExceptions()  

        t = self.config.build.templates.place # URL Template for PLACE shapefiles
        
        def rd(v):
            """Round down, to the nearest even 100"""
            import math
            return math.floor(v/100.0) * 100
        
        def ru(v):
            """Round up, to the nearest even 100"""
            import math
            return math.ceil(v/100.0) * 100
        
        r = self.library.dep('geo')
    
        
        # Iterate over the states first, since the place zone files are borken 
        # out by state. 
        
        count = 0
        self.database.clean_table('places')
        query = "select state, name from states order by state asc"
        for row in r.bundle.database.connection.execute(query):
        
            if row['state'] !=  6 :
                continue
            
            url = t.format(state_fips=row['state'])
    
            self.log("Processing state {}".format(row['name']))
    
            zip_file = self.filesystem.download(url)
            
            for file_ in self.filesystem.unzip_dir(zip_file, 
                                regex=re.compile('.*\.shp$')): pass # Should only be one
            
            shapefile = ogr.Open(file_)
            layer = shapefile.GetLayer(0)
            
            ##
            ## Rasterize with: http://gdal.org/gdal__alg_8h.html#a50caf4bc34703f0bcf515ecbe5061a0a
            ##

            with self.database.inserter('places') as ins:
                prefix = 'CG' # For Census geoids
                for i in range(layer.GetFeatureCount()):
                    
                    count += 1
                    if count%500 == 0:
                        self.log("Loaded {} extents".format(i))
                    
                    feature = layer.GetFeature(i)
                    
                    name = feature.GetField("NAME")
           
                    geometry = feature.GetGeometryRef()

                    fips = self.find_sp_zone(geometry)
                    
                    zone_r = self.get_zone(fips)
           
                    srs = ogr.osr.SpatialReference(str(zone_r['srswkt']))
                    
                    if False:
                        try:
                            geometry.TransformTo(srs)
                        except Exception as e:
                            self.error('Failed to transform with SRS: {}'.format(str(zone_r['srswkt'])))
                            continue
                        env2 = geometry.GetEnvelope()

                    # Because of the diferent SRS, the envelope will probably rotate
                    # when it is projected. So, we need to take the envelope of the projected (rotated)
                    # envelope, to ensure the original envelope of the place fits entirely in the
                    # analysis area. 
                    #
                    # So, we are (1) converting the place envelope to a shape, (2)
                    # transforming it to the AnalysisArea SRS, then taking the envelope again, 
                    # this time in the new SRS. 
                    
                    env1_bb = create_bb(geometry.GetEnvelope(), geometry.GetSpatialReference())
                    env1_bb.TransformTo(srs)       
                    env2_bb = create_bb(env1_bb.GetEnvelope(), env1_bb.GetSpatialReference())

                    env1 = geometry.GetEnvelope()
                    env2 = env1_bb.GetEnvelope()
                    
                    r = {'spcs_id' : zone_r['spcs_id'],
                         'geoid' : prefix+feature.GetField("GEOID"),
                         'name' : name.decode('latin1'), # Handle 8-bit values
                         'statefp': feature.GetField("STATEFP"),
                         'placens': feature.GetField("PLACENS"),
                         'placefp': feature.GetField("PLACEFP"),
                         'latmin':  env1[2],
                         'latmax': env1[3],
                         'lonmin': env1[0],
                         'lonmax': env1[1],
                        
                         'northmin': rd(env2[2]),
                         'northmax': ru(env2[3]),
                         'eastmin': rd(env2[0]),
                         'eastmax': ru(env2[1]),

                         }
                    
                    ins.insert(r)
                    
        return True


        
    def test(self, *args, **kwargs):
        
   

        t =  aa.get_transformer()
        
        print aa
        print 'Min',t(-117.36082,33.11470)


import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    