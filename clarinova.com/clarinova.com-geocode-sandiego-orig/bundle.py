'''

'''

from  databundles.bundle import BuildBundle
 
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        

                
    def prepare(self):
        '''Create the datbase and load the schema from a file, if the file exists. '''
        from databundles.partition import PartitionIdentity
      
        if not self.database.exists():
            self.database.create()

        if self.config.build.get('schema_file', False):
            with open(self.filesystem.path(self.config.build.schema_file), 'rbU') as f:
                self.schema.schema_from_file(f)      
                self.schema.create_tables()     

        return True

    def build(self):
        self.add_roads()
        self.add_addresses()
        
        self.recode_addresses()
        
        return True

    def add_roads(self):
        from databundles.geo.address import Parser
        p = Parser()
        
        codes_b, _ = self.library.dep('codes')
        
        cities = {row['key']:row['value'] for row in 
                  codes_b.database.query("SELECT * FROM codes WHERE `group` = 'jurisdiction' ")}
        
        lr = self.init_log_rate(message='Street segments: ')
        
        _, roads = self.library.dep('roads')
        
        partition = self.partitions.find_or_new(table='segments', tables='addresses')

        seg_ins = partition.database.inserter('segments')
        adr_ins = partition.database.inserter('addresses')
        
        for row in roads.query("""
            SELECT 
            objectid, 
            abloaddr AS number1, 
            abhiaddr AS number2, 
            rd20full as name,
            ljurisdic AS city_code, 
            rjurisdic AS city_code2, 
            l_zip AS zip,
            roadsegid AS street_source_id,
            fnode, tnode, 
            length, toxcoord, toycoord, frxcoord, frycoord, midxcoord, midycoord
            FROM roads
            """):
            
            lr()
            
            if row['name'] == 'ALLEY':
                continue
            
            #
            # NOTE! The l and r prefixes refer to the right and left sides of the road, presumably as
            # you travel from 'from' to 'to' points. Our data, however, refers to the addresses for the
            # start and end of the road, which is not only possibly different, but also illdefined. 
            
            s = "{} {}".format(row['number1'], row['name'])
           
            cc1 = str(row['city_code']).strip()
            cc2 = str(row['city_code2']).strip()
            city_code = cc1 if cc1 else cc2
            
            if city_code == 'None':
                city_code = 'CN'
            
            if not city_code:
                self.error("Missing a city. ")
           
            try:
                ps = p.parse(s)
            except Exception as e:
                self.error("ERROR: {}; {}".format(s, str(e)))
                continue
            
            #print "{:40s} {:40s}".format(s,ps)
            
            a1 = {
                'addresses_id' : row['objectid']*2,
                'addr_source_id' : None, 
                'type' : 'segend', 
                'number' : row['number1'], 
                'street' : ps.street_name, 
                'street_type' : ps.street_type, 
                'street_source_id' : row['street_source_id'], 
                'city' : cities[city_code], 
                'state' : 'CA', 
                'zip' : row['zip'], 
                'lat' : None, 
                'lon' : None, 
                'x' : row['frxcoord'], 
                'y' : row['frycoord']
            }
            
            a2 = dict(a1)
            a2['addresses_id'] = row['objectid']*2 + 1
            a2['number'] = row['number2']
            a2['x'] = row['toxcoord']
            a2['x'] = row['toycoord']

            seg = {
                'address1_id': a1['addresses_id'],
                'node1_source_id': row['fnode'],
                'address2_id': a2['addresses_id'],
                'node2_source_id': row['tnode'],
                'x1': row['toxcoord'],
                'y1': row['toycoord'],
                'x2': row['frxcoord'],
                'y2': row['frycoord'],
                'length': row['length']
            }
            
            seg_ins.insert(seg)
            adr_ins.insert(a1)
            adr_ins.insert(a2)

        seg_ins.close()
        adr_ins.close()
            
        
    def _make_addr_row(self, p, row, city_f):
 
        row = dict(row)
 
        s = "{} {} {} {}".format(int(row['addrnmbr']), 
                                 row['addrpdir'] if row['addrpdir'] else '',
                                 row['addrname'], 
                                 row['addrsfx'] if row['addrpdir'] else '' )
        try:
            ps = p.parse(s)
        except Excetion as e:
            self.error("ERROR: {}; {}".format(s, str(e)))
            return

        if None in ps.street_name:
            raise Exception("Street name has 'None' in it: {} -> {} ".format(s,ps.street_name ))
      
        a = {
            'addr_source_id' : row['objectid'], 
            'type' : 'house', 
            'number' : int(row['addrnmbr']), 
            'street' : ps.street_name, 
            'street_type' : ps.street_type, 
            'street_source_id' : row['roadsegid'], 
            'parcel_source_id' : row['parcelid'], 
            'city' : city_f(row, ps),
            'state' : 'CA', 
            'zip' : row['addrzip'], 
            'lat' : row.get('lat', None), 
            'lon' : row.get('lon', None), 
            'x' : row['x_coord'], 
            'y' : row['y_coord']
        } 
        
        return a              

    def add_addresses(self):
        from databundles.geo.address import Parser
        p = Parser()
        
        lr = self.init_log_rate(message='Addresses: ')
        
        codes_b, _ = self.library.dep('codes')
        
        cities = {row['key']:row['value'] for row in 
                  codes_b.database.query("SELECT * FROM codes WHERE `group` = 'jurisdiction' ")}
        
        
        _, addresses = self.library.dep('addresses')

        partition = self.partitions.find_or_new(table='addresses')

        def city_f(row, ps):
            return row['addrjur'] if row['addrjur'] else "CN"

        with partition.database.inserter('addresses') as ins:
            for row in addresses.query("""SELECT *, X(Transform(geometry, 4326)) AS lon, Y(Transform(geometry, 4326)) AS lat 
                FROM addresses"""):
                
                lr()
                
                a = self._make_addr_row(p, row, city_f)
                
                ins.insert(a)
         
   
    def x_recode_addresses(self):
        from databundles.geo.address import Parser
        import ogr
        import zlib 
        import csv
        
        from databundles.geo.address import Parser

        p = Parser()
        
        _, addresses = self.library.dep('addresses')
        _, municipalities = self.library.dep('municipalities')

        self.database.attach(municipalities,'mu')
        self.database.attach(addresses,'ad')

        self.jurisdiction['CN'] = 'S.D. County'
        rmap = { v:k for k, v in self.jurisdiction.items() }

        partition = self.partitions.find_or_new(table='addresses', grain='recoded')
     
        partition.database.query("DELETE FROM addresses")
            
        rl = self.init_log_rate(N=5000)
                 
        for mr in municipalities.query("SELECT distinct objectid, name FROM municipalities"):
            city = mr['name']
            jcode = rmap[city.title()]

            city_f = lambda row, ps: city.title()
            
            # First load in all of the addresses that properly have a jurisdiction
            with partition.database.inserter('addresses') as ins:
                for row in self.database.query("""
                    SELECT {ad}.addresses.objectid, parcelid, apn, roadsegid, x_coord, y_coord,
                        addrnmbr, addrpdir, addrname, addrjur, addrzip, addrsfx,
                        X(Transform({ad}.addresses.geometry, 4326)) AS lon, Y(Transform({ad}.addresses.geometry, 4326)) AS lat 
                    FROM {ad}.addresses
                    WHERE addrjur = ?""", jcode):
                
                    rl(jcode)
                    
                    a = self._make_addr_row(p, row, city_f)

                    ins.insert(a)
                    
            # Load all of the points that don't have a jurisdiction, but are inside of the municipal geometry
            with partition.database.inserter('addresses') as ins:
                for row in self.database.query("""  
                  SELECT {ad}.addresses.objectid, parcelid, apn, roadsegid, x_coord, y_coord,
                        addrnmbr, addrpdir, addrname, addrjur, addrzip, addrsfx, name,
                        X(Transform({ad}.addresses.geometry, 4326)) AS lon, Y(Transform({ad}.addresses.geometry, 4326)) AS lat 
                    from {ad}.addresses, {mu}.municipalities
                    WHERE 
                        addrjur IS NULL AND 
                        {mu}.municipalities.objectid = ? AND 
                        MbrContains({mu}.municipalities.geometry, {ad}.addresses.geometry) AND 
                        Contains({mu}.municipalities.geometry, {ad}.addresses.geometry)""", mr['objectid']):  
                    
                    rl(row['name'])
                    
                    a = self._make_addr_row(p, row, city_f)
                                
                    ins.insert(a)

    def recode_addresses(self): 
        """Look for addresses that are not in the correct municipality, with a geographic search, and
        correct the entries"""
        import ogr
        import csv

        _, municipalities = self.library.dep('municipalities')
      
        jurisdiction = dict(self.jurisdiction)
        jurisdiction['CN'] = 'S.D. County'
        rmap = { v:k for k, v in jurisdiction.items() }
     
        addresses = self.partitions.find(table='addresses')
        
        rl = self.init_log_rate(N=3000)
 
        for mr in municipalities.query("SELECT distinct objectid, name, AsText(geometry) AS wkt FROM municipalities"):

            city = mr['name']
            jcode = rmap[city.title()]
  
            g = ogr.CreateGeometryFromWkt(mr['wkt'])
            e = g.GetEnvelope()
            
            self.log("Updating {}".format(city))
       
            updates = 0
            with addresses.database.updater(cache_size = 100) as upd:
                for address in addresses.query("""SELECT *  FROM addresses 
                WHERE city != '{jur}' AND x BETWEEN {x1} AND {x2} AND y BETWEEN {y1} and {y2}
                """.format(jur=jcode, x1=e[0], x2=e[1], y1=e[2], y2=e[3])):
            
                    p = ogr.Geometry(ogr.wkbPoint)
                    p.SetPoint_2D(0, address['x'], address['y'])
        
                    if g.Contains(p):
  
                        r = {
                             '_addresses_id' : address['addresses_id'],
                             '_city' : jcode
                             }
                        
                        upd.update(r)
                        
                        rl('Update addresses: ')
                        updates += 1

            self.log("Updated {} addresses ".format(updates))

         
    def test_geo(self): 
        from databundles.geo.geocoder import Geocoder
        from pprint import pprint
        
        _,crime = self.library.dep('crime')
        
        from databundles.geo.address import Parser
        
        g = Geocoder(self.library, addresses_ds='geoaddresses')
        
        lr = self.init_log_rate(N=5000,message='Street segments: ')
        
        errors = 0
        count = 0
        num_returns = {}

        for row in crime.database.query("select * from incidents limit 10000"):

            candidates = g.geocode_semiblock(row['blockaddress'], row['city'], 'CA')

            lr()

            count += 1
            if len(candidates) != 1:
                errors += 1

            num_returns[len(candidates)] = num_returns.get(len(candidates),0) + 1  


        print "Count: {} Errors {}".format(count, errors)
        print num_returns

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    