'''

'''

from  databundles.bundle import BuildBundle
 
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    def build(self):
        self.add_roads()
        self.add_addresses()
        self.add_views()
        self.fix_address_range()
        
        #self.recode_addresses()
        
        return True

    def add_roads(self):
        from pprint import pprint
        from databundles.geo.address import Parser
        import fuzzy
        
        p = Parser()
        
        codes_b, _ = self.library.dep('codes')
        
        cities = {row['key']:row['value'] for row in 
                  codes_b.database.query("SELECT * FROM codes WHERE `group` = 'jurisdiction' ")}
        
        dmeta = fuzzy.DMetaphone()
        
        lr = self.init_log_rate(message='Street segments: ')
        
        _, roads = self.library.dep('roads')
        
        partition = self.partitions.find_or_new(table='segments', tables='addresses')
        
        try:
            partition.database.query("DELETE FROM segments")
            partition.database.query("DELETE FROM addresses")
        except:
            pass

        with partition.database.inserter('segments') as ins:
            for row in roads.query("""
                SELECT 
                abloaddr AS lnumber, abhiaddr AS hnumber, 
                rd20full as full_street,
                roadsegid AS segment_source_id,
                roadid AS road_source_id, 
                ljurisdic AS lcitycode, rjurisdic AS rcitycode, 
                l_zip AS lzip, r_zip AS rzip,
                speed, 
                l_block AS lblock, r_block AS rblock,
                segclass, funclass,
                ( CASE 
                  WHEN segclass IN ('1', '2', '7', '8', '9','A','Z','P','H','W') THEN 0 
                  WHEN abloaddr = 0 AND abhiaddr = 0 THEN 0
                  ELSE 1 END
                ) AS has_addresses,
                fnode AS node1_source_id, tnode AS node2_source_id, 
                toxcoord AS x2, toycoord AS y2, 
                frxcoord AS x1, frycoord AS y1, 
                midxcoord AS xm, midycoord AS ym,
                length, 
                AsBinary(geometry) as wkb
                FROM roads
                """):
                
                lr()
                
                if row['full_street'] == 'ALLEY':
                    continue
                
                row = dict(row)
                
                try: row['rcity'] = cities[row['rcitycode']]
                except: row['rcity'] = 'none'
                
                try:  row['lcity'] = cities[row['lcitycode']]          
                except: row['lcity'] = 'none'
                
                
                s = "{} {}".format(row['lnumber'], row['full_street'])
               
                try:
                    ps = p.parse(s)
                except Exception as e:
                    self.error("ERROR: {}; {}".format(s, str(e)))
                    continue
    
                row['street_dir'] = ps.street_direction
                row['street'] = ps.street_name
                row['street_type'] = ps.street_type
                row['street_metaphone'] = dmeta(ps.street_name)[0]
    
                ins.insert(row)
  
    def _make_addr_row(self, p, row, city_f):
 
        row = dict(row)
 
        s = "{} {} {} {}".format(int(row['addrnmbr']), 
                                 row['addrpdir'] if row['addrpdir'] else '',
                                 row['addrname'] if row['addrname'] else '', 
                                 row['addrsfx'] if row['addrsfx'] else '' )
        try:
            ps = p.parse(s)
        except Exception as e:
            self.error("ERROR: {}; {}".format(s, str(e)))
            return

        if not ps.street_name:
            self.error("ERROR, no street name: {}".format(s))
            return

        if 'None' in ps.street_name:
            raise Exception("Street name has 'None' in it: {} -> {} ".format(s,ps.street_name ))
      
        a = {
            'addr_source_id' : row['objectid'], 
            'type' : 'house', 
            'number' : int(row['addrnmbr']), 
            'street' : ps.street_name, 
            'street_dir' : ps.street_direction, 
            'street_type' : ps.street_type, 
            'segment_source_id' : row['roadsegid'], 
            'parcel_source_id' : row['parcelid'], 
            'city' : city_f(row, ps),
            'state' : 'CA', 
            'zip' : row['addrzip'], 
            'lat' : row.get('lat', None), 
            'lon' : row.get('lon', None), 
            'x' : row['x_coord'], 
            'y' : row['y_coord'],
            'error_notes' : None
        } 
        
        return a              

    def add_addresses(self):
        from databundles.geo.address import Parser
        from sqlalchemy.sql import bindparam, select
        import json
        p = Parser()
        
        lr = self.init_log_rate(message='Addresses: ')
        
        codes_b, _ = self.library.dep('codes')
        
        cities = {row['key']:row['value'] for row in 
                  codes_b.database.query("SELECT * FROM codes WHERE `group` = 'jurisdiction' ")}
        
        _, addresses = self.library.dep('addresses')

        partition = self.partitions.find_or_new(table='segments')

        def city_f(row, ps):
            return cities[row['addrjur']] if row['addrjur'] else cities["CN"]

        partition.query("DELETE FROM addresses")

        partition.database.attach(addresses, 'ad')


        # Query to search for a segment by address
        t = partition.database.table('segments')
        select = (select([t.c.segments_id])
                  .where(t.c.street == bindparam('street'))
                  .where((t.c.street_dir == None) | (t.c.street_dir == bindparam('street_dir')))
                  .where((t.c.street_type == None) | (t.c.street_type == bindparam('street_type')))
                  .where(t.c.lnumber <= bindparam('number'))
                  .where(t.c.hnumber >= bindparam('number'))
                  )

        row_count = 0.0
        bad_seg = 0.0
        fixed_seg = 0.0
        
        with partition.database.inserter('addresses') as ins:
            for row in partition.database.query("""
            SELECT *, X(Transform(addr.geometry, 4326)) AS lon, Y(Transform(addr.geometry, 4326)) AS lat 
            FROM {ad}.addresses as addr
            LEFT JOIN segments ON addr.roadsegid == segments.segment_source_id
            """):
                lr()
                errors = []
                a = self._make_addr_row(p, row, city_f)
             
                if not a:
                    continue 
                
                if a['segment_source_id'] == 0:
                    errors.append('No Segment Id')
                elif a['number'] > row['hnumber'] or a['number'] < row['lnumber']:
                    errors.append('Number Range')
                elif a['street'].title() != row['street']:
                    errors.append('Street Name')

                if len(errors) > 0:     
                    bad_seg += 1

                    r = partition.database.connection.execute(select, street=a['street'], street_dir = a['street_dir'], 
                                        street_type = a['street_type'], number = a['number']).first()
                
                    if r is not None:
                        a['segment_source_id']  = r[0]
                        errors.append('Segment Fixed')
                        fixed_seg += 1
                    else:
                        a['segment_source_id'] = 0
                        errors.append('Segment Not Fixed')

                    a['error_notes'] = json.dumps(errors)

                row_count += 1

                if row_count % 25000 == 0:
                    self.log("{} {} {} bad={}% fixed={}%"
                             .format(row_count, bad_seg, fixed_seg, 
                                     int(bad_seg/row_count*100), 
                                     ( int(fixed_seg/bad_seg*100) if bad_seg else 0 )))

                if a:
                    ins.insert(a)

  
    def recode_addresses(self): 
        """Look for addresses that are not in the correct municipality, with a geographic search, and
        correct the entries"""
        import ogr
        import csv
        import json

        _, municipalities = self.library.dep('municipalities')
      
        codes_b, _ = self.library.dep('codes')
        
        cities = {row['key']:row['value'] for row in 
                  codes_b.database.query("SELECT * FROM codes WHERE `group` = 'jurisdiction' ")}

        rmap = { v:k for k, v in cities.items() }
     
        addresses = self.partitions.find(table='segments')
        
        rl = self.init_log_rate(N=3000)
 
        for mr in municipalities.query("SELECT distinct objectid, name, AsText(geometry) AS wkt FROM municipalities"):

            city = mr['name'].title()
  
            g = ogr.CreateGeometryFromWkt(mr['wkt'])
            e = g.GetEnvelope()
            
            self.log("Updating {}".format(city))
       
            updates = 0
            
            q = """SELECT addresses.*  
                FROM addresses 
                LEFT JOIN segments ON addresses.segments_id == segments.segments_id
                WHERE city != '{city}' AND x BETWEEN {x1} AND {x2} AND y BETWEEN {y1} and {y2}
                """.format(city=city, x1=e[0], x2=e[1], y1=e[2], y2=e[3])
            
            print q
            
            with addresses.database.updater('addresses', cache_size = 100) as upd:
                for address in addresses.query(q):
            
                    address = dict(address)
            
                    p = ogr.Geometry(ogr.wkbPoint)
                    p.SetPoint_2D(0, address['x'], address['y'])
        
                    if g.Contains(p):

                        if address['error_notes']:
                            e = json.loads(address['error_notes'])
                            e.append('Recoded City')
                            errors = json.dumps(e)
                        else:
                            errors = None
  
                        r = {
                             '_addresses_id' : address['addresses_id'],
                             '_city' : city,
                             '_error_notes' : errors
                             }
                        
                        upd.update(r)
                        
                        rl('Update addresses: ')
                        updates += 1

            self.log("Updated {} addresses ".format(updates))

         
    def add_views(self):
        
        segments = self.partitions.find(table='segments')
        
        for name, code in self.config.views.items():
            
            sql = "DROP VIEW IF EXISTS {}; ".format(name)
            segments.database.connection.execute(sql)
            
            sql = "CREATE VIEW {} AS {};".format(name, code)
            segments.database.connection.execute(sql)
            
            
    def fix_address_city(self):
        """Look for addresses that are not in the correct municipality, with a geographic search, and
        correct the entries"""
        import ogr
        import csv
        import json

        addresses = self.partitions.find(table='segments')
        
        rl = self.init_log_rate(N=3000)

        with addresses.database.updater('segments', cache_size = 100) as upd:
            for row in addresses.query("""
            SELECT segments_id, street FROM segments
            WHERE street like '%th' OR  street like '%rd' OR  street like '%st'  OR  street like '%nd' 
            """):
        
                row = dict(row)  
                
                r = { 
                     '_segments_id' : row['segments_id'],
                     '_street': row['street'].title()
                      }
                
                print r
                upd.update(r)
  
    def fix_address_range(self):
        
        segments = self.partitions.find(table='segments')
        
        # Reverse the numbers if the low is higher than the high
        q = """
            UPDATE segments SET lnumber = hnumber, hnumber = lnumber WHERE lnumber > hnumber   
        """
        
        # Link setments together by their nodes and look for breaks in the address range. 
        q = """
        select s2.segments_id, s1.lnumber AS lnumber1, s1.hnumber AS hnumber1, 
        s2.lnumber AS lnumber2, s2.hnumber AS hnumber2,
        s1.street_dir,  s1.street
        FROM segments as s1, segments as s2
        WHERE s1.node2_source_id = s2.node1_source_id
        AND s1.segments_id != s2.segments_id
        AND s1.road_source_id = s2.road_source_id
        AND s1.has_addresses AND s2.has_addresses
        AND s1.street = s2.street AND (s1.street_dir == s2.street_dir OR 
                                    (s1.street_dir IS NULL AND s2.street_dir IS NULL))
        AND s2.lnumber = 0;
        """
        
        with segments.database.updater('segments') as upd:
            for row in segments.query(q):
                r = {'_segments_id': row['segments_id'], '_lnumber': row['hnumber1']+1}
                print row['hnumber1'], row['lnumber2'], r
                upd.update(r)
  
  
        # Link segments together by their nodes and look for breaks in the address range. 
        q = """
        select s2.segments_id, s2.road_source_id,
        s1.lnumber AS lnumber1, s1.hnumber AS hnumber1, 
        s2.lnumber AS lnumber2, s2.hnumber AS hnumber2,
        s1.street_dir,  s1.street
        FROM segments as s1, segments as s2
        WHERE s1.node1_source_id = s2.node1_source_id
        AND s1.segments_id != s2.segments_id
        AND s1.road_source_id = s2.road_source_id
        AND s1.has_addresses AND s2.has_addresses
        AND s1.hnumber < s2.hnumber
        AND s2.lnumber = 0;

        """
        
        with segments.database.updater('segments') as upd:
            for row in segments.query(q):
                r = {'_segments_id': row['segments_id'], '_lnumber': row['hnumber1']+1}
                print row['hnumber1'], row['lnumber2'], r
                upd.update(r)
  

        
        segments.database.connection.execute(q)
  
        # For s1 where lnumber is zero, find all of the road segments that have
        # a lower high number. The first of these should be one less than the 
        # missing low number in s1
        q = """
        SELECT *, max(hnumber2)
        FROM (
            SELECT s1.segments_id, s2.road_source_id,
            s2.lnumber AS lnumber2, s2.hnumber AS hnumber2,
            s1.lnumber AS lnumber1, s1.hnumber AS hnumber1, 
            s1.street_dir,  s1.street
            FROM segments as s1, segments as s2
            WHERE s1.road_source_id = s2.road_source_id
            AND s1.lnumber = 0
            AND s2.hnumber != 0
            AND s2.hnumber < s1.hnumber
            ORDER BY s1.road_source_id, s2.hnumber DESC 
        ) 
        GROUP BY road_source_id;
        """
  
        with segments.database.updater('segments') as upd:
            for row in segments.query(q):
                r = {'_segments_id': row['segments_id'], '_lnumber': row['hnumber2']+1}
                print row['hnumber1'], row['lnumber2'], r
                upd.update(r)
  

        q = """
        select s2.segments_id, s2.road_source_id,
        s1.lnumber AS lnumber1, s1.hnumber AS hnumber1, 
        s2.lnumber AS lnumber2, s2.hnumber AS hnumber2
        FROM segments as s1, segments as s2
        WHERE s1.node1_source_id = s2.node2_source_id
        AND s1.segments_id != s2.segments_id
        AND s1.has_addresses AND s2.has_addresses
        AND s1.hnumber < s2.hnumber
        AND s1.lcity = s2.lcity AND s1.rcity = s2.rcity
        AND s2.lnumber = 0;
        """
  
        with segments.database.updater('segments') as upd:
            for row in segments.query(q):
                r = {'_segments_id': row['segments_id'], '_lnumber': row['hnumber1']+1}
                print row['hnumber1'], row['lnumber2'], r
                upd.update(r)
  
  
  
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
     
    
    