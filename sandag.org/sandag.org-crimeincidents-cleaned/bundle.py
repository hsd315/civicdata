'''

'''

from databundles.identity import  DatasetNumber, ColumnNumber
from databundles.identity import TableNumber, PartitionNumber, ObjectNumber

import json

from  databundles.bundle import BuildBundle
 
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)


    def build(self):
        
        self.initial_build()
        self.add_places()
        
        return True

    def initial_build(self):

        import dateutil.parser
        from databundles.geo.geocoder import Geocoder
        from random import choice
        import pprint
        
        g = Geocoder(self.library, addresses_ds='geoaddresses')
                
        log_rate = self.init_log_rate(1000)
     
        p = self.partitions.find_or_new(table='incidents')
     
        _,incidents = self.library.dep('incidents')
    
        p.query("DELETE FROM incidents")
 
        with p.database.inserter() as ins:
            for i, inct in enumerate(incidents.query("SELECT * FROM incidents")):
                row = dict(inct)

                candidates = g.geocode_semiblock(row['blockaddress'], row['city'], 'CA')
                    
                if len(candidates) == 0:
                    continue
                    
                s =  candidates.popitem()[1]
        
                if not len(s):
                    continue
        
                address = choice(s)

                address['datetime'] = dateutil.parser.parse(row['datetime'])
                address['lat'] = address['latc']
                address['lon'] = address['lonc']
                address['x'] = address['xc']
                address['y'] = address['yc']
                address['address'] = "{} {}".format(row['blockaddress'].title(),row['city'].title())
                
                for k,v in address.items():
                    row[k] = v

                log_rate('Load: ')

                row['city'] = row['city'].title()

                ins.insert(row)

        return True


    def seg_incidents(self):
        """Combine crimes onto street segments, computing the count and linear density
        of crime per segment. Creates a geo partition that includes the street segment.  """
        import os

        incidents = self.partitions.find_or_new(table='incidents')

        _, segments = self.library.dep('segments')

        incidents.database.attach(segments, 'seg')

        lr = self.init_log_rate(100, "Incidents:")

        si = self.partitions.find_or_new_geo(table='segincident')

        with si.database.inserter(source_srs=segments.get_srs()) as ins:
            for row in incidents.query("""
            SELECT incidents.type,  incidents.agency, incidents.legend, incidents.description,
            incidents.datetime, incidents.segment_id, incidents.segment_source_id,  
            cast(count(*) as float)/cast(Length(geometry) as float)*1000 as density,
            AsText(geometry) AS geometry
            FROM incidents, {seg}.roads AS seg
            WHERE incidents.segment_source_id = seg.roadsegid
            group by seg.roadsegid
            """):

                lr()
           
                ins.insert(dict(row))


    def add_places(self):      
        from databundles.geo.util import segment_points

        lr = self.init_log_rate(1000)
        
        incidents = self.partitions.find(table='incidents')
        
        _, places = self.library.dep('places')
        
        for area, where, is_in in segment_points(places, 
                                        "SELECT *, AsText(geometry) AS wkt FROM places",
                                        "lon BETWEEN {x1} AND {x2} AND lat BETWEEN {y1} and {y2}"):
            self.log("{} {} {}".format(area['type'], area['name'], where))
            with incidents.database.updater('incidents') as upd:
                for incident in incidents.query("SELECT * FROM incidents WHERE {}".format(where)):
    
                    if is_in(incident['lon'], incident['lat']):
                        lr("Add place: {} {}".format(area['type'], area['name']))
                    
                        u = {'_incidents_id': incident['incidents_id'],
                               '_'+area['type'] : area['code']
                               }
          
                        upd.update(u)

 
    def extract_shapefiles(self, data):
        import pprint
        from databundles.geo.sfschema import TableShapefile
        
        name = data['name']
        fpath = self.filesystem.path('extracts', name)
        
        incidents = self.partitions.find(table='segincident')
        
        tsf = TableShapefile(self, fpath, incidents.table, source_srs = incidents.database.get_srs())
                  
        pprint.pprint(data)
    
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    