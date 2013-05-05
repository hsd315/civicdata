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
        import os
        import pprint
        
        incidents = self.partitions.find_or_new(table='incidents')

        _, segments = self.library.dep('segments')

        incidents.database.attach(segments, 'seg')

        lr = self.init_log_rate(100, "Incidents:")

        si = self.partitions.find_or_new_geo(table='segincident')
        
        if os.path.exists(si.database.path):
            os.remove(si.database.path)
        
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

    def find_area(self):
        """Update the incidents to include the neighborhood and community for San Diego"""
        from databundles.geo.util import segment_points
        
        incidents = self.partitions.find_or_new(table='incidents')
        
        lr = self.init_log_rate(1000)
        
        with incidents.database.updater('incidents') as upd:
            for name, id_, where, is_in in segment_points(self, "communities"):
                for incident in incidents.query("SELECT * FROM incidents WHERE {}".format(where)):
                    if is_in(incident['lon'], incident['lat']):
                        lr("Community "+name)

                        upd.update({'_incidents_id': incident['incidents_id'],
                               '_community_id' : id_,
                               '_community' : name.title()
                               })
        
        with incidents.database.updater('incidents') as upd:
            for name, id_, where, is_in in segment_points(self, "neighborhoods"):
                for incident in incidents.query("SELECT * FROM incidents WHERE {}".format(where)):
                    if is_in(incident['lon'], incident['lat']):
                        lr("Neighborhood "+name)

                        upd.update({'_incidents_id': incident['incidents_id'],
                               '_neighborhood_id' : id_,
                               '_neighborhood' : name.title()
                               })



import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    