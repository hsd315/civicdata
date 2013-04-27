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
        
        from datetime import date
        from databundles.identity import PartitionIdentity
        import re
        
        #today = date.today().strftime('%Y%m%d')
        today = self.config.build.source_file_date
    
        zf = self.filesystem.download(self.config.build.sources.lights.format(date=today))

        sf = self.filesystem.unzip(zf, re.compile('^.*\.kml$'))

        pid = PartitionIdentity(self.identity, table='lights_g')
           
        try: shape_partition = self.partitions.find(pid)
        except: shape_partition = None # Fails with ValueError because table does not exist. 
        
        if not shape_partition:
            shape_partition = self.partitions.new_geo_partition( pid, sf)

        self.convert_partition()
        
        return True

    def convert_partition(self):
        
        from lxml import etree
        from StringIO import StringIO
        from pprint import pprint
        import re
        
        p = self.partitions.find(table='lights_g')   
        
        lr = self.init_log_rate(message='Convert lights partition: ')
        
        dest_p = self.partitions.find_or_new(table='lights')
           
        dest_p.database.connection.execute("DELETE FROM lights")
           
        with dest_p.database.inserter() as ins:
            for row in p.query("""
                SELECT 
                    description, 
                    X(Transform(geometry, 4326)) AS lon, 
                    Y(Transform(geometry, 4326)) AS lat 
                FROM lights_g"""):

                    lr()
                    
                    r = etree.HTML(row['description'])
    
                    table = r.find('.//table').find('.//table')
    
                    trs = iter(table)
    
                    d = {}
                    for tr in trs:
                        values = [col.text for col in tr]
                        d[values[0].lower().replace(' ','_')] = values[1]
                        
                
                    if ( 'not part' in d['conversion_status'] ):
                        status = "not in project"
                    elif  'not been converted' in d['conversion_status'] :
                        status = 'not converted'
                    elif  'has been converted' in d['conversion_status']:
                        status = 'converted'
                    elif 'No Light Here' in d['conversion_status']:
                        status = 'no light'
                    else:
                        status = 'other'
                        
                    try: old_wattage = None if not d['existing_field_wattage'] else re.match('(\d+)', d['existing_field_wattage']).group(1)
                    except: old_wattage = None
                    
                    try: new_wattage = None if not d['new_wattage'] else re.match('(\d+)', d['new_wattage']).group(1)
                    except: new_wattage = None
                    
                    new_type = d['new_type']
                    
                    if d['existing_field_type'] and 'High' in d['existing_field_type']:
                        old_type = 'HPS'
                    elif d['existing_field_type'] and 'Low' in d['existing_field_type']:
                        old_type = 'LPS'
                    else:
                        old_type = None
                    
                    if d['new_type'] and "IND" in d['new_type']:
                        new_tpye = 'IND'
                    else:
                        new_type = None
                    
                    r = {
                         'status':status, 
                         'old_wattage':old_wattage, 
                         'new_wattage':new_wattage, 
                         'old_type':old_type,
                         'new_type':new_type, 
                         'lat':row['lat'],
                         'lon':row['lon']
                        }
                    
                    ins.insert(r)

    def extract_image(self, data):
        
        import databundles.geo as dg
        from databundles.geo.analysisarea import get_analysis_area
        from osgeo.gdalconst import GDT_Float32
        
        aa = get_analysis_area(self.library, geoid='CG0666000')
        trans = aa.get_translator()

        a_old = aa.new_array()
        a_new = aa.new_masked_array()
        a_nip = aa.new_array()
        a_total = aa.new_masked_array()
        
        k = dg.GaussianKernel(21,7)
        
        p = self.partitions.find(table='lights')
        
        for row in p.query("""SELECT * FROM lights """):
       
            p = trans(row['lon'], row['lat'])
       
            status = row['status']
       
            if status == 'not in project':
                k.apply_add(a_nip, p)  
            elif status == 'converted':
                k.apply_add(a_new, p)
                k.apply_add(a_total, p) 
            elif status == 'not converted':
                k.apply_add(a_old, p) 
                k.apply_add(a_total, p)  
  
            
        def fnp(prefix):
             return self.filesystem.path('extracts','{}-{}'.format(prefix,data['name']))

        aa.write_geotiff(fnp('old'), a_old, data_type=GDT_Float32)
        aa.write_geotiff(fnp('new'), a_new, data_type=GDT_Float32)
        aa.write_geotiff(fnp('nip'), a_nip, data_type=GDT_Float32)
        aa.write_geotiff(fnp('total'), a_total, data_type=GDT_Float32)
        
        pct = 1 - (a_new / a_total)
        
        aa.write_geotiff(fnp('pct'), pct, data_type=GDT_Float32)
        
        
        return fnp('total')
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    