'''
'''

from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    def build(self):
        import petl
        import petl.fluent as petlf
        
        p = self.partitions.find_or_new(table='businesses')
        
        dp = petl.dateparser('%m/%d/%Y')

        for name, url in self.sources.items():
            
            self.log("Converting: {}".format(url))
            
            df = self.filesystem.download(url)
            
            t = ( petlf.fromcsv(df)
                .addfield('businesses_id',None, index=0)
                .addfield('zip5',lambda r: r['ZIP'][0:5] if len(r['ZIP']) >=5 else None, index=7)
                .addfield('zip4',lambda r: r['ZIP'][-4:] if len(r['ZIP']) == 10 else None, index=8)
                .setheader([c.name for c in p.table.columns])
                .convert(('naics','acct_no'), int)
                .convert(('created', 'started','expires'), dp)
                .convert(('dba', 'address','city','owner','desc'), str.title)
                .convert(('incorp_type'), str.lower)
                .convert([c.name for c in p.table.columns if c.datatype =='text'], unicode)
            )
         
            t.appendsqlite3(p.database.path, p.table.name)
            
        return True
     
    def test_geo(self):
        from databundles.geo.geocoder import Geocoder
        
        g = Geocoder(self.library, addresses_ds='geoaddresses')
        
        p = self.partitions.find(table='businesses')
        
        errorp = self.partitions.find_or_new(table='businesses', grain='errors')
        
        ok = 0
        errors = 0
        with errorp.database.inserter() as ins:
            for row in p.query('SELECT * FROM businesses'):
                candidates = g.geocode_address(row['address'], row['city'], 'CA')
        
                if len(candidates) != 1 :
                    #print "('{0}', (None, '{0}','gln')),".format(row['address'])
                    errors += 1
                else:
                    ok += 1
                
                print len(candidates), ok, errors, int(float(ok)/(ok+errors) * 100)
                ins.insert(row)
        
     
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)  
    