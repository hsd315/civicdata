'''

'''


from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

 
    def meta(self):
        from bs4 import BeautifulSoup
        import urllib
        import urlparse
        import yaml
     
        # Get the api page, which, when fetched with GET, returns HTML that has
        # the meta info
        page_url = self.config.meta.meta_url
         
        doc = urllib.urlretrieve(page_url)
        
        cities = []
        licenses = {}
        with open(doc[0]) as df:
            bf = BeautifulSoup(df)
            select = bf.find("select", {"name":"q_CityLOV"})
            for option in select.find_all('option'):
                cities.append(option['value'].strip())
                
            select = bf.find("select", {"name":"q_LTLOV"})
            for option in select.find_all('option'):
                licenses[int(option['value'].strip())] = option.string.strip()
        

        with open(self.filesystem.meta_path('cities.yaml'), 'w+') as f:
            yaml.safe_dump( cities, f,default_flow_style=False, indent=4, encoding='utf-8' )
        
        with open(self.filesystem.meta_path('licenses.yaml'), 'w+') as f:
            yaml.safe_dump( licenses, f,default_flow_style=False, indent=4, encoding='utf-8' )
        
        return True

    ### Prepare is run before building, part of the devel process.  
    def prepare(self):
        
        from databundles.orm import Column
        from databundles.partition import PartitionIdentity
        import datetime
        
        if not self.database.exists():
            self.database.create()  
    
        
        self.log("Loading schema from file")
        with open(self.config.build.schema_file, 'rbU') as f:
            self.schema.schema_from_file(f)        
 
        self.database.session.commit()
         
        pid = PartitionIdentity(self.identity, table='page_cache')
        partition = self.partitions.new_partition(pid)
        partition.create()  
           
        # One partition per year. 
        pid = PartitionIdentity(self.identity, table='licenses', time=str(datetime.date.today().year))
        partition = self.partitions.new_partition(pid)
        partition.create()      
        
        return True 
                    
  
    def download_page(self, city_name, report_type=None):
        """Cache a page, or return a cached version if it is less than a month old. """
        
        import datetime
        import requests
        from databundles.partition import PartitionIdentity
        from sqlalchemy.sql import text
        import datetime
        
        pid = PartitionIdentity(self.identity, table='page_cache')
        partition = self.partitions.find(pid)
        
        #
        # Look in the cache for the page. 
        #
       
        s = text(""" SELECT date, page FROM page_cache WHERE city = :city_name 
                AND report_type = :report_type
                AND date > date('now','-1 month')
                ORDER BY date desc
                """)
       
        row = partition.database.connection.execute(s,
                                                    city_name = city_name, 
                                                    report_type = report_type
                                                    ).first()
        
      
        if row:
            return 'cached',row[1]
        
        #
        # Not found, get it from the source. 
        #
        
        url = self.config.build.api_url

        
        payload = {
            'q_CityLOV': city_name,
            'RPTYPE': report_type,
            'SUBMIT1' : 'Continue'
        }

        r  = requests.post(url, data=payload)
        
       
        with partition.database.inserter(partition.table) as ins: 
            row = {
                'date':datetime.datetime.now(),
                'city':city_name,
                'license_type': '01',
                'report_type': report_type,
                'page':r.text
            }       
            ins.insert(row)
        

        return "new",r.text

    ### Build the final package
    def build(self):
        import urllib
        import datetime
        import requests
        from bs4 import BeautifulSoup
        from databundles.partition import PartitionIdentity
        import yaml
        import time
        
        
        pid = PartitionIdentity(self.identity, table='licenses', time=str(datetime.date.today().year))
        partition = self.partitions.find(pid)
                                         
        target_cities = self.config.meta.target_cities
        
        for city in target_cities:
            self.log("Downloading: {} ".format(city))
            cache_state, page = self.download_page(city, report_type='p_Retail')
            if cache_state == 'new':
                time.sleep(5)
            else:
                self.log("Page was cached")
            
            bf = BeautifulSoup(page,'lxml')
        
            table = []
            for html_row in bf.find("table").find_all('tr', {"class":"report_column"}):
                row = []
                
                for i, cell in enumerate(html_row.find_all('td')):
                    
                    if i == 6:
                        strings = [ s.strip() for s in cell.strings]
                        if len(strings) > 0:
                            owner = strings.pop(0)
                            tract = strings.pop().split(' ')[-1]
                            address = ', '.join(map(lambda x: x.strip(), strings))

                        row.append(owner)
                        row.append(address)
                        row.append(tract)
                    else:
                        v = ' '.join(cell.strings).strip()
                        row.append(v)
  
                
                if row:
                    del row[0] # ordinal number
                    row[3] = datetime.datetime.strptime(row[3],'%m-%d-%Y').date()
                    row[4] = datetime.datetime.strptime(row[4],'%m-%d-%Y').date()
                    
                    ins_row = [None, datetime.date.today()] + row
                    table.append(ins_row)
                    
            with partition.database.inserter(partition.table, cache_size=0) as ins: 
                for row in table:
                    ins.insert(row)
            
        return True

    def geocode(self):
        """Geocode the premises and mailing addresses"""
        import databundles.orm as orm
        from databundles.identity import PartitionIdentity
        from geopy import geocoders  
        import datetime, re
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy import Table
        
        Base = declarative_base()

        key = self.config.group('accounts').yahoo.key

        pid = PartitionIdentity(self.identity, table='licenses', time=str(datetime.date.today().year))
        partition = self.partitions.find(pid)

        class Licenses(Base):
            __table__ = Table('licenses', Base.metadata,
                            autoload=True, autoload_with=partition.database.engine)
     
     
        session = partition.database.session
        y = geocoders.Yahoo(key)  
        for i, row in enumerate(session.query(Licenses)):
            
            #place, (lat, lng) = y.geocode(row.premisesaddress)  
            print '-----'
            print '  ',  row.premisesaddress
            print '  ',  row.geocode
            
            
            #print '  ',  place
            #print '  ',  lat, lng
            
            if i > 5:
                break;


        
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    