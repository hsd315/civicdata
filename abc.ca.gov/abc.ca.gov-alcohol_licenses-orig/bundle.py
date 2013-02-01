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
            
        #
        # First, create the schema for the download page cache. 
        #
       
        t = self.schema.add_table('page_cache')
        ac = self.schema.add_column
        ac(t,'id',datatype = Column.DATATYPE_INTEGER, is_primary_key = True)
        ac(t,'date',datatype = Column.DATATYPE_DATE)
        ac(t,'city',datatype = Column.DATATYPE_TEXT)
        ac(t,'license_type',datatype = Column.DATATYPE_INTEGER)
        ac(t,'report_type',datatype = Column.DATATYPE_TEXT)
        ac(t,'page',datatype = Column.DATATYPE_TEXT)
                   
        self.database.session.commit()
        self.schema.create_tables()
            
        pid = PartitionIdentity(self.identity, table='page_cache')
    
        partition = self.partitions.new_partition(pid)
        partition.create_with_tables()     
        
        #
        # Now the schema for the database records
        #
       
        t = self.schema.add_table('licenses')
        ac = self.schema.add_column
        ac(t,'id',datatype = Column.DATATYPE_INTEGER, is_primary_key = True)
        ac(t,'scrape_date',datatype = Column.DATATYPE_DATE)  
        ac(t,'licenseno',datatype = Column.DATATYPE_INTEGER)
        ac(t,'status',datatype = Column.DATATYPE_TEXT)  
        ac(t,'licensetype',datatype = Column.DATATYPE_INTEGER)  
        ac(t,'issuedate',datatype = Column.DATATYPE_DATE)        
        ac(t,'exprdate',datatype = Column.DATATYPE_DATE)  
        ac(t,'owner',datatype = Column.DATATYPE_TEXT) 
        ac(t,'premisesaddress',datatype = Column.DATATYPE_TEXT) 
        ac(t,'tract',datatype = Column.DATATYPE_TEXT)           
        ac(t,'business',datatype = Column.DATATYPE_TEXT)
        ac(t,'mailaddress',datatype = Column.DATATYPE_TEXT)
        ac(t,'geocode',datatype = Column.DATATYPE_INTEGER)                 
                           
        self.database.session.commit()
        self.schema.create_tables()
            
        # One partition per year. 
        pid = PartitionIdentity(self.identity, table='licenses', time=str(datetime.date.today().year))
        partition = self.partitions.new_partition(pid)
        partition.create_with_tables()   
        
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
            
            bf = BeautifulSoup(page)
        
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

    ### Submit the package to the repository
    def submit(self):
        import os
        import databundles.client.ckan
        import time, datetime

        self.repository.submit()
        
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    