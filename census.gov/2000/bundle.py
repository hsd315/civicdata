'''
Created on Jun 10, 2012

@author: eric
'''

from  databundles.bundle import Bundle as Base
import os.path  

class Bundle(Base):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    URLS_FILE = 'urls.yaml'

    def __init__(self,directory=None):
        '''
        Constructor
        '''
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
      
    def process_headers(self):
        '''Process the sf1header.csv file and return a config.Database object that holds
        the schema'''
        
        from databundles.config.database import Database, Table, Column
        import csv
        
        header_file = self.path(self.config['build']['headers'])
        
        reader  = csv.DictReader(open(header_file, 'rbU') )
 
        ds = Database()

        for row in reader:
            # The first two rows for the table give information about the title
            # and population universe. 
            if( not row['FIELDNUM']):
                if  row['TABNO']:
                    # First of the two info rows. 
                    table = Table(name=row['TABNO'].strip('.'),description=row['TEXT'])
                        
                    ds.add_table(table)         
                    
                elif row['TEXT']:
                    #Second and last of the two info rows, so now is time to create the table. 
                    table.universe = row['TEXT']
                  
            else:
                table.add_column(Column(name=row['FIELDNUM'], description=row['TEXT'],
                                         datatype=Column.DATATYPE_INTEGER))
        return ds
        
    def get_manifest(self):
    
        import urllib
        import urlparse
        from bs4 import BeautifulSoup
   
        urls = []
        i = 0;
   
        # Load in a list of states, so we know which links to follow
        with open(self.config['build']['statesFile']) as f:
            states = map(lambda s: s.strip(),f.readlines())
        
        # Root URL for downloading files. 
        url = self.config['build']['rootUrl']
       
        doc = urllib.urlretrieve(url)
        # Get all of the links
        for link in BeautifulSoup(open(doc[0])).find_all('a'):
            
            if not link.get('href') or not link.string or not link.contents:
                continue;# Didn't get a sensible link
            # Only descend into links that name a state
            state = link.get('href').strip('/')
            if link.string and link.contents[0] and state in states :
                stateUrl = urlparse.urljoin(url, link.get('href'))
                
                stateIndex = urllib.urlretrieve(stateUrl)
                # Get all of the zip files in the directory
                for link in  BeautifulSoup(open(stateIndex[0])).find_all('a'):
                    if link.get('href') and  '.zip' in link.get('href'):
                        urls.append({'state': state.encode('ascii', 'ignore'),
                                     'url': urlparse.urljoin(stateUrl, link.get('href')).encode('ascii', 'ignore')
                                     })   
        return urls
        
    def pre_prepare(self):
        return True
    
    def prepare(self):
        '''Create the prototype database'''

        if not self.protodb.exists():

            ds = self.process_headers()
            
            proto = self.protodb
            
            proto.delete()
            proto.create()
            
            proto.add_schema(ds)
            
        urls_file = self.path(self.URLS_FILE)
        
      
        if not os.path.exists(urls_file):
            urls = self.get_manifest();
            
            import yaml
            
            with open(urls_file,'w') as f:
                f.write(yaml.dump(urls))
                
            
            print yaml.dump(urls)

    
    def download(self):
        import download
     
        #o.get_manifest()
        
    
    def transform(self):
        self.super_.transform()
    
    def build(self):
        self.super_.build()
    
    def submit(self):
        self.super_.submit()