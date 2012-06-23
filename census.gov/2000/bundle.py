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
        '''Constructor'''
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
      
    def schemaGenerator(self):
        '''Return schema rows'''
        
        from databundles.config.orm import Table, Column
        import csv
        
        header_file = self.path(self.config['build']['headers'])
        
        reader  = csv.DictReader(open(header_file, 'rbU') )
      
        for row in reader:
            # The first two rows for the table give information about the title
            # and population universe. 
            if( not row['FIELDNUM']):
                if  row['TABNO']:
                    # First of the two info rows. 
                    table = Table(name=row['TABNO'].strip('.'),description=row['TEXT'])
                  
                    
                elif row['TEXT']:
                    #Second and last of the two info rows, so now is time to create the table. 
                    table.universe = row['TEXT']
                  
            else:
                table.add_column(Column(name=row['FIELDNUM'], description=row['TEXT'],
                                         datatype=Column.DATATYPE_INTEGER))
        return ds
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
        '''Get all of the URLs of the zip files to download '''
        import urllib
        import urlparse
        import yaml
        from bs4 import BeautifulSoup
   
        urls_file = self.path(self.URLS_FILE)
        
        # If the file has already been generated, return it. 
        if os.path.exists(urls_file):
            return  yaml.load(file(urls_file, 'r'))
   
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
        import yaml
            
        with open(urls_file,'w') as f:
            f.write(yaml.dump(urls))
            
        return urls
         
        
    def pre_prepare(self):
        return True
    
    def prepare(self):
        '''Create the prototype database'''

        if not self.protodb.exists():
            print "Creating prototype database"
            
            ds = self.process_headers()
            
            proto = self.protodb
            
            proto.delete()
            proto.create()
            proto.load_sql(self.path('meta/tables.sql'))
            #proto.add_schema(ds)
            
        # Create the manifast file if it does not exist. 
        urls = self.get_manifest()  
      
    
    def download(self):
        import download
        import urllib
        import urlparse
        import zipfile
     
        urls = self.get_manifest()  
     
        extractDir = self.directory('extracts')
        downloadDir = self.directory('downloads')
     
        for e in urls:
            state = e['state']
            aurl = e['url']
                    
            path = urlparse.urlparse(aurl).path
            webFilename = os.path.normpath(downloadDir+'/'+os.path.basename(path))
        
            if(not os.path.exists(webFilename)):
                print 'Downloading '+aurl+' to '+webFilename
                webFilename, headers = urllib.urlretrieve(aurl, webFilename )
            else:
                print 'Using cached web file '+webFilename
          
            try :
                with zipfile.ZipFile(webFilename) as zf:
                    for name in  zf.namelist():
                        extractFilename = os.path.join(extractDir,name)
                        if(not os.path.exists(extractFilename)):
                            print 'Extracting'+extractFilename+' from '+webFilename
                            name = name.replace('/','').replace('..','')
                            zf.extract(name,extractDir )
                        else:
                            print 'Using cached extract file '+extractFilename
            except zipfile.BadZipfile:
                os.unlink(webFilename)
                print "ERROR: Not a zipfile: "+webFilename
    
    def pre_transform(self):
        return True
    
    def transform(self):
        import re
        
        extractDir = self.directory('extracts')
        
        dir_listing = [os.path.join(extractDir, f) for f in os.listdir(extractDir)]
        
        geo_regex = self.get_geo_regex()
        
       
        for path in dir_listing:
            if 'geo.uf1' in path:
                self.load_geo(path, geo_regex)
                pass
            elif re.match('.*/\w{2}\d{5}.uf1', path):
                #self.load_table(path)
                pass
            else:
                raise ValueError, 'Bad path: '+path
   
   
    def load_table(self, path):
        '''Copy the prototype db into a production table and load data into it.''' 
        import re, pprint, copy, os
        
        state, number = re.match('.*/(\w{2})(\d{5}).uf1', path).groups()
        number = int(number)
        table = 'SF100{:02d}'.format(number)
        
        # Copy the bundle so we change the partition, to create the correct partition name
        bundle = copy.deepcopy(self)
        bundle.partition.space = state
        
        #bundle.partition.table = table
        
        pdb = bundle.productiondb_path

        # Loading a CSV file into sqlite is really easy, and this is probably much faster. 
        cmd = "/usr/bin/sqlite3 -csv {} '.import {} {}' ".format(pdb, path, table)
        print cmd
        
        os.system(cmd)
        
    def get_geo_regex(self):
        '''Read the definition for the fixed positioins of the fields in the geo file and
        construct a regular expresstion to parse the lines.'''
        import csv, re
        
        def_file = self.path('meta/geoheaders.csv')
        reader  = csv.DictReader(open(def_file, 'rbU') )
        
        pos = 0;
        regex = ''
        for row in reader:
            start = int(row['start']) - 1
            pos += int(row['length'])
        
            regex += "(?P<{}>.{{{}}})".format(row['column'],row['length'])
            
        print regex
       
        return re.compile(regex)
            
    def load_geo(self, path, regex):
        import re, pprint, copy
        
        state = re.match('.*/(\w{2})geo.uf1', path).group(1)
     
        # Copy the bundle so we change the partition, to create the correct partition name
        bundle = copy.deepcopy(self)
        bundle.partition.space = state
    
   
        d = bundle.database()
        con = d.engine.connect()
        
        
        with open(path) as f:
            con.begin()
            print "Loading geo file "+ path
            for line in f:
                m = regex.match(line)
                if m:
                    values = {k:v.strip() for k,v in m.groupdict().iteritems()}
                    ins = d.table('SF1GEO').insert().values(values)
                    con.execute(ins)
                    
                else:
                    con.rollback()
                    raise ValueError
            con.commit()
        
    def post_transform(self):
        return True
     
    def build(self):
        self.super_.build()
    
    def submit(self):
        self.super_.submit()