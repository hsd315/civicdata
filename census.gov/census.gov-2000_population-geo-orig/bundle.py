'''

@author: eric
'''
from  databundles.bundle import BuildBundle
import os.path
import yaml
 
class Bundle(BuildBundle):
    '''
    Bundle code for US 2000 Census, Summary File 2
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

        self._states = None
        
        bg = self.config.build
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        self.urls_file =  self.filesystem.path(bg.urlsFile)
        
    @property
    def states(self):
        if self._states  is None:
            self._states  = ['ak', 'al', 'ar', 'az', 'ca', 'co', 'ct', 'dc', 'de', 
                         'fl', 'ga', 'hi', 'ia', 'id', 'il', 'in', 'ks', 'ky', 
                         'la', 'ma', 'md', 'me', 'mi', 'mn', 'mo', 'ms', 'mt',
                         'nc', 'nd', 'ne', 'nh', 'nj', 'nm', 'nv', 'ny', 'oh',
                         'ok', 'or', 'pa', 'pr', 'ri', 'sc', 'sd', 'tn', 'tx', 
                         'ut', 'va', 'vt', 'wa', 'wi', 'wv', 'wy']
                 
        return self._states
        
    def prepare(self):
        '''Scrape the URLS into the urls.yaml file and load all of the geo data
        into partitions, without transformation'''
        from databundles.partition import PartitionIdentity
        
        self.scrape_urls()
             
        if not self.database.exists():
            self.database.create()
     
        if len(self.schema.tables) == 0 and len(self.schema.columns) == 0:
            self.log("Loading schema from file")
            with open(self.geoschema_file, 'rbU') as f:
               self.schema.schema_from_file(f)           
        else:
            self.log("Reusing schema")

        for sf in [1,2,3,4]:
            pid = PartitionIdentity(self.identity, table='geofile', grain='sf'+str(sf))
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for summary file"+str(sf))
                partition = self.partitions.new_partition(pid)
        
        return True
 
    def scrape_urls(self):
        
        if os.path.exists(self.urls_file):
            self.log("Urls file already exists. Skipping")
            return 
       
        urls_sets = {}
        rootUrl = self.config.build.rootUrl
        
        for index in [1,2,3,4]:
            url = rootUrl+"_"+str(index)
            urls_sets[index] = self._scrape_urls(url,self.states_file, '_uf'+str(index))
 
        with open(self.urls_file, 'w') as f:
            yaml.dump(urls_sets, f,indent=4, default_flow_style=False)


    def _scrape_urls(self, rootUrl, states_file, suffix='_uf1'):
        '''Extract all of the URLS from the Census website and store them'''
        import urllib
        import urlparse
        import re
        from bs4 import BeautifulSoup
    
        log = self.log
        tick = self.ptick
    
        if not rootUrl.endswith('/'):
            rootUrl = rootUrl + '/' 
    
        # Load in a list of states, so we know which links to follow
        with open(states_file) as f:
            states = map(lambda s: s.strip(),f.readlines())
        
        # Root URL for downloading files. 
       
        doc = urllib.urlretrieve(rootUrl)
        
        log('Getting URLS from '+rootUrl)
        # Get all of the links
        log('S = state, g = geo')
        tables = {}
        geos = {}

        with open(doc[0]) as df:
            for link in BeautifulSoup(df).find_all('a'):

                if not link.get('href') or not link.string or not link.contents:
                    continue;# Didn't get a sensible link
                # Only descend into links that name a state
                state = link.get('href').strip('/')
              
                if link.string and link.contents[0] and state in states :
                    tick('S')
                    stateUrl = urlparse.urljoin(rootUrl, link.get('href'))
                    stateIndex = urllib.urlretrieve(stateUrl)
                    # Get all of the zip files in the directory
                    
                    with open(stateIndex[0]) as f:
                    
                        for link in  BeautifulSoup(f).find_all('a'):
                            if link.get('href') and  '.zip' in link.get('href'):

                                final_url = urlparse.urljoin(stateUrl, link.get('href')).encode('ascii', 'ignore')

                                if 'geo'+suffix in final_url:
                                    tick('g')
                                    state = re.match('.*/(\w{2})geo'+suffix, final_url).group(1)
                                    geos[state] = final_url

            
        return geos

    def build(self):
        from databundles.partition import PartitionIdentity
        import time

        with open(self.urls_file, 'r') as f:
            self.urls_cache =  yaml.load(f) 

        row_i = 0
        t_start = time.time()      
                   
        for sf_index, urls in  self.urls_cache.items():
            
            pid = PartitionIdentity(self.identity, table='geofile', grain='sf'+str(sf_index))
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            if not partition.database.exists():
                partition.create_with_tables('geofile')
            
            for state, url in urls.items():
                
                if state not in self.states:
                    continue
                
                with partition.database.inserter(partition.table) as ins:
                    for row in self.generate_rows(state, url):

                        row_i += 1
                    
                        if row_i % 50000 == 0:
                            # Prints the processing rate in 1,000 records per sec.
                            self.log("SF"+str(sf_index)+" "+state+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ")                 
                        
                        row['name'] = row['name'].decode('latin1') # The Puerto Rico files has 8-bit names
                        row['namehash'] = hash(row['name'])
                        ins.insert(row)
                       
        return True
                    
    def generate_rows(self, state, geo_source):
        '''A generator that yields rows from the state geo files. It will 
        unpack the fixed width file and return a dict'''
        import struct
        import zipfile

        table = self.schema.table('geofile')
        header, unpack_str, length = table.get_fixed_unpack() #@UnusedVariable    

        rows = 0;

        def test_zip_file(f):
            try:
                with zipfile.ZipFile(f) as zf:
                    return zf.testzip() is None
            except zipfile.BadZipfile:
                return False

        geo_zip_file = self.filesystem.download(geo_source, test_zip_file)

        grf = self.filesystem.unzip(geo_zip_file)

        geofile = open(grf, 'rbU', buffering=1*1024*1024)

        for line in geofile.readlines():
            
            rows  += 1
            
            if rows > 20000 and self.run_args.test:
                break

            try:
                geo = struct.unpack(unpack_str, line[:-1])
            except struct.error as e:
                self.error("Struct error for state={}, file={}, line_len={}, row={}, \nline={}"
                           .format(state,grf,len(line),rows, line))
             
            if not geo:
                raise ValueError("Failed to match regex on line: "+line) 

            yield dict(zip(header,geo))

        geofile.close()

    def install(self):  
     
        self.log("Install bundle")  
        dest = self.library.put(self)
        self.log("Installed to {} ".format(dest[2]))
        
        for partition in self.partitions:
            self.log("Install partition {}".format(partition.name))  
            dest = self.library.put(partition)
            self.log("Installed to {} ".format(dest[2]))

        return True
        
import sys

if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
