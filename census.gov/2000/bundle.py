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

    def __init__(self,directory=None):
        '''Constructor'''
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
      
    def schemaGenerator(self):
        '''Return schema rows'''
    
        from databundles.orm import Table, Column
        import csv
        
        header_file = self.filesystem.path(self.config.group('build').get('headers'))
        reader  = csv.DictReader(open(header_file, 'rbU') )
      
        i = 0
        for row in reader:
            if not row['TABLE']:
                continue
      
            
            # The first two rows for the table give information about the title
            # and population universe. 
            if( not row['FIELDNUM']):
                if  row['TABNO']:
                    # First of the two info rows. 
                    last_table = row['TABLE']
                    yield Table(name=row['TABLE'].strip(),description=row['TEXT'])
              
                else:
                    last_table = row['TABLE']
                    #Second and last of the two info rows, so now is time to create the table. 
                    yield Table(name=row['TABLE'].strip(),
                                 universe=row['TEXT'].replace('Universe:','').strip()
                                 )               
            else:
                
                # The whole table will exist in one segment ( file number ) 
                # but the segment id is not included on the same lines ast the
                # table name. 
                if last_table:
                     yield Table(name=last_table.strip(),data={'segment':row['SEG']})
                     last_table = False
                         
                yield Column(name=row['FIELDNUM'],
                             table_name=row['TABLE'],
                             description=row['TEXT'].strip(),
                             datatype=Column.DATATYPE_INTEGER,
                             data={'segment':int(row['SEG'])}
                             )
 
    def table_seg_map(self):
        
        from databundles.orm import Table
        
        # Create a lookup table mapping segments to tables. 
        #  There are multiple tables per file. This query is
        # a horrible, encapsulation busting hack, and I am ashamed of it. 
        s = self.database.session
        table_seg = {}
        for table in s.query(Table).all() :
            if not table.data.get('segment',None):
                raise ValueError("Should have gotten a segment data for table")
            
            seg = int(table.data['segment'])
            
            if not  seg in table_seg:
                table_seg[seg] = []
                
            table_seg[seg].append(table.name.encode('ascii'))
            
       
        return table_seg
   
    def partitionGenerator(self):
        '''Get all of the URLs of the zip files to download '''
        import urllib
        import urlparse
        import yaml
        import re
        from bs4 import BeautifulSoup
        from databundles.partition import Partitions
    
   
        downloadDir = 'downloads'
   
        i = 0;
   
        states_file = self.filesystem.path(self.config.group('build').get('statesFile'))
   
        # Load in a list of states, so we know which links to follow
        with open(states_file) as f:
            states = map(lambda s: s.strip(),f.readlines())
        
        # Root URL for downloading files. 
        url = self.config.group('build').get('rootUrl')
       
        doc = urllib.urlretrieve(url)
        
        self.log('Getting URLS from '+url)
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
                        final_url = urlparse.urljoin(stateUrl, link.get('href')).encode('ascii', 'ignore')

                        path = urlparse.urlparse(final_url).path
                      
                        data={'source_url':final_url,
                              'zip_file':os.path.join(downloadDir+'/'+os.path.basename(path))}
                        
                        if 'geo_uf1' in final_url:
                            state = re.match('.*/(\w{2})geo_uf1', final_url).group(1)
                          
                            yield self.partitions.new_parition(
                                        space=state,table='SF1GEO',data=data)
                            
                        else:
                            m = re.match('.*/(\w{2})(\d{5})_uf1', final_url)
                            state,segment = m.groups()
                            segment = int(segment.lstrip('0'))

                            # Create a table partition for every table in the segment. 
                            for table in self.table_seg_map().get(segment):
                             
                                yield self.partitions.new_parition(
                                        space=state,table=table,data=data)
 
     
    def pre_prepare(self):
        return True
    
    def prepare(self):
        '''Create the prototype database'''
       

        header_file = self.config.group('build').get('headers')
       
        self.config.add_file(header_file)

        self.log(str(len(self.schema.tables))+' tables in database')
        self.log(str(len(self.schema.columns))+' columns in database')
        
        if (len(self.schema.tables) != 286 or 
            len(self.schema.columns) != 8113 or
            self.filesystem.ref(header_file).changed):
            self.log("Regenerating schema")
            self.schema.generate()
        else:
           self.log("Re-using schema")
      
        
        # Create the manifast file if it does not exist. 
        ##urls = self.get_manifest()  
        
        self.partitions.generate()
        
        return true

    from contextlib import contextmanager
    
    @contextmanager
    def extract_zip(self,path):
        '''Extract a the files from a zip archive'''
        
        extractDir = self.filesystem.directory('extracts')

        with zipfile.ZipFile(path) as zf:
            for name in  zf.namelist():
                extractFilename = os.path.join(extractDir,name)
                
                if(os.path.exists(extractFilename)):
                    s.unlink(extractFilename)
                    
                print 'Extracting'+extractFilename+' from '+path
                name = name.replace('/','').replace('..','')
                zf.extract(name,extractDir )
                    
                yield extractFilename
                os.unlink(extractFilename)
                        

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
        
        

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
    
    