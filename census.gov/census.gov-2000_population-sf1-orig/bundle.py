'''
Bundle build file for 2000 US Census, Summary file 1

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
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        self._source_to_partition = None

    def prepare(self):
        '''Create the prototype database'''
        from  databundles.database import Database

        self.log("Scape files ")
        self.scrape_files()
     
        self.log("Make the segment map")
        self.make_segment_map()
     
        header_file = self.config.group('build').get('headers')
       
        self.config.add_file(header_file)

        self.log("Before: "+str(len(self.schema.tables))+' tables in database')
        self.log("Before: "+str(len(self.schema.columns))+' columns in database')
         
        if (len(self.schema.tables) < 287 or 
            len(self.schema.columns) < 9623 or
            self.filesystem.ref(header_file).changed):
            self.log("Regenerating schema. This could be slow ... ")
            self.schema.generate()
        else:
           self.log("Re-using schema")

        self.log("After : "+str(len(self.schema.tables))+' tables in database')
        self.log("After : "+str(len(self.schema.columns))+' columns in database')
      
        # Create the manifast file if it does not exist. 
        ##urls = self.get_manifest()  
    
        if False:
            if self.partitions.count < 50:
                self.partitions.generate()
            else:
                self.log("Skipping building partitions, {} partitions "
                         .format(self.partitions.count))
                
            self.log('After '+str(self.partitions.count)+' partitions in database')
     
        return True
       
    def schemaGenerator(self):
        '''Return schema rows from the  columns.csv file'''
        from databundles.orm import Table, Column
        import csv
    
        self.log("Create GEO schema")
        yield Table(name='sf1geo',description='Geo header')
        header_file = self.filesystem.path(self.config.group('build').get('geoheaderFile'))
        reader  = csv.DictReader(open(header_file, 'rbU') )
        types = {'TEXT':Column.DATATYPE_TEXT,
                 'INTEGER':Column.DATATYPE_REAL}
        for row in reader: 
            yield Column(name=row['column'],datatype=types[row['datatype'].strip()])
    
        self.log("Generating main table schemas")
        self.log("T = Table, C = Column, 5 = Five geo columns")

        
        header_file = self.filesystem.path(self.config.group('build').get('headers'))
        reader  = csv.DictReader(open(header_file, 'rbU') )
      
        last_seg = None
        source_col = 5 # Offset for  common columns
        i = 0
        for row in reader:
            if not row['TABLE']:
                continue
      
            # Keep track of the column number for each segment file, resetting it when
            # the seg value changes. 
            if row['SEG'] and row['SEG'] != last_seg:
                    source_col = 5
                    last_seg = row['SEG']
      
            # The first two rows for the table give information about the title
            # and population universe, but don't have any column info. 
            if( not row['FIELDNUM']):
                if  row['TABNO']:
                    table = Table(name=row['TABLE'],description=row['TEXT'])
                else:
                    table.universe = row['TEXT'].replace('Universe:','').strip()  
            else:
                
                # The whole table will exist in one segment ( file number ) 
                # but the segment id is not included on the same lines ast the
                # table name. 
                if table:
                    table.data = {'segment':row['SEG']}
                    self.ptick("T")
                    yield table
                     
                    # First 5 fields for every record      
                    # FILEID           Text (6),  uSF1, USF2, etc. 
                    # STUSAB           Text (2),  state/U.S. abbreviation
                    # CHARITER         Text (3),  characteristic iteration, a code for race / ethic group
                    #                             Prob only applies to SF2. 
                    # CIFSN            Text (2),  characteristic iteration file sequence number
                    #                             The number of the segment file             
                    # LOGRECNO         Text (7),  Logical Record Number
              
                    tn = row['TABLE']
                    dt = Column.DATATYPE_INTEGER
                    seg = row['SEG']
                    
                    self.ptick("5")
                    yield Column(name='FILEID',table_name=tn,datatype=dt,
                                 data={'source_col':0,'segment':seg})
                    yield Column(name='STUSAB',table_name=tn,datatype=dt,
                                 data={'source_col':1,'segment':seg})
                    yield Column(name='CHARITER',table_name=tn,datatype=dt,
                                 data={'source_col':2,'segment':seg})
                    yield Column(name='CIFSN',table_name=tn,datatype=dt,
                                 data={'source_col':3,'segment':seg})
                    yield Column(name='LOGRECNO',table_name=tn,datatype=dt,
                                 data={'source_col':4,'segment':seg})
                    
                    table = None

                if row['DECIMAL'] and int(row['DECIMAL']) > 0:
                    dt = Column.DATATYPE_REAL
                else:
                    dt = Column.DATATYPE_INTEGER
                
                self.ptick("C")
                yield Column(name=row['FIELDNUM'],table_name=row['TABLE'],
                             description=row['TEXT'].strip(),
                              datatype=dt,data={'segment':int(row['SEG']),
                                                'source_col':source_col}   )
                
                source_col += 1
   
    def scrape_files(self):
        '''Extract all of the URLS from the Census website and store them'''
        import urllib
        import urlparse
        import yaml
        import re
        from bs4 import BeautifulSoup
        from databundles.partition import Partitions, Partition, PartitionId
        import sys

        i = 0;
   
        states_file = self.filesystem.path(self.config.group('build').get('statesFile'))
        urls_file =  self.filesystem.path(self.config.group('build').get('urlsFile'))
        
        if os.path.exists(urls_file):
            self.log("Urls file already exists. Skipping")
            return        
   
        # Load in a list of states, so we know which links to follow
        with open(states_file) as f:
            states = map(lambda s: s.strip(),f.readlines())
        
        # Root URL for downloading files. 
        url = self.config.group('build').get('rootUrl')
       
        doc = urllib.urlretrieve(url)
        
        self.log('Getting URLS from '+url)
        # Get all of the links
    
        tables = {}
        geos = {}
        for link in BeautifulSoup(open(doc[0])).find_all('a'):
            self.ptick('S')
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
                
                        if 'geo_uf1' in final_url:
                            self.ptick('g')
                            state = re.match('.*/(\w{2})geo_uf1', final_url).group(1)
                            geos[state] = final_url
                        else:
                            self.ptick('T')
                            m = re.match('.*/(\w{2})(\d{5})_uf1', final_url)
                            state,segment = m.groups()
                            segment = int(segment.lstrip('0'))
                            if not state in tables:
                                tables[state] = {}
                                
                            tables[state][segment] = final_url

        yaml.dump({'tables':tables,'geos':geos}, 
                  file(urls_file, 'w'),indent=4, default_flow_style=False)

   
    def make_segment_map(self):
        
        import csv
        import yaml
        
        headers_file =  self.filesystem.path(self.config.group('build').get('headers'))
        seg_map_path = self.filesystem.path(self.config.group('build').get('segMap'))
   
        if os.path.exists(seg_map_path):
            self.log("Re-using segment map")
            return;
   
        self.log("Making segment map")
   
        map = {}
        for row in csv.DictReader(open(headers_file, 'rbU') ):
            if row['SEG'] and row['TABLE']:
                seg = int(row['SEG'])
                table = row['TABLE'].strip().lower()
                
                if not seg in map:
                    map[seg] = []
                    
                # Want YAML to serialize a list, not a set. 
                if table not in map[seg]:
                    map[seg].append(table)
                
        yaml.dump(map, 
                  file(seg_map_path, 'w'),indent=4, default_flow_style=False)  
   
    def partitionGenerator(self):
        '''Get all of the URLs of the zip files to download and create partitions
        
        This method will walk the census website to get references to all of 
        the zip files and download them. Then it creates partition records 
        that reference the zip files. There are three types of partitions: 
          
        geo: for the state geo files. 
        build: partitions with space and table elements, for creating .csv files
        deploy: partitions with only the table elements, for generating databases
        
        '''
        import urllib
        import urlparse
        import yaml
        import re
        from bs4 import BeautifulSoup
        from databundles.partition import Partitions, Partition, PartitionId
        import sys

        self.log("P = Whole table partition, p =  table/state partition, G = geo partition ")

        downloadDir = 'downloads'
   
        i = 0;
   
        urls_file =  self.filesystem.path(self.config.group('build').get('urlsFile'))
        
        urls = yaml.load(file(urls_file, 'r'))
    
        
        # Construct the main table partitions. 
        partitions = {}
        
        # Get just the first state; they all have the same partitions. 
        for state,segments in urls['tables'].iteritems():
            for segment, final_url in segments.iteritems():
               
                for table in self.table_seg_map().get(segment):
                    path = urlparse.urlparse(final_url).path
                    zip_file = os.path.join(downloadDir,os.path.basename(path))
                    data = {'source_url':final_url,'zip_file':zip_file}
        
                    p_id = PartitionId(table=table)
            
                    if str(p_id) not in partitions:
                        partitions[str(p_id)] = True
                        self.ptick('P')
                        yield  Partition(self,p_id,data = data)
           
        # Construct the GEO partitions         
        for state, final_url in urls['geos'].items():
            path = urlparse.urlparse(final_url).path
            zip_file = os.path.join(downloadDir,os.path.basename(path))
            data = {'source_url':final_url,'zip_file':zip_file}
            self.ptick('G')
            yield Partition(self, PartitionId(table='SF1GEO', space=state),
                            data=data)
             
    def table_seg_map(self):
        """Create a lookup table mapping segments to tables. 
           There are multiple tables per file."""
        from databundles.orm import Table
        
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
 
 
    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        
        import yaml 
        import pp
        
        self.partitions.delete_all()
      
        urls_file =  self.filesystem.path(self.config.group('build').get('urlsFile'))
        urls = yaml.load(file(urls_file, 'r'))
           
        segmap_file =  self.filesystem.path(self.config.group('build').get('segMap'))
        segmap = yaml.load(file(segmap_file, 'r'))         
     
        # Setup the parallelization
        from multiprocessing import Pool
        p = Pool(1)
            
        for state,segments in urls['tables'].items():
            for seg_number,source in segments.items():
                self.load_table(seg_number,source, segmap)
                #result = p.apply(load_table ,(seg_number,source, segmap,))
                
        
         
     
        for state, source in urls['geos'].items():
            pass
            self.load_geo(state, source)
            #self.log("Finished geo file {} of {} ".format(i, len(urls['geos'])))
          

        return True
   

    def load_table(self,seg_number,source, segmap):
        '''For a set of partitions and a path to a zip file, break the
        data in the zip file into seperate partition files. ''' 
        import re, pprint, copy, os, csv, pprint
        from sqlalchemy.schema import CreateTable
        from databundles.partition import PartitionId
        import csv
        import petl
                
        self.log("#### "+source)
        
        if self.config.get_url(source):
            self.log("Already processed, skipping: "+source)
            return
         
        state, number = re.match('.*/(\w{2})(\d{5}).uf1', source).groups()
        number = int(number)
         
        self.log("Loading tables from {}, segment {}, state {}".format(source,seg_number, state))
        
        if seg_number != number:
            raise Exception("Number mismatch: {} != {} ".format(seg_number, number))
     
        range_map = {}
       
        for table_name in segmap[seg_number]:
            pid = PartitionId(table=table_name,space=state)
            
            # Will not re-create the partition if it already exists. 
            partition = self.partitions.new_partition(pid)
   
            partition.database.delete()
            partition.database.create()
            partition.database.create_table(table_name)
        
            table = self.schema.table(table_name)
        
            start =   None
            for column in table.columns:         
                if table.id_ not in range_map:
                    range_map[table.id_] = []
                  
                if column.data['source_col'] >= 5 and start is None:
                    start = column.data['source_col']
                             
            range_map[table.id_] = {'start':start,  
                                    'source_col':column.data['source_col']+1, 
                                    'table' : table.name,
                                    'partition':partition }

            
        import time
        start_time = time.clock()
        count = 0
        
        # Now, for each row, we can use the range map to take slices of columns
        # and write those to csv file partitions. 
        rows = 0;
        data = {}
        with self.filesystem.download(source) as zip_file:
            with self.extract_zip(zip_file) as rf:
                for row in csv.reader(open(rf, 'rbU') ):
                   
                    # Pull off the common fields. 
                    common = row[:5]
            
                    for table_id, range in range_map.iteritems():
                        partition_row = common + row[range['start']:range['source_col']]
                        if not table_id in data:
                            data[table_id] = []
                            
                        data[table_id].append(partition_row)
                        #range[2].insert(partition_row)
                        
                    rows += 1
                    
                    if rows % 10000 == 0:
                        elapsed = time.clock() - start_time
                        self.log("{} rows in {} sec: {:.2f} rows per sec "
                                 .format(rows, elapsed, rows/elapsed)
                                 )
                        
        self.log("Done compiling. Writing files")
        for table_id, range in range_map.iteritems():
            
            rows = data[table_id]
            table_name = range['table']
            db_path = range['partition'].database.path
            self.log("Write {} rows to {}, table {}".format(len(rows),db_path, table_name))
            petl.appendsqlite3(petl.progress(rows, 200000),db_path, table_name)           
                
            self.library.put(range['partition'])
            range['partition'].database.delete()
            
        # WIll create if does not exist. 
        self.config.get_or_new_url(source)
                
        return True

  
    def get_geo_regex(self):
        '''Read the definition for the fixed positioins of the fields in the geo file and
        construct a regular expresstion to parse the lines.'''
        import csv, re
                
        def_file = self.filesystem.path(self.config.group('build').get('geoheaderFile'))

        reader  = csv.DictReader(open(def_file, 'rbU') )
        pos = 0;
        regex = ''
        header = []
        for row in reader:
            start = int(row['start']) - 1
            pos += int(row['length'])
        
            regex += "(.{{{}}})".format(row['length'])
            header.append(row['column'])
       
        return header, re.compile(regex)
            
    def load_geo(self, state, source):
        from databundles.partition import PartitionId
        import re, pprint, copy
    
        import petl.fluent as petl
        header, regex = self.get_geo_regex()
  
        retry = 4
        while retry > 0:
            retry -= 1
            #try:  
            with self.filesystem.download(source) as zip_file:
                with self.extract_zip(zip_file) as rf:
                    self.log("Processing GEO file: "+rf)
 
                    # Create the partition
                    partition = self.partitions.new_partition(
                                    PartitionId(table='sf1geo',space=state))
 
                    partition.database.load_sql( self.filesystem.path('meta/sf1geo.sql'))
 
 
                    # Load the data
                              
                    db_path = partition.database.path
                    self.log("  Partion: "+str(partition))
                    self.log("  To Database: "+db_path)
                     
                    t = petl.fromregex(rf, regex=regex, header=header)
 
                    if state == 'pr':
                        t = t.convert('NAME', unicode)
 
                    t.progress(100000).appendsqlite3(db_path,'sf1geo')
               
               
                    return True
                        
    
    @staticmethod
    def parsenumber(v, strict=False):
        """
        Need to have our own version of parsenumber because the CA geo file
        has a text field with a j in it that is interpreted as a complex. 
        """
        
        try:
            return int(v)
        except:
            pass
        try:
            return long(v)
        except:
            pass
        try:
            return float(v)
        except:
            pass
        try:
            return complex(v)
        except:
            if strict:
                raise
        return v
   
def load_table(seg_number,source, segmap):
     b = Bundle();
     b.load_table(seg_number,source, segmap)
     
 
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
    
    