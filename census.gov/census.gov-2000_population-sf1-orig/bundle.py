'''
Bundle build file for 2000 US Census, Summary file 1

Created on Jun 10, 2012

@author: eric
'''
import  sourcesupport.uscensus
from  databundles.bundle import BuildBundle

import os.path  
import yaml

class Bundle(BuildBundle):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        bg = self.config.build
        self.segmap_file =  self.filesystem.path(bg.segMapFile)
        self.headers_file =  self.filesystem.path(bg.headersFile)
        self.geoheaders_file = self.filesystem.path(bg.geoheaderFile)
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.rangemap_file =  self.filesystem.path(bg.rangeMapFile)
        self.urls_file =  self.filesystem.path(bg.urlsFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        
    def scrape_urls(self):

        import sourcesupport.uscensus #@UnusedImport
        
        if os.path.exists(self.urls_file):
            self.log("Urls file already exists. Skipping")
            return 
       
        urls = sourcesupport.uscensus.scrape_files(
                    self.config.build.rootUrl,self.states_file,
                    log=self.log, tick=self.ptick)
   
        yaml.dump(urls, file(self.urls_file, 'w'),indent=4, default_flow_style=False)
            
        return yaml.load(file(self.urls_file, 'r')) 

    def make_seg_map(self):
        
        if os.path.exists(self.segmap_file):
            self.log("Re-using segment map")
            return

        self.log("Making segment map")

        map_ = sourcesupport.uscensus.make_segment_map(self.headers_file,
                                                self.log, self.ptick)

        yaml.dump(map_, file(self.segmap_file, 'w'),indent=4, default_flow_style=False)  

    def make_range_map(self):
        
        if os.path.exists(self.rangemap_file):
            self.log("Re-using range map")
            return

        self.log("Making range map")

        rangemap = sourcesupport.uscensus.make_range_map(self.urls_file, self.segmap_file,
                                                self.schema.table, self.log, self.ptick)

        yaml.dump(rangemap, file(self.rangemap_file, 'w'),indent=4, default_flow_style=False)  
        

    def generate_schema(self):
        
       sourcesupport.uscensus.generate_table_schema(self.headers_file, self.schema,  
                                                    self.log, self.ptick)

    def prepare(self):
        '''Create the prototype database'''
        from  databundles.database import Database


        if not self.database.exists():
            self.database.create()

        self.scrape_urls()
        self.make_seg_map()
        
        self.generate_schema()
        
        self.make_range_map()

    
        if not self.schema.table('sf1geo'):
            import  schema.Column
            self.schema.schema_from_file(open(self.geoschema_file, 'rbU'))
    
            # Add extra fields to all of the split_tables
            for table in self.schema.tables:
                if not table.data.get('split_table', False):
                    continue;
            
                table.add_column('hash',  datatype=Column.DATATYPE_INTEGER,
                                  uindexes = 'uihash')
        
        return True

    def generate_seg_rows(self, seg_number, source):
        '''Generate rows for a segment file. Call this generator with send(), 
        passing in the lexpected logrecno. If the next row does not have that 
        value, return a blank row until the logrecno values match. '''
        import csv
        next_logrecno = None
        with self.filesystem.download(source) as zip_file:
            with self.filesystem.unzip(zip_file) as rf:
                for row in csv.reader(open(rf, 'rbU') ):
                    # The next_logrec bit takes care of a differece in the
                    # segment files -- the PCT tables to not have entries for
                    # tracts, so there are gaps in the logrecno sequence for those files. 
                    while next_logrecno is not None and next_logrecno != row[4]:
                        next_logrecno = (yield seg_number,  [])
             
                    next_logrecno = (yield seg_number,  row)
                 
        return
                     
    def generate_rows(self):
        ''' '''
        import csv
        
        table = self.schema.table('sf1geo')
        header, regex, regex_str = table.get_fixed_regex()
        urls = yaml.load(file(self.urls_file, 'r')) 
         
         
        for state,geo_source in urls['geos'].items():
              
            gens = [self.generate_seg_rows(n,source) for n,source in urls['tables'][state].items() ]
    
            with self.filesystem.download(geo_source) as geo_zip_file:
                with self.filesystem.unzip(geo_zip_file) as grf:
                    with open(grf, 'rbU') as geofile:
                        first = True
                        for line in geofile.readlines():
                            
                            m = regex.match(line)
                             
                            if not m:
                                raise ValueError("Failed to match regex on line: "+line) 
        
                            out = {}
                            out['geo'] = dict(zip(header,m.groups()))
                            lrn = out['geo']['logrecno']
                            for g in gens:
                                try:
                                    seg_number,  row = g.send(None if first else lrn)
                                    out[seg_number] = row
                                    # The logrecno must match up across all files, except
                                    # when ( in PCT tables ) there is no entry
                                    if len(row) > 5 and row[4] != lrn:
                                        raise Exception("Logrecno mismatch for seg {} : {} != {}"
                                                        .format(seg_number, row[4],lrn))
                                except StopIteration:
                                    # Apparently, the StopIteration exception, raised in
                                    # a generator function, gets propagated all the way up, 
                                    # ending all higher level generators. thanks for nuthin. 
                                    break
    
                            yield state, out[1][4], out
                            first = False
                            
                        # Check that there are no extra lines. 
                        for g in gens:
                            try:
                                while g.next(): 
                                    raise Exception("Should not have extra items left")
                            except StopIteration:
                                pass

        return
                                
    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        
        import pprint
   
        for state, logrecno, segments in self.generate_rows():
            print state, logrecno, segments[12]
 
        return True
 
        urls = yaml.load(file(self.urls_file, 'r')) 
        range_map = yaml.load(file(self.rangemap_file, 'r'))   
  
        # Install the main bundle first to get the schem in place in the 
        # library, so subsequent partition installations have something to 
        # refer to
        self.log("Put main bundle to Database")
        self.library.put(self)
        self.log("Get started building")
        
        for state,segments in urls['tables'].items():
            for seg_number,source in segments.items():  
                self.load_table(source, range_map[state][seg_number])
      
        return True

    def load_table(self,source, range_map):
        '''For a set of partitions and a path to a zip file, break the
        data in the zip file into seperate partition files. ''' 
        import re,  copy, os, csv
        from sqlalchemy.schema import CreateTable
        from databundles.partition import PartitionIdentity
        import csv
        import petl
      
        if self.filesystem.get_url(source):
            self.log("Already processed, skipping: "+source)
            return
         
        state, number = re.match('.*/(\w{2})(\d{5}).uf1', source).groups()
        number = int(number)
         
        self.log("Loading tables from {}, segment {}, state {}"
                 .format(source,number, state))
        
        import time
        start_time = time.clock()
        count = 0
        
        # Now, for each row, we can use the range map to take slices of columns
        # and write those to csv file partitions. 
        rows = 0;
        data = {}
        with self.filesystem.download(source) as zip_file:
            with self.filesystem.unzip(zip_file) as rf:
                for row in csv.reader(open(rf, 'rbU') ):            
                    # Pull off the common fields. 
                    common = row[:5]
            
                    for table_id, range in range_map.iteritems():
                        partition_row = common + row[range['start']:range['source_col']]
                        if not table_id in data:
                            data[table_id] = []
                            # Duplicate the first row. PETL will strip this one
                            # off, assuming that it is the header row. 
                            data[table_id].append(partition_row)
                            
                        data[table_id].append(partition_row)
                       
                    rows += 1
                    
                    if rows % 10000 == 0:
                        elapsed = time.clock() - start_time
                        self.log("{} rows in {} sec: {:.2f} rows per sec "
                                 .format(rows, elapsed, rows/elapsed)
                                 )
                        
        self.log("Done compiling. Writing files")
     
        #
        # Write the CSV files to the database. 
        #
        for table_id, range in range_map.iteritems():
            
            table_name = range['table']
            
            pid = PartitionIdentity(self.identity, table = table_name, space=state)
            partition = self.partitions.find(pid)
            
            if not partition:
                from databundles.exceptions import ResultCountError
                raise ResultCountError("Didn't get partition for "+pid.name)
            
            partition.database.delete()
            partition.database.create()
            partition.database.create_table(table_name)
            
            rows = data[table_id]
           
            db_path = partition.database.path
            self.log("Write {} rows to {}, table {}".format(len(rows),db_path, table_name))
            petl.appendsqlite3(petl.progress(rows, 200000),db_path, table_name)           
            
            dest = self.library.put(partition)
            self.log("Install in library: "+dest)
            partition.database.delete()
            
        # Will create if does not exist. 
        self.filesystem.get_or_new_url(source)
        self.database.close()
                
        return True
        

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
    
    