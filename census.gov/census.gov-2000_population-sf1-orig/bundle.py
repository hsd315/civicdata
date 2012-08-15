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

    def generate_partitions(self):
        from databundles.partition import PartitionIdentity
        #
        # Geo split files
        for table in self.build_split_geo_tables():
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for "+table.name)
                partition = self.partitions.new_partition(pid)
            else:
                self.log("Already created partition, skipping "+table.name)

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
            from databundles.orm import Column
            self.schema.schema_from_file(open(self.geoschema_file, 'rbU'))
    
            # Add extra fields to all of the split_tables
            for table in self.schema.tables:
                if not table.data.get('split_table', False):
                    continue;
            
                table.add_column('hash',  datatype=Column.DATATYPE_INTEGER,
                                  uindexes = 'uihash')
        
        self.generate_partitions()
        
        # First install the bundle main database into the library
        # so all of the tables will be there for installing the
        # partitions. 
        self.log("Install bundle")
        if self.library.get(self.identity.id_):
            self.log("Found in bundle library, skipping. ")
        else:
            self.library.put(self)
        
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
        
                            segments = {}
                            geo = m.groups()
                            lrn = geo[6]
                            for g in gens:
                                try:
                                    seg_number,  row = g.send(None if first else lrn)
                                    segments[seg_number] = row
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
    
                            yield state,  segments[1][4], dict(zip(header,geo)), segments
                            first = False
                            
                        # Check that there are no extra lines. 
                        for g in gens:
                            try:
                                while g.next(): 
                                    raise Exception("Should not have extra items left")
                            except StopIteration:
                                pass

        return
        
    def build_split_geo_tables(self):
        
        for table in self.schema.tables:
            
            if table.data.get('split_table', '') == 'A':
                yield table
        
    def build_get_geo_processors(self, table):
        '''Return the processor functions for a table. '''
        
        from databundles.transform import PassthroughTransform, CensusTransform
        
        source_cols = ([c.name for c in table.columns 
                            if not ( c.name.endswith('_id') and not c.is_primary_key)
                            and c.name != 'hash'
                           ])
        
        columns = [c for c in table.columns if c.name in source_cols  ]  
        processors = [CensusTransform(c) for c in columns]
        processors[0] = lambda row : None # Primary key column
        
        if table.name != 'record_code':
            columns += [ table.column('hash')]
            processors += [lambda row : None]
 
        return columns, processors
               
    def build_get_split_geo_processors(self):
        '''Generate a complete set of geo processors for all of the split tables'''
            
        processor_set = {}
        for table in self.build_split_geo_tables():
            processor_set[table.id_] = (table,)+self.build_get_geo_processors(table)  
            
        return processor_set
    
    def row_hash(self, values):
        '''Calculate a hash from a database row, for geo split tables '''  
        import hashlib
        
        m = hashlib.md5()
        for x in values[1:]:  # The first record is the primary key
            m.update(str(x))   
        hash = int(m.hexdigest()[:15], 16) # First 8 hex digits = 32 bit @ReservedAssignment
     
        return hash
          
    def split_geo_get_partition(self, table):
        '''Called in geo_partition_map to fetch, and create, the partition for a
        table ''' 
        from databundles.partition import PartitionIdentity
        from databundles.database import  insert_or_ignore
        
        pid = PartitionIdentity(self.identity, table=table.name)
        partition = self.partitions.find(pid) # Find puts id_ into partition.identity
        
        if not partition:
            raise Exception("Failed to get partition: "+str(pid.name))
        
        if not partition.database.exists():
            partition.create_with_tables(table.name)
            
            # Ensure that the first record is the one with all of the null values
            ins = insert_or_ignore(table.name, [table.columns[0]])
            db = partition.database
            db.dbapi_cursor.execute(ins, [None])
            db.dbapi_connection.commit()
            db.dbapi_close()
            
        return partition
      
    def geo_partition_map(self):
        '''Create a map from table it to partition for the geo split table'''
        partitions = {}
        for table in self.build_split_geo_tables():
            partitions[table.id_] = self.split_geo_get_partition(table)
            
        return partitions
    
    
    def get_geo_record_id(self, table_id, hash):
        '''Return the primary key Id for a table, givn the hash of the values'''
        pass
    
    
    def get_table_by_table_id(self,table_id):  
        '''Get the table definition from the schema'''
        pass
        
    def write_geo_row(self, table, values):
        pass
    
    def write_fact_row(self, table_id, geo, values):
        pass
             
    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        import time
        from databundles.transform import PassthroughTransform, CensusTransform
        
        header, regex, regex_str = self.schema.table('sf1geo').get_fixed_regex()
   
        range_map = yaml.load(file(self.rangemap_file, 'r')) 
   
        table_processors = self.build_get_split_geo_processors()

        geo_partitions = self.geo_partition_map()

        row_i = 0
        t_start = time.time()
        for state, logrecno, geo, segments in self.generate_rows():
          
            for table_id, cp in table_processors.items():
                table, columns, processors = cp
 
                
                values=[ f(geo) for f in processors ]
                values[-1] = self.row_hash(values)
                
                partition = geo_partitions[table_id]
                self.write_geo_row(table, values)
               
            
            for seg_number, segment in segments.items():
                for table_id, range in range_map[seg_number].iteritems():
                    values = ([geo['stusab'], geo['sumlev'], geo['geocomp'], geo['chariter'], geo['cifsn'], geo['logrecno'] ] +
                                   segment[range['start']:range['source_col']] )
                    self.write_fact_row(table_id, geo,  values)
                    
            if row_i % 1000 == 0:
                # Prints a number representing the processing rate, 
                # in 1,000 records per sec.
                
                self.log(str(int( row_i/(time.time()-t_start)))+" "+str(row_i)+" "+str(int((time.time()-t_start)/60)))

            row_i += 1

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
    
    