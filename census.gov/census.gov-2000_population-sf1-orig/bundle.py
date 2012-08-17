'''
Bundle build file for 2000 US Census, Summary file 1

Created on Jun 10, 2012

@author: eric
'''
import  sourcesupport.uscensus
from  databundles.bundle import BuildBundle
from databundles.util import lfu_cache
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
        
        self._table_id_cache = {}
        self._table_iori_cache = {}
        
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
        for table in self.geo_tables():
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for "+table.name)
                partition = self.partitions.new_partition(pid)
            else:
                self.log("Already created partition, skipping "+table.name)

        # The Fact partitions
        for table in self.fact_tables():
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

        if not self.schema.table('sf1geo'): # Do this only once for the database
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
                     
    def generate_rows(self, state, urls):
        '''A Generator that yelds a tuple that has the logrecno row
        for all of the segment files and the geo file. '''
        import csv
        
        table = self.schema.table('sf1geo')
        header, regex, regex_str = table.get_fixed_regex()
         
        geo_source = urls['geos'][state]
      
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
                        sumlev = geo[2]
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

                        first = False

                        yield state, segments[1][4], dict(zip(header,geo)), segments

                    # Check that there are no extra lines. 
                    for g in gens:
                        try:
                            while g.next(): 
                                raise Exception("Should not have extra items left")
                        except StopIteration:
                            pass
        return
        
    def geo_tables(self):
        
        m = { t.name:t for t in self.schema.tables }
        
        for table_name in sourcesupport.uscensus.geo_tables():
            table = m[table_name]
            
            if table.data.get('split_table', '') == 'A':
                yield table
     
    def fact_tables(self):
        for table in self.schema.tables:
            if table.data.get('fact',False):
                yield table

    def geo_processors(self):
        '''Generate a complete set of geo processors for all of the split tables'''
        from databundles.transform import PassthroughTransform, CensusTransform
        from collections import  OrderedDict
        
        processor_set = OrderedDict()
        
        for table in self.geo_tables():
          
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
     
            processor_set[table.id_] = (table, columns, processors )         
             
            
        return processor_set
    
    def fact_processors(self):
        '''Generate a complete set of processors for all of the fact tables.
        These processors only deal with the forieng keys to the geo split tables. '''
        from databundles.transform import PassthroughTransform, CensusTransform
        
        processor_set = {}
        for table in self.fact_tables():
          
            source_cols = [c.name for c in table.columns if c.is_foreign_key ]
            
            columns = [c for c in table.columns if c.name in source_cols  ]  
            processors = [PassthroughTransform(c) for c in columns]
     
            processor_set[table.id_] = (table, columns, processors )  
       
        return processor_set
    
    def geo_key_columns(self):
        ''' '''
        
        from databundles.transform import PassthroughTransform, CensusTransform
        
        column_sets = {}
        for table in self.fact_tables():
          
            source_cols = [c.name for c in table.columns if c.is_foreign_key ]
         
            column_sets[table.id_] = (table, source_cols)  
       
        return column_sets
        
    def row_hash(self, values):
        '''Calculate a hash from a database row, for geo split tables '''  
        import hashlib
        
        m = hashlib.md5()
        for x in values[1:]:  # The first record is the primary key
            m.update(str(x))   
        hash = int(m.hexdigest()[:15], 16) # First 8 hex digits = 32 bit @ReservedAssignment
     
        return hash
          
    def geo_partition(self, table):
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
            vals = [c.default for c in table.columns]
            vals[-1] = self.row_hash(vals)
            vals[0] = 0;
            
            ins = insert_or_ignore(table.name, table.columns)
            db = partition.database
            db.dbapi_cursor.execute(ins, vals)
            db.dbapi_connection.commit()
            db.dbapi_close()
            
        return partition
   
    def geo_partition_map(self):
        '''Create a map from table id to partition for the geo split table'''
        partitions = {}
        for table in self.geo_tables():
            partitions[table.id_] = self.geo_partition(table)
            
        return partitions
     
    def fact_partition(self, table):
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
            
        return partition
    
    def fact_partition_map(self):
        '''Create a map from table id to partition for the geo split table'''
        partitions = {}
        for table in self.fact_tables():
            partitions[table.id_] = self.fact_partition(table)
            
        return partitions 

    
    def insertion_map(self):
        from databundles.database import  insert_or_ignore
        
        for table in self.schema.tables:
            pass


    def get_table_by_table_id(self,table_id):  
        '''Get the table definition from the schema'''
        t = self._table_id_cache.get(table_id, False)
        
        if not t:
            t = self.schema.table(table_id)
            self._table_id_cache[table_id] = t

        return t

    def write_geo_row(self, partition, table, columns,values):
        from databundles.database import  insert_or_ignore
        
        ins = self._table_iori_cache.get(table.name, False)
        if not ins:
            ins = insert_or_ignore(table.name, columns)
            self._table_iori_cache[table.name] = ins
            
      
        #self.log('Write {} to {}: '.format(table.name, db.path))
        db = partition.database
        cur = db.dbapi_cursor
        cur.execute(ins, values)
        lastrowid =  cur.lastrowid
        db.dbapi_connection.commit()
       
        cur.execute("SELECT {} FROM {} WHERE hash = ?".format(table.name+"_id", table.name), 
                    (values[-1],) )
    
        
        row = cur.fetchone()
        
        if row:
            return row[0]
        else:
            print "FAILED IN {} FOR HASH {}".format(table.name, str(values[-1]))
            print values
            return -1
    
    def write_fact_rows(self, partition,  values):
        from databundles.database import  insert_or_ignore
        
        table = partition.table
        
        db = partition.database
        cur = db.dbapi_cursor
      
        ins = insert_or_ignore(table.name, table.columns)
        try:
            cur.executemany(ins, values)
        except Exception as e:
            self.log("ERROR: Failed to write to {}".format(db.path))
            
            raise e

    def run_state(self, state):
        
        import time
        from databundles.transform import PassthroughTransform, CensusTransform
        import random
        
        row_i = 0
        
        header, regex, regex_str = self.schema.table('sf1geo').get_fixed_regex()
   
        range_map = yaml.load(file(self.rangemap_file, 'r')) 
        urls = yaml.load(file(self.urls_file, 'r')) 
        geo_processors = self.geo_processors()
        fact_processors = self.fact_processors()

        geo_partitions = self.geo_partition_map()
        fact_partitions = self.fact_partition_map()
        
        row_cache = {table.id_:[] for table in self.schema.tables}
        
        write_frequency = 1000
        
        self.log("\n")
        self.ptick(state+' ')
        
        for state, logrecno, geo, segments in self.generate_rows(state, urls ):
            
            if row_i == 0:
                t_start = time.time()
            
            if row_i % 1000 == 0:
                self.ptick('.')
                
            if row_i % 5000 == 0:
                # Prints a number representing the processing rate, 
                # in 1,000 records per sec.
                self.ptick(str(int( row_i/(time.time()-t_start)))+'/s ')                
                
            if row_i % 25000 == 0:
                self.ptick(str(row_i/1000)+"K ")
                
            row_i += 1
            
            geo_keys = []
            
            for table_id, cp in geo_processors.items():
                table, columns, processors = cp
            
                values=[ f(geo) for f in processors ]
                values[-1] = self.row_hash(values)
                
                partition = geo_partitions[table_id]
                r = self.write_geo_row(partition, table, columns, values)
            
                geo_keys.append(r)           
            
            for seg_number, segment in segments.items():
                for table_id, range in range_map[seg_number].iteritems():
                     seg = segment[range['start']:range['source_col']]
                     table = self.get_table_by_table_id(table_id)
                     if len(seg) > 0:    
                         # The values can be null for the PCT tables, which don't 
                         # exist for some summary levels.       
                         values =  geo_keys + seg                                      
                         row_cache[table.id_].append(values)
                        
                     if row_i % write_frequency == 0:
                         # Chunk the commits to the database to speed things up. 
                         partition = fact_partitions[table_id]
                         self.write_fact_rows(partition, row_cache[table.id_])
                        
            if row_i % write_frequency == 0:
                # looks like re-building the entire list is a 
                # better way to clear out memory. 
                row_cache = {table.id_:[] for table in self.schema.tables}
                    
        #Write the remainder of rows and commit. 
        for seg_number, segment in segments.items():
            for table_id, range in range_map[seg_number].iteritems():
                partition = fact_partitions[table_id]
                
                self.write_fact_rows(partition, row_cache[table_id])
                
                partition.database.dbapi_connection.commit() 
                       

               
    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
       
        from multiprocessing import Pool

        self.multi = True

        urls = yaml.load(file(self.urls_file, 'r')) 
        
        if self.run_args.multi:
            p = Pool(int(self.run_args.multi))
            
            p.map(run_state, urls['geos'].keys())
            
        else:
            for state in urls['geos'].keys():
                self.run_state(state)

        return True

import sys

def run_state(state):
    b = Bundle()
    b.log("Starting process for {}".format(state))
    b.run_state(state)

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
    
    