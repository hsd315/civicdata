'''
Bundle build file for 2000 US Census, Summary file 1

Created on Jun 10, 2012

@author: eric
'''

from  sourcesupport.uscensus import UsCensusBundle

import os.path  

class Bundle(UsCensusBundle):
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

     
        self.scrape_files()
        self.make_segment_map()
        self.build_schema()
        range_map = self.build_range_map()
        self.build_partitions(range_map)
        
        return True
       
 
    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        
        import yaml 
        from multiprocessing import Pool
        import tempfile
 
        urls = yaml.load(file(self.urls_file, 'r')) 
        range_map = yaml.load(file(self.rangemap_file, 'r'))   
  
        # Process the state files. 
        from multiprocessing import Pool
        p = Pool(4)
        for state,segments in urls['tables'].items():
            for seg_number,source in segments.items():  
                
                f = tempfile.NamedTemporaryFile(delete = False, prefix='bundle-range')    
                yaml.dump(range_map[state][seg_number],f)     
                #load_table_static(source,  f.name, self.filesystem.path('loadtable.py'))
                     
                #self.load_table(source, range_map[state][seg_number])
                p.apply_async(load_table_static, (source,  f.name, self.filesystem.path('loadtable.py')))
                
            
        # Process the geo files. 
        for state, source in urls['geos'].items():
            self.load_geo(state, source)

        return True

    def load_table(self,source, range_map):
        '''For a set of partitions and a path to a zip file, break the
        data in the zip file into seperate partition files. ''' 
        import re,  copy, os, csv
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
            with self.extract_zip(zip_file) as rf:
                for row in csv.reader(open(rf, 'rbU') ):            
                    # Pull off the common fields. 
                    common = row[:5]
            
                    for table_id, range in range_map.iteritems():
                        partition_row = common + row[range['start']:range['source_col']]
                        if not table_id in data:
                            data[table_id] = []
                            
                        data[table_id].append(partition_row)
                       
                    rows += 1
                    
                    if rows % 10000 == 0:
                        elapsed = time.clock() - start_time
                        self.log("{} rows in {} sec: {:.2f} rows per sec "
                                 .format(rows, elapsed, rows/elapsed)
                                 )
                        
        self.log("Done compiling. Writing files")
     
        for table_id, range in range_map.iteritems():
            
            table_name = range['table']
            
            pid = PartitionId(table = table_name, space=state)
            partition = self.partitions.find(pid)
            
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
        self.config.get_or_new_url(source)
        self.database.close()
                
        return True
        
    def load_geo(self, state, source):
        from databundles.partition import PartitionId
        import re,  copy
    
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
                        
def load_table_static(source, range_map_file_name, cmd):
    '''Adapter function to call a seperate process. It is outside of the class, 
    because the multiprocessing module uses pickling, which can't deal with 
    class methods. It then calls a sub process because doing the work directly results in 
    incomplrehensible, opaque errors
    
    This is a complete, horrible hack.'''
    import subprocess
    import sys
    import os
      
    print "Executing {} {} {} {} ".format(sys.executable, cmd, source, range_map_file_name)
    args = [sys.executable, cmd, source, range_map_file_name]    
         
    subprocess.check_call(args)

    os.remove(range_map_file_name)
   

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
    
    