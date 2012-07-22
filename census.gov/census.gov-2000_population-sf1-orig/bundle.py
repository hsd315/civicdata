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


    def prepare(self):
        '''Create the prototype database'''
        from  databundles.database import Database

        self.database.create()

        self.scrape_files()
        self.make_segment_map()

        self.generate_table_schema()

        range_map = self.build_range_map()
        self.build_partitions(range_map)
        
        return True

    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        import yaml
 
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
    
    