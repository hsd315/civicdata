'''

'''

from databundles.bundle import BuildBundle

import databundles.library

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    ### Prepare is run before building, part of the devel process.  
  
    def generate_schema(self):
        '''Return schema rows from the geoschema.csv file, 
        fetched from Google Docs '''
       
        if len(self.schema.tables) > 0 and len(self.schema.columns) > 0:
            self.log("Reusing schema")
            return True
            
        else:
            self.log("Regenerating schema. This could be slow ... ")
    
        # The geo headers for this package is online as a google spreadsheet
        import urllib2
        headers = urllib2.urlopen(self.config.build.geoheaders);
        
        self.schema.schema_from_file(headers)
  
    def generate_partitions(self):
        from databundles.partition import PartitionIdentity
        
        # Combined for year 2000 census
        pid = PartitionIdentity(self.identity, time='2000')
        self.partitions.new_partition(pid)
        
        for table in self.schema.tables:
            
            if table.name in ['sf1geo2000','sf1geo','record_code', 'geo_compat', 'release','usgs']:
                continue
            
            pid = PartitionIdentity(self.identity, table=table.name)
            
            if not self.partitions.find(pid):
                self.log("Create partition for "+pid.name)
                p = self.partitions.new_partition(pid)
                p.create_with_tables(table.id_)
            else:
                self.log("Found partition; Skipping "+pid.name)
        
        self.database.commit()
        
    def prepare(self):
        
        if not self.database.exists():
            self.database.create()
        
        self.generate_schema()
        self.schema.create_tables()
        
        self.generate_partitions()
        
        return True

    def split_geo(self):
        '''Split the geo file into seperate tables'''
        from databundles.database import  insert_or_ignore
        import time

        l = databundles.library.get_library()
        q = (l.query()
                 .identity(creator='clarinova.com', dataset='2000 Population',
                           subset = 'sf1geo', variation='orig')
                 .partition(any=True) # Get partitions, not just root bundle. 
            )
   
        for local_partition in self.partitions.all:
            
            if not local_partition.table:
                continue
            
            q = q.table(name=local_partition.table.name)
            
            try :
                remote_partition_result = q.one
                print local_partition.name, remote_partition_result.Partition.name
            except:
                self.error("Missing corresponding local_partition for: "+local_partition.name)
                continue

            remote_partition = l.get(remote_partition_result.Partition)
            
            row_i = 0
            t_start = time.clock()
            value_set = {}
            self.ptick(local_partition.table.name)
  
            sf1t = self.schema.table('sf1geo2000')
            source_cols = [ c.name for c in sf1t.columns ]
            table = local_partition.table
            processors = [c.processor() for c in table.columns if c.name in source_cols ]
            
            columns = [c for c in table.columns if c.name in source_cols ]
            ins = insert_or_ignore(table.name, columns)
            db = local_partition.database
            
            for row in remote_partition.database.connection.execute(
                                    "SELECT * FROM {}".format(local_partition.table.name)):
                row_i += 1
                
                if row_i % 50000 == 0:
                    self.ptick('.')
                    
                    db.dbapi_cursor.executemany(ins, value_set)
                    db.dbapi_connection.commit()
                    
                    value_set = []
                if row_i % 1000000 == 0:
                    self.ptick(str(row_i/1000000)+"M")
                    
                print "KEYS",row.keys()
                values =[ f(row) for f in processors ]
                value_set.append(values)
                print values

                continue
                

            self.ptick(' {}/s '.format(int( row_i/(time.clock()-t_start))))
    
        return True

    ########################
    # Old Code
 
   
    def build(self):
    
        #self.combine_geo()
        
        self.split_geo()
        
        #self.split_geo_sqlite()
        
        return True

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    