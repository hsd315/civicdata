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
            pid = PartitionIdentity(self.identity, table=table.name)
            self.partitions.new_partition(pid)
        
        self.database.commit()
        
  
    def prepare(self):
        
        if not self.database.exists():
            self.database.create()
        
        self.database.delete()
        self.database.create()
        
        self.generate_schema()
        self.schema.create_tables()
        
        self.generate_partitions()
        
        return True
    
  
    def split_geo(self):
        '''Split the geo file into seperate tables'''
        from databundles.partition import PartitionIdentity
        from databundles.database import  insert_or_ignore
        import sqlite3
        import time

    

       
        sf1t = self.schema.table('sf1geo2000')
        source_cols = [ c.name for c in sf1t.columns ]

        #
        # Construct a dict for information about the tables we are going to split
        # split into
        ti = {} 
   
        for table in self.schema.tables:
 
            # These tables have other sources, or will get processed later. 
            if table.name in ['sf1geo2000','sf1geo','record_code', 'geo_compat', 'release','usgs']:
                continue
      
      
            if not table.name in ti:
                ti[table.name] = { 'columns':[], 'f':[]}
            
            p = self.partitions.find(
                        PartitionIdentity(self.identity, table=table.id_))

       
            # Create database if they don't exist. We are only creating
            # the one table mentioned in the partition. 
            if not  p.database.exists():
                p.database.create();
                p.database.copy_table_from(self.database, table.id_)
                p.schema.create_tables()
         
            for column in table.columns:
                if column.name in source_cols:
                    ti[table.name]['columns'].append(column)      
                    ti[table.name]['f'].append(column.processor()) 
                    
                if not column.default:
                    raise Exception("Column {} does not have a default value".format(column.name))
                
                   
            # Get rid of any tables that don't recieve any columns 
            if len(ti[table.name]['columns']) == 0:
                del ti[table.name]
                continue
        
            ti[table.name]['ins'] = insert_or_ignore(table.name, ti[table.name]['columns'])
            ti[table.name]['meta'] =  p.database.table(table.name)
            ti[table.name]['db'] = p.database


        # Now get all of the state partitions fro the library. 
        l = databundles.library.get_library()
        q = (l.query()
                 .identity(creator='clarinova.com', dataset='2000 Population',
                           subset = 'sf1geo', variation='orig')
                 .partition(any=True) # Get partitions, not just root bundle. 
            )

        
        geo_i = 0;
 
        for result in q.all:
 
            geo = l.get(result.Partition)
            geo_i += 1

            value_set = {}
            row_i = 0;
            t_start = time.clock()
            
            print "\nGEO {} ".format(geo_i),geo.database.path  
            for row in geo.database.connection.execute("SELECT * FROM sf1geo"):
                row_i += 1
                
                if row_i % 1000 == 0:
                    self.ptick('.')
                
                for table in ti.keys():  
                 
                    if table not in value_set:
                        value_set[table] = []
                 
                    values =[ f(row) for f in ti[table]['f'] ]
                    value_set[table].append(values)

            self.ptick(' {}/s '.format(int( row_i/(time.clock()-t_start))))
            
            for table in ti.keys(): 
                db =  ti[table]['db']
                db.dbapi_cursor.executemany( ti[table]['ins'] , value_set[table])
                db.dbapi_connection.commit()
                
        for table in ti.keys():
            db.dbapi_close()

    ########################
    # Old Code
 
   
    def build(self):
    
        #self.combine_geo()
        
        #self.split_geo()
        
        self.split_geo_sqlite()
        
        return True

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    