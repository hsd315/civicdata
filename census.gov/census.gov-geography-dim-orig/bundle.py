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
        from databundles.orm import Column
        import csv, re
    
        if len(self.schema.tables) > 0 and len(self.schema.columns) > 0:
            self.log("Reusing schema")
            return True
            
        else:
            self.log("Regenerating schema. This could be slow ... ")
    
        # The geo headers for this package is online as a google spreadsheet
        import urllib2
        headers = urllib2.urlopen(self.config.build.geoheaders);
        
        reader  = csv.DictReader(headers)
    
        self.log("Generating main table schemas")
       
        t = None

        tm = {
              'TEXT':Column.DATATYPE_TEXT,
              'INTEGER':Column.DATATYPE_INTEGER,
              'REAL':Column.DATATYPE_REAL,
              }

        new_table = True
        for row in reader:
          
            # If the spreadsheet gets downloaded rom Google Spreadsheets, it is
            # in UTF-8
            row = { k:v.decode('utf8').encode('ascii','ignore').strip() for k,v in row.items()}

            if not row['table']:
                new_table = True
                continue

            if new_table and row['table']:
                #print 'Table',row['table']
                t = self.schema.add_table(row['table'], **row)
                new_table = False
              
            # Ensure that the default doesnt get quotes if it is a number. 
            if row['default']:
                try:
                    default = int(row['default'])
                except:
                    default = row['default']
          
            # Build the index and unique constraint values. 
            indexes = [ row['table']+'_'+c for c in row.keys() if (re.match('i\d+', c) and row[c].strip())]  
            uindexes = [ row['table']+'_'+c for c in row.keys() if (re.match('ui\d+', c) and row[c].strip())]  
            uniques = [ row['table']+'_'+c for c in row.keys() if (re.match('u\d+', c) and  row[c].strip())]  
    
            datatype = tm[row['type'].strip()]
    
            if row['size'].strip():
                try:
                    size = int(row['size'])
                except:
                    size = None
            else:
                size = None
    
            if  size and size > 0:
                illegal_value = '9' * size
            else:
                illegal_value = None
   
            #print 'Column',row['table'], row['column']
            self.schema.add_column(t,row['column'],
                                   is_primary_key=row['is_pk'],
                                   description=row['description'].strip(),
                                   datatype=datatype,
                                   unique_constraints = ','.join(uniques),
                                   indexes = ','.join(indexes),
                                   uindexes = ','.join(uindexes),
                                   default = default,
                                   illegal_value = illegal_value,
                                   size = size
                                   )

        self.database.commit()
  
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
    
    def combine_geo(self):
        """Combine all of the serverate geo files into a single database, and
        trim all of the values """

        from databundles.partition import PartitionIdentity

        #
        # Get the original geo files from the library
        #
        l =  databundles.library.get_library()
        q = (l.query()
                 .identity(creator='clarinova.com', dataset='2000 Population',
                           subset = 'sf1geo', variation='orig')
                 .partition(any=True) # Get partitions, not just root bundle. 
            )

        combined = self.partitions.find(PartitionIdentity(self.identity, time='2000'))
     
        combined.database.create()
        combined.database.create_table('sf1geo2000')

        print "COMBINED", combined.database.path
  
        # Copy from the indidual partitions into the combined geo file, 
        # by attaching and copying within sqlite. 
        cdb = combined.database

        for result in q.all:   
 
            geo = l.get(result.Partition)
            print "GEO",geo.database.path 

            name = cdb.attach(geo.database)
            
            sf1t = geo.schema.table('sf1geo')
           
            map_ = { "TRIM({})".format(c.name): c.name for  c in sf1t.columns}
            
            cdb.copy_from_attached(('sf1geo','sf1geo2000'), 
                                   name=name,
                                   columns=map_,
                                    on_conflict = 'IGNORE')
            cdb.detach(name)
 
    def split_geo(self):
        '''Split the geo file into seperate tables'''
        from databundles.partition import PartitionIdentity
        from databundles.database import  insert_or_ignore
        import sqlite3
        import time
        

    
        def set_default(column, v):
            # The 'illegal' value is usually a string of '9'
            if str(v) == column.illegal_value or  ( v == '' or v is None):
                if column.datatype == 'text':
                    v = column.default
                else: # Ignoring case for REAL, since there aren't any. 
                    v = int(column.default)
                    
            return v

        def coerce_int(v):
            try:
                return int(v)
            except:
                return v

       
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
               
            ti[table.name]['partition'] = p
       
            # Create database if they don't exist. We are only creating
            # the one table mentioned in the partition. 
            if not  p.database.exists():
                p.database.create();
                p.database.copy_table_from(self.database, table.name)
                p.schema.create_tables()
        
            
            for column in table.columns:
                if column.name in source_cols:
                    ti[table.name]['columns'].append(column)
                    
                    # Strip test values, but not numbers
                    f = lambda v:  v.strip() if isinstance(v,basestring) else v
                    
                    # for numbers try to coerce to an integer. Doesn't work for None
                    # so we use an intermediate function
                    if column.datatype == 'integer':
                        f = lambda v, f=f: coerce_int(f(v))
 
                    # Set the default value. 
                    f = lambda v, column=column, f=f : set_default(column,f(v) )
             
                    # Extract the value from a position in the row
                    f = lambda row, column=column, f=f: f(row[column.name])

                    ti[table.name]['f'].append(f) 
                    
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
 
        print ti.keys()
 
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

 
    def split_geo_sqlite(self):
        '''Split the geo file using attachment and table copy'''  
    
                
        from databundles.partition import PartitionIdentity

        #
        # Get the original geo files from the library
        #
        l =  databundles.library.get_library()
        q = (l.query()
                 .identity(creator='clarinova.com', dataset='2000 Population',
                           subset = 'sf1geo', variation='orig')
                 .partition(any=True) # Get partitions, not just root bundle. 
            )

 
        for result in q.all:   

            geo = l.get(result.Partition)
            print "GEO",geo.database.path 

            geo_col_names = [ c.name for c in geo.schema.table('sf1geo').columns ]

            
            for table in self.schema.tables:
           
                p = self.partitions.find(
                        PartitionIdentity(self.identity, table=table.id_))
                
                pdb = p.database
                
                if not pdb.exists():
                    pdb.create();
                    
                
                print "PARTITION", p.name, p.identity.name, p.database.path
                
                pdb.create_table(table.name)
                attach_name = pdb.attach(geo.database)
                map_ = {}
                for c in table.columns:
                    if c.name in geo_col_names:
                        map_["TRIM({})".format(c.name)] = c.name
                
                if map_.items():
                    pdb.copy_from_attached(('sf1geo', table.name),
                                            columns=map_, name=attach_name,
                                            on_conflict = 'IGNORE')
                
                pdb.detach(attach_name)

    def build(self):
    
        #self.combine_geo()
        
        self.split_geo()
        
        return True

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    