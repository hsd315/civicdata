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
        import csv
    
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
        self.log("T = Table, C = Column")
    
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
          
            # Create extra indexes based on which source this shows up in 
            source_cols = ['c_2000', 'c_2010', 'acs_2010']
            
            # Remove empty fields
            indexes = [i for i in row['indexes'].strip().split(',') if i]
            
            for c in source_cols:
                if row[c].strip:
                    idx_name = row['table']+'_'+c
                    indexes.append(idx_name)
      
            #print 'Column',row['table'], row['column']
            self.schema.add_column(t,row['column'],
                                   is_primary_key=row['is_pk'],
                                   description=row['description'].strip(),
                                   datatype=tm[row['type'].strip()],
                                   unique_constraints = row['unique_constraints'].strip(),
                                   indexes = ','.join(indexes),
                                   default = default
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
        
        
        return
        
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
        from databundles.partition import PartitionIdentity
        import sqlite3

        sf1t = self.schema.table('sf1geo2000')
        source_cols = [ c.name for c in sf1t.columns ]

        ti = {} 
        for table in self.schema.tables:
 
            if table.name in ['sf1geo','record_code', 'geo_compat', 'release','usgs']:
                continue
            
            if not table.name in ti:
                ti[table.name] = { 'columns':[]}
            
            p = self.partitions.find(
                        PartitionIdentity(self.identity, table=table.id_))
               
            ti[table.name]['partition'] = p
       
            if not  p.database.exists():
                p.database.create();
                p.database.copy_table_from(self.database, table.name)
                p.schema.create_tables()
        
            for column in table.columns:
                if column.name in source_cols:
                    ti[table.name]['columns'].append(column.name)
        
            ti[table.name]['meta'] =  p.database.table(table.name)
            ti[table.name]['path'] = p.database.path
            ti[table.name]['connection'] = sqlite3.connect(p.database.path)
            ti[table.name]['cursor'] = ti[table.name]['connection'].cursor()
           

               
        l = databundles.library.get_library()
        q = (l.query()
                 .identity(creator='clarinova.com', dataset='2000 Population',
                           subset = 'sf1geo', variation='orig')
                 .partition(any=True) # Get partitions, not just root bundle. 
            )

        n = 0;
        for result in q.all:   
 
            geo = l.get(result.Partition)
            print "\nGEO",geo.database.path  
         
         
            for row in geo.database.connection.execute("SELECT * FROM sf1geo"):
                
                n = n + 1
                
                if n % 100 == 0:
                    self.ptick('.')
         
                ids = {}
                for table in ti.keys():  
                    if table in ['sf1geo2000','sf1geo','record_code', 'geo_compat', 'release','usgs']:
                        continue
   
               
                    trow = []
                    for column in ti[table]['columns']:
                        trow.append(row[column])
                      
                    ins = ("""INSERT OR IGNORE INTO {table} ({columns}) VALUES ({values})"""
                            .format(
                                 table=table,
                                 columns =','.join(ti[table]['columns']),
                                 values = ','.join(['?' for i in ti[table]['columns']]) #@UnusedVariable
                            )
                         )
                
               

                    values = [row[c] if not isinstance(row[c],basestring) else row[c].strip()
                                 for c in ti[table]['columns']]                       
  
                    ti[table]['cursor'].execute(ins,values) 

                    ids[table] = ti[table]['cursor'].lastrowid

                    ti[table]['connection'].commit()
                    
                print ids
               
 
    def split_geo_sqlite(self):
                
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
     
    
    