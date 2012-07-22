'''

'''

import os.path

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
            
            row = { k:v.encode('ascii','ignore').strip() for k,v in row.items()}

         
            if not row['table']:
                new_table = True
                continue

            if new_table and row['table']:
                #print 'Table',row['table']
                t = self.schema.add_table(row['table'], **row)
                new_table = False
              
          
            #print 'Column',row['table'], row['column']
            self.schema.add_column(t,row['column'],
                                   is_primary_key=row['is_pk'],
                                   description=row['description'].strip(),
                                   datatype=tm[row['type'].strip()],
                                   unique_constraints = row['unique_constraints'].strip(),
                                   indexes = row['indexes'].strip()
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
        from databundles.orm import Column

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
            geo_col_names = [ c.name for c in sf1t.columns ]
            
            map_ = { "TRIM({})".format(c.name): c.name for  c in sf1t.columns}
            
            cdb.copy_from_attached(('sf1geo','sf1geo2000'), 
                                   name=name,
                                   columns=map_,
                                    on_conflict = 'IGNORE')
            cdb.detach(name)
            
    def split_geo(self):
        
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
    
        self.combine_geo()
        
        return True

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    