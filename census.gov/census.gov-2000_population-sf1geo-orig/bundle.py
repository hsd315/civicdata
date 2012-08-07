'''

'''

from  sourcesupport.uscensus import UsCensusBundle

class Bundle(UsCensusBundle):
    '''
    Build a bundle for the US 200 Census, SF1 File, geographic components
    
    Build Options
    
    Build options can be asses with the -b option. 
    
        split-reset. In the split_geo reutine, ignore the partitions in the library
    
    '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        bg = self.config.build
        self.geoheaders_file = self.filesystem.path(bg.geoheaderFile)
        self.sumlev_file =  self.filesystem.path(bg.summaryLevelsFile)
        
    def prepare(self):
        '''Create the prototype database'''
        from databundles.orm import Column
  
        #self.clean()
  
        if not self.database.exists():
            self.database.create()

        self.scrape_files()

        if not self.schema.table('sf1geo'):
            self.schema.schema_from_file(open(self.geoheaders_file, 'rbU'))

            # Add extra fields to all of the split_tables
            for table in self.schema.tables:
                if not table.data.get('split_table', False):
                    continue;
            
                table.add_column('hash',  datatype=Column.DATATYPE_INTEGER,
                                  uindexes = 'uihash')
                                
            

        self.database.commit()
        
        self.create_partitions()
        
        # First install the bundle main database into the library
        # so all of the tables will be there for installing the
        # partitions. 
        if self.library.get(self.identity.id_):
            self.log("Found in bundle library, skipping. ")
        else:
            self.library.put(self)
        
        return True
 
    def create_partitions(self):
        from databundles.partition import PartitionIdentity 
        import yaml
 
        print "Partitions", len(self.partitions.all)
 
        # Generate the state partitions
        urls = yaml.load(file(self.urls_file, 'r')) 
        for state, source in urls['geos'].items(): #@UnusedVariable

            pid = PartitionIdentity(self.identity, space=state)
            if not self.partitions.find(pid):
                self.log("Create partition for "+state)
                self.partitions.new_partition(pid)
               
            else:
                self.log("Already created partition, skipping "+state)
                
        # Generate partitions for the individual tables. 
        for table in self.schema.tables:
            
            # These tables have other sources, or will get processed later. 
            if table.name in ['sf1geo']:
                continue
      
            pid = PartitionIdentity(self.identity, table=table.name)
            if not self.partitions.find(pid):
                self.log("Create partition for "+table.name)
                self.partitions.new_partition(pid)
                #p.create_with_tables(table.name)
            else:
                self.log("Already created partition, skipping "+table.name)
           
        # create the combined partition
        pid = PartitionIdentity(self.identity, table='sf1geo')   
        if not self.partitions.find(pid):
            self.log("Create partition for "+table.name)
            self.partitions.new_partition(pid)
        else:
            self.log("Already created partition, skipping "+table.name)
            
        # Create summary level partitions
        if False:
            sumlevs = yaml.load(file(self.sumlev_file, 'r')) 
            for table in self.schema.tables:
                if table.name in ['sf1geo']:
                    continue;
                for sumlev in sumlevs:
                    pid = PartitionIdentity(self.identity, table=table.name, grain=sumlev)
                    if not self.partitions.find(pid):
                        self.log("Create sumlev partition for "+pid.name)
                        self.partitions.new_partition(pid)
                        #p.create_with_tables(table.name)
                    else:
                        self.log("Already created sumlev , skipping "+pid.name)
        
            
            
        self.database.commit() 
 
    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''

        import yaml 
         
        # Need to install the bundle in the library to get all of the tables, 
        # required to install  split partitions in the library. 
        
        dest = self.library.put(self, remove=self.run_args.reset)
        self.log("Installed main bundle in library: "+dest)
        
        # Process the geo files. 
        urls = yaml.load(file(self.urls_file, 'r')) 
        for state, source in urls['geos'].items():
            self.load_geo(state, source)
    
        self.combine_geo()
    
        #self.split_geo_sqlite()
   
        self.split_geo()
    
        return True
    
    def load_geo(self, state, source):
        from databundles.partition import PartitionIdentity 
        import petl.fluent as petl
        import databundles.dbpetl
        
        from databundles.transform import CensusTransform

        table = self.schema.table('sf1geo')
        header, regex, regex_str = table.get_fixed_regex() #@UnusedVariable

        pid = PartitionIdentity(self.identity, space=state)
        partition = self.partitions.find(pid)
       
        if self.library.get(partition.identity):
            self.log("Found in library, skipping. "+pid.name)
            return
   
        if not partition.database.exists():
            self.log("Create database "+partition.database.path)
            partition.create_with_tables('sf1geo')
        else:
            self.log("Database already exists "+partition.database.path)

     
        processors = [CensusTransform(c, useIndex = True) for c in table.columns ]
            
        def process_row(row):
            return [ f(row) for f in processors ]
  
        retry = 4
        while retry > 0:
            retry -= 1
            #try:  
            with self.filesystem.download(source) as zip_file:
                with self.filesystem.unzip(zip_file) as rf:
    
                    self.log("Processing GEO file: "+rf)
                              
                    db_path = partition.database.path
                    self.log("Partion: "+partition.name)
                    self.log("To Database: "+db_path)
                               
                    t = databundles.dbpetl.wrap(databundles.dbpetl.fromregex)(
                                            rf, regex=regex, header=header) #@UndefinedVariable

                    if state == 'pr': # Some unicode characters, but only in this file
                        t = t.convert('name', unicode)

                    (t
                     .mogrify(lambda row: process_row(row))
                     .progress(10000)
                     .appendsqlite3(db_path,'sf1geo'))
                              
                    dest = self.library.put(partition)
                    self.log("Install in library: "+dest)
                    
                    if not self.library.get(partition.identity):
                        raise Exception("Library Installation failed: "+partition.identity)
                    
                    partition.database.delete()
            
                    return True
                
        return False
                
    def combine_geo(self):
        """Combine all of the seperate geo files into a single database, and
        trim all of the values """

        from databundles.partition import PartitionIdentity


        pid = PartitionIdentity(self.identity, table='sf1geo')
        combined = self.partitions.find(pid)

        if self.library.get(combined.identity.id_):
            self.log("Found combined partition in library, skipping. "+combined.identity.name)
            return True

        combined.database.delete()
        combined.create_with_tables(combined.table.name)

        #
        # Get the original geo files from the library
        #
        l =  databundles.library.get_library()
        q = (l.query()
                 .identity(creator='clarinova.com', dataset='2000 Population',
                           subset = 'sf1geo', variation='orig')
                 .partition(any=True) # Get partitions, not just root bundle. 
            )

        # Copy from the indidual partitions into the combined geo file, 
        # by attaching and copying within sqlite. 
        cdb = combined.database

        row_i = 0
        for result in q.all:   

            geo = l.get(result.Partition)

            if not geo.identity.space:
                continue; # Only take state file; ignore national split files
            
            row_i += 1
            self.log(str(row_i)+" Loading: "+geo.database.path)
            
            name = cdb.attach(geo.database)
        
            cdb.copy_from_attached(('sf1geo','sf1geo'), 
                                   name=name,
                                   on_conflict = 'ABORT')
            cdb.detach(name)
 

        dest = self.library.put(combined)
        self.log("Installed combined file in library: "+dest)

    def split_geo_sumlev(self):
        '''Split the geo file using attachment and table copy'''         
        from databundles.partition import PartitionIdentity

        #
        # Get the original geo files from the library. These are the ones we 
        # just stored in load_geo
        #
        l =  databundles.library.get_library()
        q = (l.query()
                 .identity(creator='clarinova.com', dataset='2000 Population',
                           subset = 'sf1geo', variation='orig')
                 .partition(any=True) # Get partitions, not just root bundle. 
            )

        pid = PartitionIdentity(self.identity, table='sf1geo')
        p = self.partitions.find(pid) # Identity needs to have id_
        combined = self.library.get(p.identity)

        
        for row in combined.database.connection.execute("SELECT * FROM sf1geo"):
            for partition in self.partitions.all:
                if partition.identity.grain is None:
                    continue
                
                db = partition.database
 
    def get_processors(self, table):
        from databundles.transform import PassthroughTransform
        
        source_cols = ([c.name for c in table.columns 
                            if not ( c.name.endswith('_id') and not c.is_primary_key)
                            and c.name != 'hash'
                           ])
        
        columns = [c for c in table.columns if c.name in source_cols  ]  
        processors = [PassthroughTransform(c) for c in columns]
        processors[0] = lambda row : None # Primary key column
        
        if table.name != 'record_code':
            columns += [ table.column('hash')]
            processors += [lambda row : None]
 
        return columns, processors
 
    def get_split_partition(self, table):
        from databundles.partition import PartitionIdentity
        pid = PartitionIdentity(self.identity, table=table.id_)
        partition = self.partitions.find(pid) # Find puts id_ into partition.identity
        
        self.log("Processing table {} in partition {}  ".format(table.name, partition.identity.name))
        
        if not partition.database.exists():
            partition.create_with_tables(table.name)
         
        return partition

    def split_geo(self):
        '''Split the geo file into seperate tables'''
        import time
        from databundles.database import  insert_or_ignore
        from databundles.partition import PartitionIdentity
    
        pid = PartitionIdentity(self.identity, table='sf1geo')
        p = self.partitions.find(pid) # Identity needs to have id_
        combined = self.library.get(p.identity)
   
        if not combined:
            raise Exception("Didn't get Combined bundle from library "+pid.path)
      
        for table in self.schema.tables:

            if not table.data.get('split_table', False) and table.name != 'record_code':
                continue

            # Get all of the source columns, but exclude the foriegn keys
            columns, processors = self.get_processors(table)
             
            ins_gen = self.row_gen(combined, processors)
            ins = insert_or_ignore(table.name, columns)
          
            partition = self.get_split_partition(table)
          
            db = partition.database
            
            self.log(" ---- " + table.name)
            self.log('Write to db: '+db.path)
           
            db.dbapi_cursor.executemany(ins, ins_gen)
            db.dbapi_connection.commit()
            db.dbapi_close()
           
            self.log("Install in library: "+partition.name)
            dest = self.library.put(partition)
            self.log("Installed in library: "+dest)
        
    def row_gen(self, combined, processors):
        import hashlib
        import time
        row_i = 0
        
        # The hashes set keeps track of which hashes we've seen, so we don't
        # send everything to the database. 
        hashes = set()
        t_start = time.clock()
        for row in combined.database.connection.execute("SELECT * FROM sf1geo where sumlev = 91"):
        
            row_i += 1
        
            if row_i % 25000 == 0:
                # Prints a number representing the processing rate, 
                # in 1,000 records per sec.
                self.ptick(' ')
                self.ptick(str(int( row_i/(time.clock()-t_start)/1000)))
            
            if row_i % 500000 == 0:
                self.ptick(' ')
                self.ptick(str(float(row_i) / 1000000)+'M')
    
            values=[ f(row) for f in processors ]
           
            m = hashlib.md5()
            for x in values[1:]:  m.update(str(x))   
            hash = int(m.hexdigest()[:15], 16) # First 8 hex digits = 32 bit @ReservedAssignment
     
            values[-1] = hash
            
            if hash not in hashes:
                hashes.add(hash)
                yield values    
  
        self.ptick("\n")
            
    def split_geo_hash(self):
        '''Split the geo file into seperate tables'''
        from databundles.database import  insert_or_ignore
        from databundles.partition import PartitionIdentity
        from databundles.transform import PassthroughTransform
        import time
        import hashlib
        from collections import OrderedDict 
 
        pid = PartitionIdentity(self.identity, table='sf1geo')
        p = self.partitions.find(pid) # Identity needs to have id_
        combined = self.library.get(p.identity)
   
        if not combined:
            raise Exception("Didn't get Combined bundle from library "+pid.path)
   
        tables = []
        
        # Must have record_code first so
        for table in self.schema.tables:
            if table.name == 'record_code':
                tables.insert(0, table)
            elif table.data.get('split_table', False):
                tables.append(table)

      
        for table in tables:

            row_i = 0
            t_start = time.clock()
            value_set = OrderedDict()
            id_set = {}

            pid = PartitionIdentity(self.identity, table=table.id_)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if 'split-reset' in self.run_args.build_opt:
                self.log("split-reset, ignoring Library.")
            if self.library.get(partition.identity):
                self.log("Found in library, skipping. "+partition.name)
            
            
            self.log("Processing table {} in partition {}  ".format(table.name, partition.identity.name))
            
            if not partition.database.exists():
                self.log("Create database with table "+table.name)
                partition.create_with_tables(table.name)
            else:
                self.log("Database exists: "+partition.database.path)
         
            # Get all of the source columns, but exclude the foriegn keys
            source_cols = ([c.name for c in table.columns 
                            if not ( c.name.endswith('_id') and not c.is_primary_key)
                           ])
                
            processors = [PassthroughTransform(c) for c in table.columns if c.name in source_cols ] 
            processors[0] = lambda row: 95959 # replace the primary key with a placeholder
                 
            columns = [c for c in table.columns if c.name in source_cols ]
                
            self.log(" ")
            self.log(" ---- " + table.name)
            for row in combined.database.connection.execute("SELECT * FROM sf1geo"):
            
                row_i += 1
                
                if row_i % 10000 == 0:
                    self.ptick('.')
                    
                if row_i % 100000 == 0:
                    self.ptick(str(row_i/100000))
                    self.ptick("E5 ")
                    self.ptick(str(int( row_i/(time.clock()-t_start))))
                    self.ptick(' ')
                    self.ptick(str(len(value_set)))   
                    

                values=[ f(row) for f in processors ]
               
                m = hashlib.md5()
                for x in values:
                    m.update(str(x))
         
                hash = m.digest()
         
                if hash not in id_set:
                    value_set[hash] = values
                    id_set[hash] = [(row['stusab'], row['logrecno'])]
                else:
                    id_set[hash].append((row['stusab'],row['logrecno']))
           
            self.log(' ')
            self.log("Length: "+str(len(value_set)))  
            self.log('Collection Write {}/s '.format(int( len(value_set)/(time.clock()-t_start+.01))))

            # This generator expressions replaces the first value in the row, the placeholder
            # primary key, with an acualt key. 
          
            ins_gen = ([i]+item[1][1:] for i, item in enumerate(value_set.iteritems()))

            ins = insert_or_ignore(table.name, columns)
          
            db = partition.database
            self.log('Write to db: '+db.path)
            t_start = time.clock()
            db.dbapi_cursor.executemany(ins, ins_gen)
            db.dbapi_connection.commit()
            db.dbapi_close()
            self.log('Db Write {}/s '.format(int( len(value_set)/(time.clock()-t_start+.01))))
        
            self.log("Install in library: "+partition.name)
            dest = self.library.put(partition)
            self.log("Installed in library: "+dest)
            
        
            if table.name != 'record_code':
                
                partition.database.delete()
                #
                # Update the record_codes table to make the logrecno and stusab
                # keys to the new table rows. 
                
                def update_gen(): 
                    for i, item in enumerate(value_set.iteritems()):
                        for id in id_set[item[0]]:
                            yield (i,id[0], id[1])

                rcp = self.partitions.find(
                    PartitionIdentity(self.identity, table='record_code'))
            
                update = ("update record_code set {}=? where stusab = ? and logrecno = ?"
                           .format(table.name+'_id' ) )
                
                print update
                g = update_gen()
            
                print rcp.database.path
                db = rcp.database
                t_start = time.clock()
                db.dbapi_cursor.executemany(update, g)
                db.dbapi_connection.commit()
                db.dbapi_close()
                self.log('Record Code Write {}/s '.format(int( len(value_set)/(time.clock()-t_start+.01))))

                # Also install the record code into the library. We will end up doing
                # this multiple times, bu it will ensure that the library is in a useful
                # state if the program crashes and has to be restarted. 

                self.log("Install record_code in library: "+rcp.name)
                dest = self.library.put(rcp)
                self.log("Installed in library: "+dest)
               
        return True

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
