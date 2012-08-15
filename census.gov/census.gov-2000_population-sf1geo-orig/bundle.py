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
        self.urls_file =  self.filesystem.path(bg.urlsFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        
    
        
    def scrape_urls(self):
        import os.path
        import yaml
        import sourcesupport.uscensus #@UnusedImport
        
        if os.path.exists(self.urls_file):
            self.log("Urls file already exists. Skipping")
            return 
       
        urls = sourcesupport.uscensus.scrape_files(
                    self.config.build.rootUrl,self.states_file,
                    log=self.log, tick=self.ptick)
   
        yaml.dump(urls, file(self.urls_file, 'w'),indent=4, default_flow_style=False)
            
        return yaml.load(file(self.urls_file, 'r')) 

        
    def prepare(self):
        '''Create the prototype database'''
    
        from databundles.orm import Column
        from databundles.partition import PartitionIdentity
        import yaml
  
        # This must be here, since run_args is set outside of the bundle
        # constructor
        if self.run_args.reset:  
            self.reset = True     
        else:
            self.reset = False
  
        #
        # Pull the bundle database from the library. This is required 
        # because we'll be skipping partitions that have already been stowed
        # in the library, and if we don't re-use the installed bundle, the
        # partition ids won'd match. 
        bundle = self.library.get(self.identity)
        
        if bundle and not self.reset:
            import shutil
            rpath = bundle.database.path
            lpath = self.database.path
            
            self.database.close()
            shutil.copy(rpath, lpath)
     
        if self.reset:
            import os.path
            self.clean()
            self.database.delete()
            if os.path.exists(self.urls_file):
                os.remove(self.urls_file)
  
        
  
        if not self.database.exists():
            self.database.create()

        self.scrape_urls()

        if not self.schema.table('sf1geo'):
            self.schema.schema_from_file(open(self.geoheaders_file, 'rbU'))

            # Add extra fields to all of the split_tables
            for table in self.schema.tables:
                if not table.data.get('split_table', False):
                    continue;
            
                table.add_column('hash',  datatype=Column.DATATYPE_INTEGER,
                                  uindexes = 'uihash')
                       
        #
        # Combined partition 
        pid = PartitionIdentity(self.identity, table='sf1geo')
        combined = self.partitions.find(pid)
        if not combined :
            self.log("Create combined partition")
            combined = self.partitions.new_partition(pid)
        else:
            self.log("Already created combined partition, skipping ")

        # 
        # State geo partitions
        urls = yaml.load(file(self.urls_file, 'r')) 
        
        for state, source in urls['geos'].items(): #@UnusedVariable
            pid = PartitionIdentity(self.identity, space=state)
            partition = self.partitions.find(pid)
            
            if not partition:
                partition = self.partitions.new_partition(pid)
                self.log("Create partition:  "+partition.identity.name)
            else:
                self.log("Partition exists: "+partition.identity.name)
        
        #
        # Geo split files
        for table in self.split_geo_tables():
            pid = PartitionIdentity(self.identity, table=table.name)
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                self.log("Create partition for "+table.name)
                partition = self.partitions.new_partition(pid)
            else:
                self.log("Already created partition, skipping "+table.name)
        
        
        self.database.commit()
        
        # First install the bundle main database into the library
        # so all of the tables will be there for installing the
        # partitions. 
        if self.library.get(self.identity.id_):
            self.log("Found in bundle library, skipping. ")
        else:
            self.library.put(self)
        
        return True
 
            
        
 
    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''

        import yaml 
         
        # Need to install the bundle in the library to get all of the tables, 
        # required to install  split partitions in the library. 
        
        dest = self.library.put(self, remove=self.run_args.reset)
        self.log("Installed main bundle in library: "+dest)
        
        self.load_geo()
    
        self.combine_geo()
    
        #self.split_geo_sqlite()
   
        self.split_geo()
    
        return True
    
    def load_geo(self):
        '''Load all of the geo files into partitions, one for each state. '''
        import yaml
        # Process the geo files. 
        
        self.log('-------------- Load Geo ---------------')
        
        urls = yaml.load(file(self.urls_file, 'r')) 
        
        for state, source in urls['geos'].items():
            self._load_geo(state, source)
    
    def load_geo_get_partition(self,state):
        '''Get a partition for the state. Return False if the partition already
        exists in the library'''
        from databundles.partition import PartitionIdentity 
        
        pid = PartitionIdentity(self.identity, space=state)
        partition = self.partitions.find(pid)
        

        if self.library.get(partition.identity) and not self.reset:
            return True, partition
   
   
        if not partition.database.exists():
            self.log("Create database "+partition.database.path)
            partition.create_with_tables('sf1geo')
        else:
            self.log("Database already exists "+partition.database.path)  
      
        return False, partition 
    
    def _load_geo(self, state, source):
       
        import petl.fluent as petl #@UnusedImport
        import databundles.dbpetl
        
        from databundles.transform import CensusTransform

        table = self.schema.table('sf1geo')
        header, regex, regex_str = table.get_fixed_regex() #@UnusedVariable

        skip, partition = self.load_geo_get_partition(state)
           
        if skip :
            self.log("Found in library, skipping. "+partition.identity.name)
            return

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

        self.log('-------------- Combine Geo ---------------')

        # get  the combined partition
        pid = PartitionIdentity(self.identity, table='sf1geo')   
        combined = self.partitions.find(pid)
       
        if self.library.get(combined.identity) and not self.reset:
            self.log("Found combined partition in library, skipping. "+combined.identity.name)
            return True

        # If it hasn't been installed in the library, whatever database have is incomplete
        # and must be rebuilt. 
        combined.database.delete()
        combined.create_with_tables(combined.table.name)
        
        self.database.commit() 

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


    
    def split_geo_row_gen(self, combined, processors):
        '''A generator function to return rows from the sf1geo file, broken out
        into values for a single part type. We iterate over this file many times, 
        because we can't store the whole result set in memory for more than one
        table ''' 
        
        import hashlib
        import time
        row_i = 0
        
        # The hashes set keeps track of which hashes we've seen, so we don't
        # send everything to the database. 
        hashes = set()
        t_start = time.time()

        for row in combined.database.connection.execute("SELECT * FROM sf1geo"):
        
            row_i += 1
        
            if row_i % 25000 == 0:
                # Prints a number representing the processing rate, 
                # in 1,000 records per sec.
                self.ptick(' ')
                self.ptick(str(int( row_i/(time.time()-t_start)/1000)))
            
            if row_i % 500000 == 0:
                self.ptick(' ')
                self.ptick(str(float(row_i) / 1000000)+'M')
                self.ptick(' ')
                self.ptick(str(len(hashes)/ 1000)+'K')
    
            values=[ f(row) for f in processors ]
           
            m = hashlib.md5()
            for x in values[1:]:  m.update(str(x))   
            hash = int(m.hexdigest()[:15], 16) # First 8 hex digits = 32 bit @ReservedAssignment
     
            values[-1] = hash
            
            if hash not in hashes:
                hashes.add(hash)
                yield values    
  
        self.ptick("\n")

    def split_geo_get_partition(self, table):
        from databundles.partition import PartitionIdentity
        from databundles.database import  insert_or_ignore
        
        pid = PartitionIdentity(self.identity, table=table.name)
        partition = self.partitions.find(pid) # Find puts id_ into partition.identity
        
        if not partition:
            raise Exception("Failed to get partition: "+str(pid))
        
        if not partition.database.exists():
            partition.create_with_tables(table.name)
            
            # Ensure that the first record is the one with all of the null values
            ins = insert_or_ignore(table.name, [table.columns[0]])
            db = partition.database
            db.dbapi_cursor.execute(ins, [None])
            db.dbapi_connection.commit()
            db.dbapi_close()
            
         
        return partition


    

    def split_geo(self):
        '''Split the geo file into seperate tables, for all of the 
        summary levels that are not 101 ( census block ). There will be one partition
        for each geo split table, with the national level data. '''
     
        from databundles.database import  insert_or_ignore
        from databundles.partition import PartitionIdentity
    
        self.log('-------------- Split Geo ---------------')
    
        partition = self.partitions.find(PartitionIdentity(self.identity, table='sf1geo')) # Identity needs to have id_
        combined = self.library.get(partition.identity)
   
        if not combined:
            raise Exception("Didn't get Combined bundle from library "+partition.identity)
      
        for table in self.split_geo_tables():
            
            partition = self.split_geo_get_partition(table)
            
            if self.library.get(partition.identity) and not self.reset:
                self.log("Found partition in library, skipping: "+partition.identity.name)
                continue
            
            self.log("Processing table {} in partition {}  ".format(table.name, partition.identity.name))

            columns, processors = self.split_geo_get_processors(table)
             
            ins_gen = self.split_geo_row_gen(combined, processors)
            ins = insert_or_ignore(table.name, columns)
          
            db = partition.database
            
            self.log(" ---- " + table.name)
            self.log('Write to db: '+db.path)
           
            db.dbapi_cursor.executemany(ins, ins_gen)
            db.dbapi_connection.commit()
            db.dbapi_close()
           
            self.log("Install in library: "+partition.name)
            dest = self.library.put(partition)
            self.log("Installed in library: "+dest)
        

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
