'''

'''

from  sourcesupport.uscensus import UsCensusBundle

class Bundle(UsCensusBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        bg = self.config.build
        self.geoheaders_file = self.filesystem.path(bg.geoheaderFile)
        
    def prepare(self):
        '''Create the prototype database'''
  
        if not self.database.exists():
            self.database.create()

        self.scrape_files()

        if not self.schema.table('sf1geo'):
            self.schema.schema_from_file(open(self.geoheaders_file, 'rbU'))

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
            
            
        self.database.commit() 
 
    def build(self):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''

        import yaml 
      
        urls = yaml.load(file(self.urls_file, 'r')) 
          
        # Process the geo files. 
        for state, source in urls['geos'].items():
            self.load_geo(state, source)
    
        self.combine_geo()
    
        self.split_geo_sqlite()
    
        return True
    
    def load_geo(self, state, source):
        from databundles.partition import PartitionIdentity 
        import databundles.dbpetl
        import petl.fluent as petl
        from databundles.transform import CensusTransform

        table = self.schema.table('sf1geo')
        header, regex, regex_str = table.get_fixed_regex() #@UnusedVariable

        pid = PartitionIdentity(self.identity, space=state)
        partition = self.partitions.find(pid)
       
        if self.library.get(partition.identity.id_):
            self.log("Found in library, skipping. "+pid.name)
            return
   
        if not partition.database.exists():
            partition.create_with_tables('sf1geo')

        def print_follow(row):
            print row
            
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
                    self.log("  Partion: "+str(partition))
                    self.log("  To Database: "+db_path)
                     
                    t = petl.fromregex(rf, regex=regex, header=header) #@UndefinedVariable
 
                    if state == 'pr': # Some unicode characters, but only in this file
                        t = t.convert('name', unicode)

                    (t
                     .mogrify(lambda row: process_row(row))
                     .progress(100000)
                     .appendsqlite3(db_path,'sf1geo'))
                          
                    dest = self.library.put(partition)
                    self.log("Install in library: "+dest)
                    partition.database.delete()
                                  
                    return True
                
        return False
                
    def combine_geo(self):
        """Combine all of the seperate geo files into a single database, and
        trim all of the values """

        from databundles.partition import PartitionIdentity


        combined = self.partitions.find(PartitionIdentity(self.identity, table='sf1geo'))
     
        if not combined.database.exists():
            combined.create_with_tables(combined.table.name)

        if self.library.get(combined.identity.id_):
            self.log("Found in library, skipping. "+combined.identity.name)
            return True


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
 
            row_i += 1
 
            geo = l.get(result.Partition)
            self.log(str(row_i)+" Loading: "+geo.database.path)

            if not geo.identity.space:
                continue; # Only take state file; ignore national split files
            

            name = cdb.attach(geo.database)
            
            sf1t = geo.schema.table('sf1geo')
           
            map_ = { "TRIM({})".format(c.name): c.name for  c in sf1t.columns}
            
            cdb.copy_from_attached(('sf1geo','sf1geo'), 
                                   name=name,
                                   columns=map_,
                                on_conflict = 'ABORT')
            cdb.detach(name)
 

        dest = self.library.put(combined)
        self.log("Installed combined file in library: "+dest)

    def split_geo_sqlite(self):
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

        i = 0
        
        for result in q.all:   

            i += 1

            geo = l.get(result.Partition)
            self.log(str(i)+" --------- "+geo.database.path);
            
            if not geo.identity.space:
                continue; # Only take state file; ignore the ones this routine produces
            
            # Doing this in the loop means not having to do a seperate query
    
            # outsize of the loop
            geo_col_names = [ c.name for c in geo.schema.table('sf1geo').columns ]

            for table in self.schema.tables:
                
                if table.name in ['sf1geo']:
                    continue

                p = self.partitions.find(
                        PartitionIdentity(self.identity, table=table.id_))
                
                pdb = p.database
                
                if not pdb.exists():
                    pdb.create();
  
                self.log("Loading {} into {} ".format(geo.identity.name, p.identity.name) )
                
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
        #
        # Now that all of the bundles are done, install them in the library. 

        for table in self.schema.tables:
        
            if table.name in ['sf1geo']:
                continue  
            
            pid = PartitionIdentity(self.identity, table=table.id_)
            p = self.partitions.find(pid)
            
            self.log("Install in library: "+p.name)
            dest = self.library.put(p)
            self.log("Installed in library: "+dest)
            p.database.delete()
            
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
