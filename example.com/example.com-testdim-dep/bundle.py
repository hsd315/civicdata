'''
Generates dimentsion tables for the original dataset. 
'''


from  databundles.bundle import BuildBundle
from databundles.library import Library
import petl.fluent as petl

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
       


    def prepare(self):
        # Get any dependencies. Doing it here just to get error in pre_prepare
        
        self.clean()
        self.database.create()
        
        test_bundle = self.library.require('test')
        
        from databundles.orm import Table, Column
        from databundles.partition import PartitionIdentity
        
        #
        # Build the Schema
        s = self.schema
        s.clean()
        t = s.add_table('tags')
        s.add_column(t, 'tags_id',datatype=Column.DATATYPE_INTEGER)
        s.add_column(t, 'tag',datatype=Column.DATATYPE_TEXT)
     
        t = s.add_table('flags')
        s.add_column(t, 'flags_id',datatype=Column.DATATYPE_INTEGER)
        s.add_column(t, 'flags',datatype=Column.DATATYPE_TEXT)
        
        self.schema.create_tables() # Create the tables in the database. Normally done in the inserter
        
        #
        # Build partitions
        #
        
        p = self.partitions
        p.clean()
        p.new_partition(PartitionIdentity(self.identity, table='tags'))
        p.new_partition(PartitionIdentity(self.identity, table='flags'))
        
        self.database.commit()
        
        print self.database.path
        
        return True

    ### Build the final package

    def build(self):
        test_bundle = self.library.require('test')
        source_db_path = test_bundle.database.path
        
        sink_db_path = self.database.path
        self.log("Processing original: "+source_db_path)
        table = (petl.fromsqlite3(source_db_path, "SELECT * FROM example")
                 .convertnumbers())
   
        tags = (table.valuecounts('tag')
                .cut('value')
                .addrownumbers(10)
                .rename('value','tag')
                .rename('row','tag_id'))
        
        tags.tosqlite3(sink_db_path, 'tags')
        
        # Save again for the parition. 
        part = self.partitions.find(table='tags')
        tags.tosqlite3(part.database.path, 'tags')
        
        flags = (table.valuecounts('flags')
                .cut('value')
                .addrownumbers(10)
                .rename('value','flags')
                .rename('row','flags_id')) 


        # Testing new petl code. 
        #dummy, flags = table.unjoin('flags',autoincrement=(10, 1))
        #print petl.look(flags)

        flags.tosqlite3(sink_db_path, 'flags')
     
         # Save again for the parition. 
        part = self.partitions.find(table='flags')
        tags.tosqlite3(part.database.path, 'flags')
        
        self.log('Wrote dimension tables to: '+sink_db_path)
        
        return True

    def install(self):
      
        self.log("Installing to library" + self.library.root)
        self.library.put(self)

        return True
     
    ### Submit the package to the repository
 
    def pre_submit(self):
        return True
    
    def submit(self):
        return True
        
    def post_submit(self):
        return True
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    