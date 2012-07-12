'''
Generates dimentsion tables for the original dataset. 
'''


from  databundles.bundle import Bundle as Base
from databundles.library import Library
import petl.fluent as petl

class Bundle(Base):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    ### Prepare is run before building, part of the devel process.  

    def schemaGenerator(self):
        '''Get the first line of the file and make a schema from it. ''' 
        from  databundles.orm import Table, Column
        
        yield Table(name='tags')
        yield Column(name='tags_id',datatype=Column.DATATYPE_INTEGER)
        yield Column(name='tag',datatype=Column.DATATYPE_TEXT)
     
        yield Table(name='flags')
        yield Column(name='flags_id',datatype=Column.DATATYPE_INTEGER)
        yield Column(name='flags',datatype=Column.DATATYPE_TEXT)

    def partitionGenerator(self):
        '''The Tags and Flags dimensions get created in both the
        main db and in partitions '''    
        from databundles.partition import  Partition, PartitionId
        
        yield Partition(self,PartitionId(table='tags'))
        yield Partition(self,PartitionId(table='flags'))
        
    def prepare(self):
        # Get any dependencies. Doing it here just to get error in pre_prepare
        test_bundle = self.library.require('test')
        self.schema.generate() # Add the schema information to the metadata tables
        self.schema.create_tables() # Create the tables in the database. Normally done in the inserter
        self.partitions.generate()
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
     
    
    