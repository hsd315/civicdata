'''

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
        
        tname = 'tags'
        yield Table(name=tname)
        yield Column(table_name=tname,name='tags_id',datatype=Column.DATATYPE_INTEGER)
        yield Column(table_name=tname,name='tag',datatype=Column.DATATYPE_TEXT)
        
        tname = 'flags'
        yield Table(name=tname)
        yield Column(table_name=tname,name='flags_id',datatype=Column.DATATYPE_INTEGER)
        yield Column(table_name=tname,name='flags',datatype=Column.DATATYPE_TEXT)
    
  

    def prepare(self):
        # Get any dependencies. Doing it here just to get error in pre_prepare
        test_bundle = self.library.require('test')
        self.schema.generate()
        return True
        

    ### Build the final package

    def build(self):
        test_bundle = self.library.require('test')
        source_db_path = test_bundle.database.path
        
        sink_db_path = self.database.path
        self.log("Processing original: "+source_db_path)
        table = (petl.fromsqlite3(source_db_path, "SELECT * FROM example")
                 .convertnumbers())
       
        print petl.look(table)
        
        (table.valuecounts('tag')
                .cut('value')
                .addrownumbers(10)
                .rename('value','tag')
                .rename('row','tag_id')
                .tosqlite3(sink_db_path, 'tags'))
        
        (table.valuecounts('flags')
                .cut('value')
                .addrownumbers(10)
                .rename('value','flags')
                .rename('row','flags_id')
                .tosqlite3(sink_db_path, 'flags')) 
        
        print 'Wrote to ',sink_db_path
        
        return True
     
   
  
     
    def install(self):
      
        self.log("Installing to library" + self.library.root)
        self.library.install_bundle(self)

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
     
    
    