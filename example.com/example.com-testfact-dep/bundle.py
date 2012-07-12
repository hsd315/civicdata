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
    
        tname = 'example'
        yield Table(name=tname)
        yield Column(table_name=tname,name='rand1',datatype=Column.DATATYPE_INTEGER)
        yield Column(table_name=tname,name='rand2',datatype=Column.DATATYPE_REAL)
        yield Column(table_name=tname,name='uuid',datatype=Column.DATATYPE_TEXT)     
        yield Column(table_name=tname,name='tag_id',datatype=Column.DATATYPE_INTEGER)  
        yield Column(table_name=tname,name='flags_id',datatype=Column.DATATYPE_INTEGER)   
         
    def prepare(self):
        # Get any dependencies. Doing it here just to get error in pre_prepare
        self.library.require('test')
        self.library.require('dim')
        
        self.schema.generate() # Add the schema information to the metadata tables
        self.schema.create_tables() # Create the tables in the database. Normally dont in the inserter
        return True

    ### Build the final package

    def build(self):
        orig_bundle = self.library.require('test') # original data
        dim_bundle = self.library.require('dim') # dimension table from orig
        
        sink = self.database.path

        tags = (petl.fromsqlite3(dim_bundle.database.path, "SELECT * FROM tags"))
        flags = (petl.fromsqlite3(dim_bundle.database.path, "SELECT * FROM flags"))
        
        orig = (petl.fromsqlite3(orig_bundle.database.path, "SELECT * FROM example"))
        
        print petl.look(tags)
        print petl.look(flags)
     
        fact = (orig
            .hashjoin(tags,key='tag').cutout('tag')
            .hashjoin(flags,key='flags').cutout('flags'))
        
        print petl.look(fact)
        
        fact.tosqlite3(sink, 'example', create=False)
      
        self.log("Wrote fact table to: "+self.database.path)
      
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
     
    
    