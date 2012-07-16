'''

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
        self.clean()
        self.database.create()
        
        # Get any dependencies. Doing it here just to get error in pre_prepare
        self.library.require('test')
        self.library.require('dim')
       
        from  databundles.orm import Column
    
        s = self.schema
        s.clean()
        t = s.add_table('example')
        s.add_column(t,'rand1',datatype=Column.DATATYPE_INTEGER)
        s.add_column(t,'rand2',datatype=Column.DATATYPE_REAL)
        s.add_column(t,'uuid',datatype=Column.DATATYPE_TEXT)     
        s.add_column(t,'tag_id',datatype=Column.DATATYPE_INTEGER)  
        s.add_column(t,'flags_id',datatype=Column.DATATYPE_INTEGER)           
        self.database.commit()

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
     
    
    