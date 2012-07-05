'''
'''

from  databundles.bundle import Bundle as Base
from  databundles.orm import Table, Column
from databundles.exceptions import ConfigurationError
class Bundle(Base):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    ### Prepare is run before building, part of the devel process.  

    def schemaGenerator(self):
        '''Get the first line of the file and make a schema from it. ''' 

       
        yield Table(name='example')
        yield Column(name='rand1',datatype=Column.DATATYPE_INTEGER)
        yield Column(name='rand2',datatype=Column.DATATYPE_REAL)
        yield Column(name='uuid',datatype=Column.DATATYPE_TEXT)
        yield Column(name='tag',datatype=Column.DATATYPE_TEXT)
        yield Column(name='flags',datatype=Column.DATATYPE_TEXT)       
  
    def prepare(self):
        self.schema.generate()
        return True

    ### Build the final package

       
    def build(self):
        '''Create a table full or random data'''
        import random
        import uuid
        
    
        tags = ['one','two','three','pizza','unicorn']
        flags = ['a','b','c','d','e']
        
        with self.database.inserter('example') as ins:
            for i in range(1,1000):
                ins.insert([
                       random.randint(1,10000),
                       random.random()*1000,
                       str(uuid.uuid4()),
                       random.choice(tags),
                       random.choice(flags)+random.choice(flags)
                       ])

        return True
       
    ### Submit the package to the repository
 
    def install(self):
        
        try:
            self.log("Installing to library" + self.library.root)
            self.library.install_bundle(self)
        except ConfigurationError:
            self.log("ERROR: Missing configuration for library root in bundle.yaml")
            return False
            
        return True
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    