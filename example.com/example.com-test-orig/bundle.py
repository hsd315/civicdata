'''
'''

from  databundles.bundle import BuildBundle
from  databundles.orm import Table, Column
from databundles.dbexceptions import ConfigurationError
import os.path
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self, directory=None):
        self.super_ = super(Bundle, self)
     
        self.super_.__init__(directory)

  
    def prepare(self):
        
        #self.database.delete()
        self.database.create()
        
        s = self.schema 
        s.clean()

        t1 = s.add_table('example')
                
        s.add_column(t1,'rand1',datatype=Column.DATATYPE_INTEGER )
        s.add_column(t1,'rand2',datatype=Column.DATATYPE_REAL)
        s.add_column(t1,'uuid',datatype=Column.DATATYPE_TEXT)
        s.add_column(t1,'tag',datatype=Column.DATATYPE_TEXT)
        s.add_column(t1,'flags',datatype=Column.DATATYPE_TEXT)
        
        self.database.session.commit()
        
        return True

    ### Build the final package

       
    def build(self):
        '''Create a table full or random data'''
        import random
        import uuid
        from databundles.partition import PartitionIdentity
        
    
        tags = ['one','two','three','pizza','unicorn']
        flags = ['a','b','c','d','e']
        
        # Create data in the main bundle, using the inserter
        with self.database.inserter('example') as ins:
            for i in range(1,1000):
                ins.insert([
                       random.randint(1,10000),
                       random.random()*1000,
                       str(uuid.uuid4()),
                       random.choice(tags),
                       random.choice(flags)+random.choice(flags)
                       ])

        # make a few partitions
        
        for space in ['space1', 'space2', 'space3', 'space4' ]:
            for time in ['time1','time2', 'time3','time4']:
                pid = PartitionIdentity(self.identity, time=time, space=space)
                partition = self.partitions.new_partition(pid)
                partition.database.session.commit()
                self.log('Creating: '+partition.identity.name)
                partition.init()

                with partition.database.inserter('example') as ins:
                    for i in range(1,10):
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
            self.log("Installing base to library")
            self.library.put(self)
            
            for partition in self.partitions.all:
                self.log("Installing partition to library: "+str(partition.identity.name))
                self.library.put(partition)
            
            
        except ConfigurationError:
            self.log("ERROR: Missing configuration for library root in bundle.yaml")
            return False
            
        return True
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    