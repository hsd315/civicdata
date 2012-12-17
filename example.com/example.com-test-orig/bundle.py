'''
'''

from  databundles.bundle import BuildBundle
from  databundles.orm import Table, Column
from databundles.dbexceptions import ConfigurationError
import os.path
import logging
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self, directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        self.logger.setLevel(logging.DEBUG) 
  
    def prepare(self):
        from databundles.partition import PartitionIdentity
        
        self.database.delete()
        self.database.create()
        
        s = self.schema 
        s.clean()

        t1 = s.add_table('example')
                
        s.add_column(t1,'rand1',datatype=Column.DATATYPE_INTEGER, indexes='index1')
        s.add_column(t1,'rand2',datatype=Column.DATATYPE_REAL, indexes='index1')
        s.add_column(t1,'rand3',datatype=Column.DATATYPE_INTEGER, indexes='index1')
        s.add_column(t1,'rand4',datatype=Column.DATATYPE_REAL, indexes='index1')
        s.add_column(t1,'rand5',datatype=Column.DATATYPE_INTEGER, indexes='index1')
        s.add_column(t1,'rand6',datatype=Column.DATATYPE_REAL, indexes='index1')
        s.add_column(t1,'uuid',datatype=Column.DATATYPE_TEXT)
        s.add_column(t1,'tag',datatype=Column.DATATYPE_TEXT)
        s.add_column(t1,'flags',datatype=Column.DATATYPE_TEXT, indexes='index2')
        
        self.database.session.commit()
        
        spaces = ['space1', 'space2', 'space3', 'space4' ]
        times = ['time1','time2', 'time3','time4']
        
        spaces = ['space1','space2' ]
        times = ['time1','time2']
        
        table = self.schema.table('example')
        
        for space in spaces:
            for time in times:
                pid = PartitionIdentity(self.identity, time=time, space=space, table=table.name)
                partition = self.partitions.new_partition(pid)
                partition.database.session.commit()
                self.log('Creating: '+partition.identity.name)
                partition.create_with_tables()
                        
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
                       random.randint(1,10000),
                       random.random()*1000,
                       random.randint(1,10000),
                       random.random()*1000,
                       str(uuid.uuid4()),
                       random.choice(tags),
                       random.choice(flags)+random.choice(flags)
                       ])

        # make a few partitions

        
        for partition in self.partitions:

            import time
            t_start = time.time()
            with partition.database.inserter('example') as ins:
                self.log('Building partition: {}'.format(partition.identity.name))
                for i in range(1,5000):
                    ins.insert([
                           random.randint(1,10000),
                           random.random()*1000,
                           random.randint(1,10000),
                           random.random()*1000,
                           random.randint(1,10000),
                           random.random()*1000,
                           str(uuid.uuid4()),
                           random.choice(tags),
                           random.choice(flags)+random.choice(flags)
                           ])

                    if i % 1000 == 0:
                        # Prints the processing rate in 1,000 records per sec.
                        self.log(str(int( i/(time.time()-t_start)))+'/s '+str(i/1000)+"K ")
            partition.database.close()

        return True
       

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    