'''
Created on Jul 30, 2012

@author: eric
'''
import unittest
import databundles.library


class Test(unittest.TestCase):


    def setUp(self):
        import os.path
        import databundles.filesystem
        import imp
        
        base_dir = databundles.filesystem.Filesystem.find_root_dir(
                    start_dir=os.path.abspath(os.path.dirname(__file__)))
    
        m = imp.load_source('bundle',os.path.join(base_dir, 'bundle.py'))
 
        self.bundle = m.Bundle( base_dir)
   

    def tearDown(self):
        pass


    def test_partitions(self):
        
        for partition in self.bundle.partitions.all:
            id = partition.identity
            
            if id.table is None:
                continue
            
            pb = self.bundle.library.get(id)
        
            q = "select count(*) from {}".format(partition.table.name)
        
           
            for row in pb.database.connection.execute(q):
                print row[0],',',partition.table.name
                
            

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()