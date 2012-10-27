'''
Created on Aug 17, 2012

@author: eric
'''
import unittest

bundle = None # Bundle is set to the current bundle by the run.run() function

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testName(self):
        import databundles.library
       
        l = databundles.library.get_library()
        
        name = bundle.identity.id_
        
        print "Looking for "+name
        
        lb = l.get(name)
        
        print "Bundle: "+lb.database.path

        partition = lb.partitions.find(table='p001')

        print partition.identity.name, partition.database.path

        for row in partition.database.connection.execute("select * from p001"):
            print row;
            

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()