'''
Created on Jun 7, 2012

@author: eric
'''
import unittest



class Test(unittest.TestCase):


    def testBasic(self):
        from databundle import files
        rd = files.rootDir()
        print rd

        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()