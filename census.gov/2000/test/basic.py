'''
Created on Jun 9, 2012

@author: eric
'''
import unittest


class Test(unittest.TestCase):


    def test_basic(self):
        import pprint
        import sys
        import os.path
        # Reset the path so the Bundle() constructor will walk up the right directory    
        sys.path[0] = os.path.dirname(__file__)
       
        from databundles.bundle  import Bundle
       
        b = Bundle()

        pprint.pprint(b.config.yaml)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()