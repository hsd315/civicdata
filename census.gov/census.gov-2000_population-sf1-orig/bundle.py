'''
Bundle build file for 2000 US Census, Summary file 1

Created on Jun 10, 2012

@author: eric
'''
from  sourcesupport.us2000census import Us2000CensusBundle
    
class Bundle(Us2000CensusBundle):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
    
        
    def build(self, multi_func=None):
        super(Bundle, self).build(multi_func=run_state_tables)
        
import sys

def run_state_tables(state):
    b = Bundle()
    b.log("Building (MP) fact tables for {}".format(state))
    b.run_state_tables(state)

if __name__ == '__main__':
    import databundles.run
    #import cProfile 


    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
    