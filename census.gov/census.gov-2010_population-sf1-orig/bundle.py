'''
Bundle build file for 2000 US Census, Summary file 1

Created on Jun 10, 2012

@author: eric
'''
from  databundles.sourcesupport.us2010census import Us2010CensusBundle
   
def mp_run_state_tables(arg):
    n, state = arg
    b = Bundle()
    b.parse_args(sys.argv[1:])
    b.log("Building (MP) fact tables for {} {}/52".format(state, n))
    b.run_state_tables(state)
  
def mp_run_fact_db(arg):
    n, state = arg
    b = Bundle()
    b.parse_args(sys.argv[1:])
    b.log("Building (MP) fact tables for {} {}/52".format(state, n))
    b.run_fact_db(state)
    
class Bundle(Us2010CensusBundle):
    '''
    Bundle code for US 2010 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
    
    def build(self):
        return super(Bundle, self).build(mp_run_state_tables,mp_run_fact_db)
        
    def prepare(self):
        '''Create the prototype database'''
        return super(Bundle, self).prepare()

import sys


if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
    