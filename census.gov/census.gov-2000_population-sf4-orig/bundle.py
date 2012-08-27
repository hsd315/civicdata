'''
Bundle build file for 2000 US Census, Summary file 4

Created on Jun 10, 2012

@author: eric
'''
from  sourcesupport.us2000census import Us2000CensusBundle
  
def mp_run_state_tables(arg):
    n, state = arg
    b = Bundle()
    b.parse_args(sys.argv[1:])
    
    b.log("Building (MP) fact tables for {} {}/52".format(state, n))
    b.run_state_tables(state)
    return state
  
def mp_run_fact_db(arg):
    n, table_id = arg
    b = Bundle()
    b.parse_args(sys.argv[1:])
   
    b.log("Building (MP) fact database for {} {}/52".format(table_id, n))
    b.run_fact_db(table_id)
    return table_id
    
class Bundle(Us2000CensusBundle):
    '''
    Bundle code for US 2000 Census, Summary File 4
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
    
        
    def build(self):
        return super(Bundle, self).build(mp_run_state_tables,mp_run_fact_db)
        
    def prepare(self):
        '''Create the prototype database'''

        if not self.database.exists():
            self.database.create()

        self.scrape_urls(suffix='_uf4')
      
        self.create_table_schema()

        self.make_range_map()
    
        self.create_split_table_schema()

        self.generate_partitions()
    
        
        return True
        
import sys

if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
