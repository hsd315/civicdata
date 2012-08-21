'''
Bundle build file for 2000 US Census, Summary file 1

Created on Jun 10, 2012

@author: eric
'''
from  sourcesupport.us2010census import Us2010CensusBundle
    
class Bundle(Us2010CensusBundle):
    '''
    Bundle code for US 2010 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
    
        
    #def build(self, multi_func=None):   
    #    super(Bundle, self).build(multi_func=run_state_tables)
     
    def build(self, multi_func=None):
        '''Create data  partitions. 
        First, creates all of the state segments, one partition per segment per 
        state. Then creates a partition for each of the geo files. '''
        import yaml
        from multiprocessing import Pool

        urls = yaml.load(file(self.urls_file, 'r')) 
        
        n = len(urls.keys())
        i = 1
        
        for state in urls.keys():
            self.log("Building Geo state for {}, {} of {}".format(state, i, n))
            self.run_state_geo(state)
            i = i + 1
         
        self.store_geo_splits()
            
        if self.run_args.multi:
            pool = Pool(processes=int(self.run_args.multi))
            result = pool.map_async(multi_func, urls['geos'].keys())
            print result.get()
        else:
            for state in urls['geos'].keys():
                self.log("Building fact tables for {}".format(state))
                self.run_state_tables(state)
          
        return True
        
    def prepare(self):
        '''Create the prototype database'''

        if not super(Bundle, self).prepare():
            return False

        return True

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
    
    