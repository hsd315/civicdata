'''
Bundle build file for 2000 US Census, Summary file 1

Created on Jun 10, 2012

@author: eric
'''
from  databundles.sourcesupport.us2000census import Us2000CensusDimBundle
 
def mp_run_geo_dim(arg):
    import time
    n, table_id = arg

    if n!=0:
        # Let the first process get started early, to set up some database things
        # and prevent contention
        time.sleep(5)

    b = Bundle()
    b.parse_args(sys.argv[1:])
   
    b.log("Creating (MP) geo dim tables for {} {}/52".format(table_id, n))
    b.run_geo_dim(table_id)
    return table_id
  
def mp_run_join_geo_dim(partition_id):

    b = Bundle()
    b.parse_args(sys.argv[1:])
   
    b.log("Joining .csv files ( MP ) for partition {}".format(partition_id))
    partition = b.partitions.get(partition_id)
    
    try:
        b.join_geo_dim(partition)
    except:
        b.error("Failed to join {} ".format(partition.identity.name))
        
    return partition.identity.name  
    
class Bundle(Us2000CensusDimBundle):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
    
    def build(self):
        return super(Bundle, self).build(mp_run_join_geo_dim, 
                                         mp_run_geo_dim)
        
    def prepare(self):
        '''Create the prototype database'''
        
        return super(Bundle, self).prepare()
        
import sys

if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
