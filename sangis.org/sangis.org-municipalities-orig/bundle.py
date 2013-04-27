'''

'''

from  databundles.bundle import BuildBundle
 
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)


    def build(self):
        from databundles.identity import PartitionIdentity
        
        pid = PartitionIdentity(self.identity, table='municipalities')
        gp = self.partitions.new_geo_partition(pid, self.config.build.sources.boundaries)
        
        return True

    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    