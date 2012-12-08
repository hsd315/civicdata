'''

@author: eric
'''
from  databundles.bundle import BuildBundle
import os.path
import yaml
 
class Bundle(BuildBundle):
    '''
    Bundle code for US 2010 Census geo files. 
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        bg = self.config.build
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.states_file =  self.filesystem.path(bg.statesFile)

  
        
    def prepare(self):
        '''Scrape the URLS into the urls.yaml file and load all of the geo data
        into partitions, without transformation'''
        from databundles.partition import PartitionIdentity
      
        if not self.database.exists():
            self.database.create()
     
        if len(self.schema.tables) == 0 and len(self.schema.columns) == 0:
            self.log("Loading schema from file")
            with open(self.geoschema_file, 'rbU') as f:
               self.schema.schema_from_file(f)           
        else:
            self.log("Reusing schema")


        self.database.session.commit()
        
        return True

    def build(self):

        print self.identity.name

        return True       

    def install(self):  
     
        self.log("Install bundle")  
        dest = self.library.put(self)
        self.log("Installed to {} ".format(dest[2]))
        
        for partition in self.partitions:
        
            self.log("Install partition {}".format(partition.name))  
            dest = self.library.put(partition)
            self.log("Installed to {} ".format(dest[2]))

        return True
        
import sys

if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
