'''

'''

from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    header = ['county','state','ssacd','fipscd','cbsa','name']
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)


    def prepare(self):
        return True
        
    def raw_import(self, url):
        from databundles.partition import PartitionIdentity
        import time
        import re
        import pprint
        import csv
        import petl.fluent as petlf
        import re

        zip_file = self.filesystem.download(url)
        
        for f in self.filesystem.unzip_dir(zip_file):
            self.log("Examining zip file {}".format(f))
            if f.endswith('.txt'):
                t = (petlf.fromcsv(f, delimiter="\t")
                     .cut( *range(0, 6))
                     .convertnumbers()
                     .setheader(self.header)
                     .cutout('name')
                     .convert({'ssacd':int, 'fipscd':int, 'cbsa':int})
                     .sort()
                     
                      )
           
                return t
            
        return None
        
    def build(self):
        import petl.fluent as petlf;
        
        urls = self.config.build.urls

        arrays = []
        for key, url in urls.items():  
            self.log("Processing {}".format(key))  
            t = self.raw_import(url)
            if t:
                arrays.append(t)
            else:
                self.error("Failed Processing {}".format(key))  
        
        
        added, sub = petlf.diff(arrays[0], arrays[2])
        
        print added.lookall()
        
        print sub.lookall()
        
        
        return True
  
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    