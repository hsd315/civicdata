'''
Created on Jun 10, 2012

@author: eric
'''

from  databundles.bundle import Bundle as Base
  
class Bundle(Base):
    '''
    classdocs
    '''


    def __init__(self,directory=None):
        '''
        Constructor
        '''
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
    def prepare(self):
        import processheaders
        o = processheaders.ProcessHeaders(self).run()
    
    def download(self):
        self.super_.download()  
    
    def transform(self):
        self.super_.transform()
    
    def build(self):
        self.super_.build()
    
    def submit(self):
        self.super_.submit()