'''
Created on Jun 10, 2012

@author: eric
'''

from databundles.bundle  import Bundle
  
class MyClass(Bundle):
    '''
    classdocs
    '''


    def __init__(self,params):
        '''
        Constructor
        '''
        super(Bundle, self).__init__(params)
        
    def prepare(self):
        super(Bundle, self).prepare(params)
    
    def download(self):
        super(Bundle, self).download(params)  
    
    def transform(self):
        super(Bundle, self).transform(params)
    
    def build(self):
        super(Bundle, self).build(params)
    
    def submit(self):
        super(Bundle, self).submit(params)