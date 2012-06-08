'''
Created on Jun 7, 2012

@author: eric
'''

class Config(object):
    '''
    classdocs
    '''
    configFile = None
    o = None

    def __init__(self, configFile):
        '''
        Constructor
        '''
        self.configFile = configFile
        
    def load(self):
        import yaml
        self.o = yaml.load(file(self.configFile, 'r'))
      
        return self.o
    
    