'''
Created on Jun 7, 2012

@author: eric
'''

def rootDir(testFile='bundle.yaml'):
        '''
        Find the parent directory that contains the bundle.yaml file
        '''
        import sys
        import os
        
        d = sys.path[0]
        
        while os.path.isdir(d) and d != '/':
            test =  os.path.normpath(d+'/'+testFile)
            print "D "+test
            if(os.path.isfile(test)):
                return d
            d = os.path.dirname(d)
             
        return None

class RootDir:
    directory = None
    def __init__(self, directory):
        '''
        Constructor
        '''
        self.directory = directory
        
    

class Files(object):
    '''
    classdocs
    '''


    def __init__(self, params):
        '''
        Constructor
        '''
        
    def local(self):
        pass
    
   