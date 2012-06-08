'''
Created on Jun 7, 2012

@author: eric
'''

class ProcessHeaders:
       
    def __init__(self):
        '''
        Constructor
        '''
        pass
    
    def run(self):
        from databundle import files
        rd = files.rootDir()
        print rd
           

if __name__ == '__main__':
    o = ProcessHeaders();
    o.run()