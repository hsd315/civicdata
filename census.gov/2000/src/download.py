'''
Created on Jun 7, 2012

@author: eric
'''

class Download:
    
    def __init__(self):
        '''
        Constructor
        '''
        pass
    
    def run(self):
       
        import pprint
        import os.path
        import urllib
        import urlparse
        import zipfile
        
        from databundles.config  import Config
        from bs4 import BeautifulSoup
        
        configFile = os.path.normpath(os.path.dirname(__file__)+'/bundle.yaml')
       
        downloadDir = os.path.normpath(os.path.dirname(__file__)+'/downloads')
        
        if(not os.path.isdir(downloadDir) ):
            os.makedirs(downloadDir)
        
        extractDir = os.path.normpath(os.path.dirname(__file__)+'/extracts')
        
        if(not os.path.isdir(extractDir) ):
            os.makedirs(extractDir)
        
        config = Config(configFile)
        r = config.load()
        
        states = r['build']['states']
        url =r['build']['rootUrl']
       
        doc = urllib.urlretrieve(url)
       
        soup = BeautifulSoup(open(doc[0]))
        
        for link in soup.find_all('a'):
            # 
            if(link.string and link.string[:-1] in states):
                stateUrl = urlparse.urljoin(url, link.get('href'))
                
                stateIndex = urllib.urlretrieve(stateUrl)
                soup = BeautifulSoup(open(stateIndex[0]))
        
                for link in soup.find_all('a'):
                    if link.get('href') and  '.zip' in link.get('href'):
                        aurl = urlparse.urljoin(stateUrl, link.get('href'))
                       
                        path = urlparse.urlparse(aurl).path
                        webFilename = os.path.normpath(downloadDir+'/'+os.path.basename(path))
                      
                        if(not os.path.exists(webFilename)):
                            print 'Downloading '+aurl+' to '+webFilename
                            webFilename, headers = urllib.urlretrieve(aurl, webFilename )
                        else:
                            print 'Using cached web file '+webFilename
                      
                     
                        try :
                            with zipfile.ZipFile(webFilename) as zf:
                                for name in  zf.namelist():
                                    extractFilename = os.path.normpath(extractDir+'/'+name)
                                    if(not os.path.exists(extractFilename)):
                                        print 'Extracting'+extractFilename+' from '+webFilename
                                        name = name.replace('/','').replace('..','')
                                        zf.extract(name,extractDir )
                                    else:
                                        print 'Using cached extract file '+extractFilename
                        except zipfile.BadZipfile:
                            os.unlink(webFilename)
                            print "ERROR: Not a zipfile: "+webFilename
                                
                               
                            
if __name__ == '__main__':
    o = Download();
    o.run()