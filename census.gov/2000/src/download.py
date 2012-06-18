'''
Created on Jun 7, 2012

@author: eric
'''

class Download:
    
    def __init__(self, bundle):
        '''
        Constructor
        '''
        self.bundle = bundle
    

                        
    def run(self):
        
        extractDir = os.path.normpath(os.path.dirname(__file__)+'/extracts')
        
        if(not os.path.isdir(extractDir) ):
            os.makedirs(extractDir)
             
        downloadDir = self.bundle.path('downloads')
        
        if(not os.path.isdir(downloadDir) ):
            os.makedirs(downloadDir)
        
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