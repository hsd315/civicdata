'''
Created on Jun 10, 2012

@author: eric
'''
import sys, getopt

def get_args(argv):
    try:
      opts, args = getopt.getopt(argv,"hi:o:",["long1=","long2="])
    except getopt.GetoptError:
      print 'test.py -i <inputfile> -o <outputfile>'
      sys.exit(2)
    for opt, arg in opts:
        print opt+" "+arg

       
    return opts, args 

def main(argv):
    
    opts, args = get_args(argv)
    
    
    phase = args.pop(0)
    
    from bundle import Bundle

    b = Bundle()

    if phase == 'all':
        phases = ['prepare','download']
    else:
        phases = [phase]
  
    print phases
  
    if 'prepare' in phases:
        if b.pre_prepare():
            if b.prepare():
                print "Preparing"
                b.post_prepare()
                print "Done Preparing"
            
    if 'download' in phases:
        if b.pre_download():
            if b.download():
                print "Downloading"
                b.post_download()
                print "Done Downloading"
                
        
    if 'transform' in phases:
        if b.pre_transform():
            if b.transform():
                print "Transforming"
                b.post_transform()
                print "Done Transforming"
                
                  


if __name__ == '__main__':
    main(sys.argv[1:])