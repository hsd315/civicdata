'''
Created on Jun 10, 2012

@author: eric
'''

def main():
    from bundle import Bundle

    b = Bundle()

  
    if b.pre_prepare():
        if b.prepare():
            b.post_prepare()
            
    if b.pre_download():
        if b.download():
            b.post_download()       


if __name__ == '__main__':
    main()