'''
Created on Jun 10, 2012

@author: eric
'''

def main():
    from bundle import Bundle

    b = Bundle()
    
    import pprint
    pprint.pprint(b.config.yaml)

    if b.pre_prepare():
        if b.prepare():
            b.post_prepare()
            
        


if __name__ == '__main__':
    main()