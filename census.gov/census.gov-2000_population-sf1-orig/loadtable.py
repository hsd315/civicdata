'''
Created on Jul 13, 2012

@author: eric

Program to load a single source file, to allow parallel operation. 

'''
import argparse
from bundle import  Bundle
import yaml

def main():
    parser = argparse.ArgumentParser(description='Load a segment file into tables') 
    parser.add_argument('source_url', help='')
    parser.add_argument('range_file', help='')
    args = parser.parse_args()
    
    b = Bundle()
    b.log("Running load tables for {} {} ".format(args.source_url, args.range_file))
    range_map  = yaml.load(file(args.range_file, 'r'))
    b.load_table(args.source_url, range_map)
    
if __name__ == '__main__':
    main()