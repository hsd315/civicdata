'''
Created on Jul 30, 2012

@author: eric
'''
import unittest
import databundles.library


class Test(unittest.TestCase):


    def setUp(self):
        import os.path
        import databundles.filesystem
        import imp
        
        base_dir = databundles.filesystem.Filesystem.find_root_dir(
                    start_dir=os.path.abspath(os.path.dirname(__file__)))
    
        m = imp.load_source('bundle',os.path.join(base_dir, 'bundle.py'))
 
        self.bundle = m.Bundle( base_dir)
   

    def tearDown(self):
        pass


    def test_schema(self):
        
        print self.bundle.database.path
        
        sf1geo = self.bundle.schema.table('sf1geo')
        
        for table in  self.bundle.schema.tables:
            
            if table.name == 'sf1geo':
                continue;
     
            print "----", table.name
            for c1 in table.columns:
                
                if c1.is_primary_key:
                    continue
                
                try:
                    c2 = sf1geo.column(c1.name)
                    #print c1.name, c2.name
                    
                    if c2.datatype != c1.datatype:
                        print "Mismatch", c1.name, c2.datatype,  c1.datatype
                        
                except Exception as e:
                    print "Failed: ", c1.name, str(e)
        
              
    def x_test_datatypes(self):
        '''Iterate through the sf1geo combined file and check if each value
        can be converted to an int. If it can't, mark the row as a TEXT type. 
        Then, write an update of the geoschema.csv file to change the column
        datatypes'''
        
        import time
        import csv

        l =  databundles.library.get_library()
        result = (l.query()
                 .identity(name='census.gov-2000_population-sf1geo-orig-a7d9-r1')
                 .partition(table = 'sf1geo') # sf1geo table
            ).one
        
        partition = l.get(result.Partition)
        
        table = self.bundle.schema.table('sf1geo')
        
        c = partition.database.connection
    
        isints = {}
        columns = table.columns
        for col in columns:
            isints[col.name] = True;
    
        i = 0
        t_start = time.clock()
        for row in c.execute("SELECT * FROM sf1geo"):
            
            for k,v in row.items():
                if  (isints[k] 
                     and not isinstance(v, (long,int) ) 
                     and  v.strip(' #!') # Special codes, See SF1 Tech Doc data dictionary
                     and not v.isdigit()
                     
                     ):
                    print k, v
                    isints[k] = False

            i += 1;

            if i % 100000 == 0:
                print '{} {}/s '.format(i, int(i/(time.clock()-t_start)))
          
          
        for k,v in isints.items():
            datatype = 'integer' if v else 'text'
            c = table.column(k)
            if datatype != c.datatype:
                print "{}\t{}\t{}".format(c.name, c.datatype, datatype)
        
        file_ = open(self.bundle.geoheaders_file, 'rbU')
        output = open(self.bundle.geoheaders_file+".update.csv", 'w')

        reader  = csv.DictReader(file_)
        writer = csv.DictWriter(output, reader.fieldnames+['d_orig_type'])
        
        writer.writeheader()
        for row in reader:
            if row['column'].strip and isints.get(row['column'], False):
                datatype = 'INTEGER' if isints[row['column']] else 'TEXT'
                if row['type'] != datatype:
                    row['d_orig_type'] = row['type'] 
                    row['type'] = datatype
                    row['default'] = '-1' if isints[row['column']] else 'NONE'
            writer.writerow(row)
          
    

    def XtestName(self):
        
        l =  databundles.library.get_library()
        q = (l.query()
                 .identity(name='census.gov-2000_population-sf1geo-orig-a7d9-r1')
                 .partition(table = 'sf1geo') # sf1geo table
            )
        
        partition = l.get(q.one.Partition)
        
        print partition.name
        
        for row in partition.database.connection.execute("SELECT * FROM sf1geo"):
            print row


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()