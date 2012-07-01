'''
Created on Jun 9, 2012

@author: eric
'''


from  databundles.bundle import Bundle as Base
from  databundles.orm import Table, Column
import os.path  
import zipfile
import csv
from contextlib import contextmanager

class Bundle(Base):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)


    @contextmanager
    def downloader(self):
        url = self.config.group('files').get('sourceUrl')
        with self.filesystem.download(url, cache=True) as f:
            extractDir = os.path.dirname(f)
            with zipfile.ZipFile(f) as zf:
                for name in  zf.namelist():
                    
                    if 'allgmp.csv' in name:  
                        extractFilename = os.path.join(extractDir,name)
                        
                        if os.path.exists(extractFilename):
                            os.remove(extractFilename)
                            
                        self.log('Extracting'+extractFilename+' from '+f)
                        name = name.replace('/','').replace('..','')
                        zf.extract(name,extractDir )
                        yield extractFilename
                        os.unlink(extractFilename)
                        return
    
    def schemaGenerator(self):
        '''Get the first line of the file and make a schema from it. ''' 
        
        tname = self.identity.dataset
        
        
        yield Table(name=tname)
        
        with self.downloader() as f:
            reader = csv.reader(open(f, 'rb'))
            
            # The header map and uniques dicts keep track of the non-fact values
            # so we can make dimension tables. 
            header = reader.next()    
            first = reader.next()
           
           
        for index,name in enumerate(header):
            
            try:
                int(first[index])
                dt = Column.DATATYPE_INTEGER
            except:
                dt = Column.DATATYPE_TEXT
            
            yield Column(name=name,table_name=tname,datatype=dt)
        
        
    def prepare(self):
        self.schema.generate()
    
    ### Build the final package

    def pre_build(self):
        return True
        
  
    def build(self):
        with self.downloader() as f:
            
            file_ = open(f, 'rb')
            reader = csv.reader(file_)
     
            # The header map and uniques dicts keep track of the non-fact values
            # so we can make dimension tables. 
            header = reader.next()
            col_name_to_pos = { value: index for index, value in enumerate(header) } 
            col_pos_to_name = { index : value for index, value in enumerate(header) } 

            ins = self.database.inserter('metro_gdp')

            for row in reader:

                if len(row) != len(header):
                    break # exclude notes at end. 

                ins.insert(row)
               
               
        return True;

    def build_fact_dimension(self):
        '''Version of build that creates fact and dimension tables. ''' 
        
        with self.downloader() as f:
            
            file_ = open(f, 'rb')
            reader = csv.reader(file_)
     
            # The header map and uniques dicts keep track of the non-fact values
            # so we can make dimension tables. 
            header = reader.next()
            col_name_to_pos = { value: index for index, value in enumerate(header)} 
            col_pos_to_name = { index : value for index, value in enumerate(header)} 
            uniques = { index : set() for index, value in enumerate(col_pos_to_name.keys())}   

            import pprint
            pprint.pprint(header)

            i  =0;
            for row in reader:

                if len(row) != len(header):
                    break # exclude notes at end. 

                # Build the uniques map. 
                for index, value in enumerate(row):
                    try:
                        int(col_pos_to_name[index]) # skip if column name is a year
                        # From here, only deal with year columns
                        try:
                            int(value)
                        except:
                            # Only collect the non number values, such as (D), 
                            # (L), which are for notes. 
                            uniques[index].add(row[index].strip())
                        
                    except ValueError:
                        # From Here, deal with non-year columns
                        uniques[index].add(row[index].strip())
                        

            # Now turn the sets in the unique map to real maps. 
            for index, values in uniques.items():
                uniques[index] = { orig_val : index for index, orig_val in enumerate(values)}
                
                # For the year coluns, columns, invert the value to be negative, 
                # so it can be distinguished from the real, positive values. 
                try:
                    int(col_pos_to_name[index])
                    uniques[index] = { orig_value : -1-new_value for orig_value, new_value in uniques[index].items() }
                    
                except ValueError as e:
                    pass
          
          
            import pprint
            pprint.pprint(uniques)
            
          
            # ok, now we have to go back through and translate the fact portion to the
            # dimension table. 
            file_.seek(0)
            reader.next() # skip header
            for row in reader:
                
                if len(row) != len(header):
                    break
    
                for index, map_ in uniques.items():
                    row[index] = map_.get(row[index].strip(), row[index].strip())


    
    def post_build(self):
        return True
    
 
   
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    