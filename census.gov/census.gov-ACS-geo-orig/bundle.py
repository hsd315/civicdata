'''

@author: eric
'''
from  databundles.bundle import BuildBundle
import os.path
import yaml
 
class Bundle(BuildBundle):
    '''
    Bundle code for US 2010 Census geo files. 
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        bg = self.config.build
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.states_file =  self.filesystem.path(bg.statesFile)

  
        
    def prepare(self):
        '''Scrape the URLS into the urls.yaml file and load all of the geo data
        into partitions, without transformation'''
        from databundles.partition import PartitionIdentity
      
        if not self.database.exists():
            self.database.create()
     
        if len(self.schema.tables) == 0 and len(self.schema.columns) == 0:
            self.log("Loading schema from file")
            with open(self.geoschema_file, 'rbU') as f:
               self.schema.schema_from_file(f)           
        else:
            self.log("Reusing schema")

        # Create the All partition now. The others are created after we know what
        # summary levels we have. 

        for time, url in self.config.build.urls.items():

            pid = PartitionIdentity(self.identity, table='geofile', time=str(time) )
            partition = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not partition:
                partition = self.partitions.new_partition(pid)
                partition.create_with_tables('geofile')          
        
        self.database.session.commit()
        
        return True
 
    def generate_geofiles(self):
        '''Generate a series of local geofiles, for each of the summary state and national
        files on the census.gov servers. '''
        import re, yaml
        
        with open(self.states_file, 'r') as f:
            states =  yaml.load(f)      
        
        for time_id, url_template in self.config.build.urls.items():
            
            for stateabr, state in states.items():
                
                
                if  isinstance(state, basestring):
                    state = [state]
                    
                
                for s in state:
                    url = url_template.format(stateabr=stateabr, state=s)
                    try:
                        exc = None
                        file = self.filesystem.download(url)
                        break
                    except Exception as e:
                        self.error("Failed to download {}:{}".format(url, e))
                        exc = e
            
                if exc is not None:
                    raise exc
            
                yield stateabr, time_id, file;
                                    
        return 
 

    def build(self):
        from databundles.partition import PartitionIdentity
        import time
        import re
        import pprint
        import csv

        table = self.schema.table('geofile')

        header = [ c.name for c in table.columns]

        url = self.config.build.urls[20105];

        zip_file = self.filesystem.download(url)
        
        files = self.filesystem.unzip_dir(zip_file);
        
        partition = self.partitions.find(PartitionIdentity(
                            self.identity, table='geofile', time=str(20105) ))
        
        row_i = 0
        for file in files:
            
            if not file.endswith('.csv'):      
                continue
            
            self.log("Opening "+file)
            
            with open(file, 'rbU', buffering=1*1024*1024) as gf:
                with partition.database.inserter(partition.table) as ins: 

                    for row in csv.reader(gf):
         
                        if row_i == 0:
                            t_start = time.time()      
                
                        row_i += 1
            
                        if row_i % 150000 == 0:
                            # Prints the processing rate in 1,000 records per sec.
                            self.log(str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ") 

                        row = [ v.strip() for v in row]

                        row = dict(zip(header,row))

                        if 'name' not in row:
                            self.error("Bad file: "+file_name)
                            pprint.pprint(row)
                            
                        row['name'] = row['name'].decode('latin1') # The Puerto Rico files has 8-bit names

                        ins.insert(row)   
         
        return True
  
    def load_individual_files(self):
        from databundles.partition import PartitionIdentity
        import time
        import re
        import pprint
        import csv

        table = self.schema.table('geofile')

        header = [ c.name for c in table.columns]

        row_i = 0

        for stateabr, time_id, file_name in self.generate_geofiles():

            partition = self.partitions.find(PartitionIdentity(
                            self.identity, table='geofile', time=str(time_id) )) 

            with open(file_name, 'rbU', buffering=1*1024*1024) as gf:
                with partition.database.inserter(partition.table) as ins: 

                    for row in csv.reader(gf):
         
                        if row_i == 0:
                            t_start = time.time()      
                
                        row_i += 1
            
                        if row_i % 150000 == 0:
                            # Prints the processing rate in 1,000 records per sec.
                            self.log(stateabr+" "+" "+str(time_id)+" "+
                                     str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ") 

                        row = [ v.strip() for v in row]

                        row = dict(zip(header,row))

                        if 'name' not in row:
                            self.error("Bad file: "+file_name)
                            pprint.pprint(row)
                            
                        row['name'] = row['name'].decode('latin1') # The Puerto Rico files has 8-bit names

                        ins.insert(row)   
    def install(self):  
     
        return False
     
        self.log("Install bundle")  
        dest = self.library.put(self)
        self.log("Installed to {} ".format(dest[2]))
        
        for partition in self.partitions:
        
            self.log("Install partition {}".format(partition.name))  
            dest = self.library.put(partition)
            self.log("Installed to {} ".format(dest[2]))

        return True
        
import sys

if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
