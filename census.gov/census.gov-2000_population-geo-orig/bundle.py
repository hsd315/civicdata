'''

@author: eric
'''
from  databundles.bundle import BuildBundle
import os.path
import yaml
 
class Bundle(BuildBundle):
    '''
    Bundle code for US 2000 Census, Summary File 2
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

        self._states = None
        
        bg = self.config.build
        self.geoschema_file = self.filesystem.path(bg.geoschemaFile)
        self.states_file =  self.filesystem.path(bg.statesFile)
        self.urls_file =  self.filesystem.path(bg.urlsFile)
        
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

        pid = PartitionIdentity(self.identity, table='geofile', grain='all')
        partition = self.partitions.find(pid) # Find puts id_ into partition.identity
        
        if not partition:
            partition = self.partitions.new_partition(pid)
            partition.create_with_tables('geofile')          
        
        self.database.session.commit()
        
        return True
 
    def generate_urls(self):
        '''  '''
        from databundles.partition import PartitionIdentity
        
        
        with open(self.states_file, 'r') as f:
            states =  yaml.load(f) 
 

        template = self.config.build.urlTemplate
 
        for index in [1,2,3,4]:
            for stateabr, state in states.items():
                url = template.format(index=index, state=state, stateabr=stateabr)
                yield stateabr, url

        nationals = self.config.build.nationalFiles

        for index, url in nationals.items():
            grain = 'sf'+str(index)+'us'
            pid = PartitionIdentity(self.identity, table='geofile', grain=grain)
            yield 'us',  url

        return 
 

    def build(self):
        from databundles.partition import PartitionIdentity
        import time

        self.load()
        
        self.split()
              
        return True
           
    def load(self):
        from databundles.partition import PartitionIdentity
        import time
        
        row_i = 0
        t_start = time.time()      
        
        #
        # Load all of the records into
        partition = self.partitions.find(PartitionIdentity(self.identity, table='geofile', grain='all')) 
        if not partition.database.exists():
            partition.create_with_tables()          
    

        with partition.database.inserter(partition.table) as ins:        
            for  state, url in  self.generate_urls():
                for row in self.generate_rows(state, url):
                    
                    row_i += 1
                
                    if row_i % 50000 == 0:
                        # Prints the processing rate in 1,000 records per sec.
                        self.log(state+" "+str(int( row_i/(time.time()-t_start)))+'/s '+str(row_i/1000)+"K ") 
               
                    if state == 'pr' or state == 'us':
                        row['name'] = row['name'].decode('latin1') # The Puerto Rico files has 8-bit names
                        
                    row = { k:v.strip() for k,v in row.items()}
                    ins.insert(row)  
                
    def split(self):
        '''Split the SF1 file into blocks and non-blocks components. This will reduce the
        size below the 5gb limit for storing files on Amazon. '''
        from databundles.partition import PartitionIdentity
        
        #
        # Generate a list of unique summary levels 
        # 
        self.log("Compile unique summary levels")
        all = self.partitions.find(PartitionIdentity(self.identity, table='geofile', grain='all'))
        n1 = self.database.attach(all);
        self.database.connection.execute('DROP TABLE IF EXISTS sumlev');
        q='CREATE TABLE sumlev AS  SELECT DISTINCT trim(sumlev) as sumlev, trim(fileid) as fileid FROM {}.geofile;'.format(n1)
        self.database.connection.execute(q);
  
        self.log("Build summary levels file table")
        self.database.connection.execute('DROP TABLE IF EXISTS slfiles');
        q="""
CREATE TABLE slfiles AS
SELECT DISTINCT  s0.sumlev, 
s1.fileid as sf1_file, s2.fileid as sf2_file, s3.fileid as sf3_file, s4.fileid as sf4_file,
s1us.fileid as sf1us_file, s2us.fileid as sf2us_file, s3us.fileid as sf3us_file, s4us.fileid as sf4us_file
FROM sumlev as s0
LEFT JOIN sumlev s1 ON s1.sumlev = s0.sumlev AND s1.fileid = 'uSF1'
LEFT JOIN sumlev s2 ON s2.sumlev = s0.sumlev AND s2.fileid = 'uSF2'
LEFT JOIN sumlev s3 ON s3.sumlev = s0.sumlev AND s3.fileid = 'uSF3'
LEFT JOIN sumlev s4 ON s4.sumlev = s0.sumlev AND s4.fileid = 'uSF4'
LEFT JOIN sumlev s1us ON s1.sumlev = s0.sumlev AND s1us.fileid = 'uSF1F'
LEFT JOIN sumlev s2us ON s2.sumlev = s0.sumlev AND s2us.fileid = 'uSF2F'
LEFT JOIN sumlev s3us ON s3.sumlev = s0.sumlev AND s3us.fileid = 'uSF3F'
LEFT JOIN sumlev s4us ON s4.sumlev = s0.sumlev AND s4us.fileid = 'uSF4F'
;
        """
        self.database.connection.execute(q);
     
        sumlevs = []
        for row in self.database.connection.execute('SELECT DISTINCT sumlev FROM sumlev'):
            sumlevs.append(row[0])
            
        
        for sumlev in sumlevs:
            self.log("Splitting summary level {}".format(sumlev))
            pid = PartitionIdentity(self.identity, table='geofile', grain=str(sumlev))  
            partition = self.partitions.new_partition(pid)
            partition.create_with_tables('geofile')    
        
            db = partition.database
            name = db.attach(all);
            
            q='INSERT INTO geofile  SELECT * FROM {}.geofile WHERE sumlev = ?'.format(name)
            db.connection.execute(q, sumlev)
            
            db.detach(name)
  
    def generate_rows(self, state, geo_source):
        '''A generator that yields rows from the state geo files. It will 
        unpack the fixed width file and return a dict'''
        import struct
        import zipfile

        table = self.schema.table('geofile')
        header, unpack_str, length = table.get_fixed_unpack() #@UnusedVariable    

        rows = 0;


        geo_zip_file = self.filesystem.download(geo_source, 'zip')

        grf = self.filesystem.unzip(geo_zip_file)

        geofile = open(grf, 'rbU', buffering=1*1024*1024)

        for line in geofile.readlines():
            
            rows  += 1
            
            if rows > 20000 and self.run_args.test:
                break

            try:
                geo = struct.unpack(unpack_str, line[:-1])
            except struct.error as e:
                self.error("Struct error for state={}, file={}, line_len={}, row={}, \nline={}"
                           .format(state,grf,len(line),rows, line))
             
            if not geo:
                raise ValueError("Failed to match regex on line: "+line) 

            yield dict(zip(header,geo))

        geofile.close()


        
import sys

if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
