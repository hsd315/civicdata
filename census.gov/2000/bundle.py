'''
Bundle build file for 2000 US Census, Summary file 1

Created on Jun 10, 2012

@author: eric
'''

from  databundles.bundle import Bundle as Base
import os.path  

class Bundle(Base):
    '''
    Bundle code for US 2000 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        '''Constructor'''
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        
        
       
    def prepare(self):
        '''Create the prototype database'''
       

        header_file = self.config.group('build').get('headers')
       
        self.config.add_file(header_file)

        self.log("Before: "+str(len(self.schema.tables))+' tables in database')
        self.log("Before: "+str(len(self.schema.columns))+' columns in database')
        
        if (len(self.schema.tables) != 286 or 
            len(self.schema.columns) != 9543 or
            self.filesystem.ref(header_file).changed):
            self.log("Regenerating schema")
            self.schema.generate()
        else:
           self.log("Re-using schema")
      
        
        self.log("After : "+str(len(self.schema.tables))+' tables in database')
        self.log("After : "+str(len(self.schema.columns))+' columns in database')
      
        # Create the manifast file if it does not exist. 
        ##urls = self.get_manifest()  
    
        self.log('Before '+str(self.partitions.count)+' partitions in database')
      
        if self.partitions.count < 50:
            self.partitions.generate()
        else:
            self.log("Skipping building partitions")
            
        self.log('After '+str(self.partitions.count)+' partitions in database')
            
        return True
       
    def schemaGenerator(self):
        '''Return schema rows from the  columns.csv file'''
    
        from databundles.orm import Table, Column
        import csv
        
        header_file = self.filesystem.path(self.config.group('build').get('headers'))
        reader  = csv.DictReader(open(header_file, 'rbU') )
      
        last_seg = None
        source_col = 5 # Offset for  common columns
        i = 0
        for row in reader:
            if not row['TABLE']:
                continue
      
            # Keep track of the column number for each segment file, resetting it when
            # the seg value changes. 
            if row['SEG'] and row['SEG'] != last_seg:
                    source_col = 5
                    last_seg = row['SEG']
      
            # The first two rows for the table give information about the title
            # and population universe, but don't have any column info. 
            if( not row['FIELDNUM']):
                if  row['TABNO']:
                    table = Table(name=row['TABLE'],description=row['TEXT'])
                else:
                    table.universe = row['TEXT'].replace('Universe:','').strip()  
            else:
                
                # The whole table will exist in one segment ( file number ) 
                # but the segment id is not included on the same lines ast the
                # table name. 
                if table:
                    table.data = {'segment':row['SEG']}
                    yield table
                     
                    # First 5 fields for every record      
                    # FILEID           Text (6),  uSF1, USF2, etc. 
                    # STUSAB           Text (2),  state/U.S. abbreviation
                    # CHARITER         Text (3),  characteristic iteration, a code for race / ethic group
                    #                             Prob only applies to SF2. 
                    # CIFSN            Text (2),  characteristic iteration file sequence number
                    #                             The number of the segment file             
                    # LOGRECNO         Text (7),  Logical Record Number
              
                    tn = row['TABLE']
                    dt = Column.DATATYPE_INTEGER
                    seg = row['SEG']
              
                    yield Column(name='FILEID',table_name=tn,datatype=dt,
                                 data={'source_col':0,'segment':seg})
                    yield Column(name='STUSAB',table_name=tn,datatype=dt,
                                 data={'source_col':1,'segment':seg})
                    yield Column(name='CHARITER',table_name=tn,datatype=dt,
                                 data={'source_col':2,'segment':seg})
                    yield Column(name='CIFSN',table_name=tn,datatype=dt,
                                 data={'source_col':3,'segment':seg})
                    yield Column(name='LOGRECNO',table_name=tn,datatype=dt,
                                 data={'source_col':4,'segment':seg})
                    
                    table = None

                if row['DECIMAL'] and int(row['DECIMAL']) > 0:
                    dt = Column.DATATYPE_REAL
                else:
                    dt = Column.DATATYPE_INTEGER
                
                yield Column(name=row['FIELDNUM'],table_name=row['TABLE'],
                             description=row['TEXT'].strip(),
                              datatype=dt,data={'segment':int(row['SEG']),
                                                'source_col':source_col}   )
                
                source_col += 1
               
    def table_seg_map(self):
        """Create a lookup table mapping segments to tables. 
           There are multiple tables per file."""
        from databundles.orm import Table
        
        s = self.database.session
        table_seg = {}
        for table in s.query(Table).all() :
            if not table.data.get('segment',None):
                raise ValueError("Should have gotten a segment data for table")
            
            seg = int(table.data['segment'])
            
            if not  seg in table_seg:
                table_seg[seg] = []
                
            table_seg[seg].append(table.name.encode('ascii'))
            
        return table_seg
   
    def partitionGenerator(self):
        '''Get all of the URLs of the zip files to download and create partitions
        
        This method will walk the census website to get references to all of 
        the zip files and download them. Then it creates partition records 
        that reference the zip files. There are three types of partitions: 
          
        geo: for the state geo files. 
        build: partitions with space and table elements, for creating .csv files
        deploy: partitions with only the table elements, for generating databases
        
        '''
        import urllib
        import urlparse
        import yaml
        import re
        from bs4 import BeautifulSoup
        from databundles.partition import Partitions, Partition, PartitionId
        import sys

        downloadDir = 'downloads'
   
        i = 0;
   
        states_file = self.filesystem.path(self.config.group('build').get('statesFile'))
   
        # Load in a list of states, so we know which links to follow
        with open(states_file) as f:
            states = map(lambda s: s.strip(),f.readlines())
        
        # Root URL for downloading files. 
        url = self.config.group('build').get('rootUrl')
       
        doc = urllib.urlretrieve(url)
        
        self.log('Getting URLS from '+url)
        # Get all of the links
        partitions = {}
        for link in BeautifulSoup(open(doc[0])).find_all('a'):
            
            if not link.get('href') or not link.string or not link.contents:
                continue;# Didn't get a sensible link
            # Only descend into links that name a state
            state = link.get('href').strip('/')
            if link.string and link.contents[0] and state in states :
                stateUrl = urlparse.urljoin(url, link.get('href'))
                stateIndex = urllib.urlretrieve(stateUrl)
                # Get all of the zip files in the directory
                for link in  BeautifulSoup(open(stateIndex[0])).find_all('a'):
                    if link.get('href') and  '.zip' in link.get('href'):
                        final_url = urlparse.urljoin(stateUrl, link.get('href')).encode('ascii', 'ignore')
                        path = urlparse.urlparse(final_url).path
                      
                        data={'source_url':final_url,
                              'zip_file':os.path.join(downloadDir+'/'+os.path.basename(path))}
                        
                        if 'geo_uf1' in final_url:
                            state = re.match('.*/(\w{2})geo_uf1', final_url).group(1)
                            self.ptick('G')
                            yield Partition(self, PartitionId(table='SF1GEO'), data=data)
                         
                        else:
                            m = re.match('.*/(\w{2})(\d{5})_uf1', final_url)
                            state,segment = m.groups()
                            segment = int(segment.lstrip('0'))

                            # Create a table partition for every table in the segment. 
                            for table in self.table_seg_map().get(segment):
                                
                                # These partitions are fo colleting csv files, not
                                # for constucting databases
                                self.ptick('p')
                                yield Partition(self,  
                                                PartitionId(table=table, space=state), 
                                                data=data,
                                                state='build'
                                                )
                                
                                # Yield one for the the whole table, regarless of which space partition 
                                # This is one we will actually build a database for
                                p_id = PartitionId(table=table)
                                
                                if str(p_id) not in partitions:
                                    partitions[str(p_id)] = True
                                    self.ptick('P')
                                    yield  Partition(self,p_id,data = data,state='deploy')
 
    def build(self):
        self.load_partitions()
        
        #db =  pr.database
        #metadata, at = self.schema.get_table_meta(p.table.name)
        #print CreateTable(at)
        #metadata.create_all(bind=db.engine)
        #vi = db.inserter(p.table.name)
                
    
        return True
    
    def load_partitions(self):
        from databundles.orm import Partition as OrmPartition
    
        for deploy in self.partitions.query.filter(OrmPartition.state=='deploy').all():
            print deploy.id_
            
            deploy_partition = self.partitions.partition(deploy)
            db =  deploy_partition.database
            metadata, at = self.schema.get_table_meta(deploy.table.name)
            #print CreateTable(at)
            metadata.create_all(bind=db.engine)
            #vi = db.inserter(p.table.name)
            
            q = self.partitions.query.filter(
                                            OrmPartition.state=='build',
                                            OrmPartition.t_id == deploy.t_id
                                            )
            if q.count() != 52:
                raise Exception("Bad count:"+str(q.count))
            
            for build in q.all():
                print '    ',build.id_
                
                table = deploy.table
                f = self.filesystem.build_path('states',build.space,table.name,build.id_+".csv")
                # Loading a CSV file into sqlite is really easy, and this is probably much faster. 
                cmd = "/usr/bin/sqlite3 -csv {} '.import {} {}' ".format(db.path, f, table.name)
                print cmd
                os.system(cmd)
                
    
    def repartition_csv(self):
        '''Walk the partitions records to create csv files for each 
        'build' partition. 

        The method builds a set of data from all partitions, collecting all of the
        source files and linking them to an array of partitions that use that
        source. After downloading the source files, it will create .csv files
        for all of the linked partitions. 
        
        '''
        import urllib, yaml, pprint, curses.ascii
        from databundles.orm import Partition as OrmPartition
        
        i = 0
        
        # First, invert the relationship between source_urls and
        # partitions, grouping all of the partitions under the source_url
        # that has the data for the partions. 

       
        iset = {} 
        # We're just getting the 'build' partitions here. THese partitions
        # break out both table and state. We'll create CSV files for those, 
        # then combine them into the database in a more efficient manner
        for p in self.partitions.query.filter(OrmPartition.state=='build').all():

            partition = self.partitions.partition(p);      

            if p.data['source_url'] not in iset:
                iset[p.data['source_url']] = []
            
            iset[p.data['source_url']].append(p)
       
            #self.progress(p.id_)
        
        
        # Now, download each of the source files and process them.         

        for source,partitions in iset.items():
            import zipfile, re
            
            zip_file = self.filesystem.path(partitions[0].data['zip_file'])
 
            retry = 4
            while retry > 0:
                retry -= 1
                try:               
                    if not os.path.exists(zip_file):
                        self.log("Download: "+source)
                        urllib.urlretrieve(source, zip_file)
                    else:
                        self.log("Skip:   :"+zip_file+" exists")

                    with self.extract_zip(zip_file) as f:
                
                        if 'geo.uf1' in f:
                            self.log("Process geo: "+f)
                            ##self.load_geo(path, geo_regex)
                            pass
                        elif re.match('.*/\w{2}\d{5}.uf1', f):
                            self.log("Process tables: "+f)
                            self.load_table(f, partitions)
                            pass
                        else:
                            raise ValueError, 'Bad path: '+path
                        
                    retry = 0
                except zipfile.BadZipfile:
                    self.log("ERROR: Failed to get valid zip file from "+zip_file)
                    self.log("Retry")
                    if os.path.exists(zip_file):
                        os.remove(zip_file)


        return True
  
    def load_table(self, path, partitions):
        '''For a set of partitions and a path to a zip file, break the
        data in the zip file into seperate partition files. ''' 
        import re, pprint, copy, os, csv, pprint
        from sqlalchemy.schema import CreateTable
        import csv
         
        state, number = re.match('.*/(\w{2})(\d{5}).uf1', path).groups()
        number = int(number)

        # Map the Partition Database records to the Partition
        # references
        p_map = {}
        for p in partitions:  
            table = p.table
            f = self.filesystem.build_path('states',p.space,table.name,p.id_+".csv")
            p_map[p.id_] = { 'partition': self.partitions.partition(p),
                             'writer':csv.writer(open(f, 'wb')) }

        import time
        start_time = time.clock()
        count = 0
        
        # Build a range map, which has the first and last columns from the rows for
        # each of the partition tables. 
        tv = {}
        for p in partitions:  
            start =   None
            for column in p.table.columns:         
                if p.table.id_ not in tv:
                    tv[p.table.id_] = []
                  
                if column.data['source_col'] >= 5 and start is None:
                    start = column.data['source_col']
                             
            tv[p.table.id_] = [start, column.data['source_col']+1]
 
        # Now, for each row, we can use the range map to take slides of columns
        # and write those to csv file partitions. 
        for row in csv.reader(open(path, 'rbU') ):
           
            # Pull off the common fields. 
            common = row[:5]
    
            for p in partitions: 
                r =  tv[p.table.id_]
                partition_row = common + row[r[0]:r[1]]
                p_map[p.id_]['writer'].writerow(partition_row)
                count += 1

        print "{} records per second {}".format(count/(time.clock()-start_time), int(time.clock()-start_time))
  
    def get_geo_regex(self):
        '''Read the definition for the fixed positioins of the fields in the geo file and
        construct a regular expresstion to parse the lines.'''
        import csv, re
                
        def_file = self.filesystem.path(self.config.group('build').get('geoheaderFile'))

        reader  = csv.DictReader(open(def_file, 'rbU') )
        pos = 0;
        regex = ''
        for row in reader:
            start = int(row['start']) - 1
            pos += int(row['length'])
        
            regex += "(?P<{}>.{{{}}})".format(row['column'],row['length'])
            
        print regex
       
        return re.compile(regex)
            
    def load_geo(self, path, regex):
        import re, pprint, copy
        
        state = re.match('.*/(\w{2})geo.uf1', path).group(1)
     
        # Copy the bundle so we change the partition, to create the correct partition name
        bundle = copy.deepcopy(self)
        bundle.partition.space = state
    
        d = bundle.database()
        con = d.engine.connect()
          
        with open(path) as f:
            con.begin()
            print "Loading geo file "+ path
            for line in f:
                m = regex.match(line)
                if m:
                    values = {k:v.strip() for k,v in m.groupdict().iteritems()}
                    ins = d.table('SF1GEO').insert().values(values)
                    con.execute(ins)
                    
                else:
                    con.rollback()
                    raise ValueError
            con.commit()




import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
    
    