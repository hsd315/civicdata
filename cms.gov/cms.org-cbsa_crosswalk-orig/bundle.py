'''

'''

from  databundles.bundle import BuildBundle
 
class Bundle(BuildBundle):
    ''' '''
 
    header = ['county','state','ssacd','fipscd','cbsa','name']
    types  = [str,      str,    int,    int    , int,   str]
    
    header05 = ['county','state','ssacd','fipscd','oldmsa','oldname', 'newmsa','newname']
    types05   = [str,      str,    int,    int,     int,     str,      int,      str  ]
    
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)


    def prepare(self):
        from databundles.orm import Column
        
        if not self.database.exists():
            self.database.create()
        
        if len(self.schema.tables) == 0:
            for key, url in self.config.build.urls.items(): 
                self.log("Creating table for {} ".format(key))
                if key == 'FY_05':
                    cols = dict(zip(self.header05,  self.types05))
                else:
                    cols = dict(zip(self.header,  self.types))
            
                t = self.schema.add_table(key)
                
                for name,type in cols.items():
                    self.schema.add_column(t,name,
                                    datatype = Column.DATATYPE_TEXT if type == str else Column.DATATYPE_INTEGER)
    
            self.database.session.commit()
            self.schema.create_tables()
            
        return True
        
    def import_csv(self, url):
        '''Download a URL, look for a file with a .txt extention, 
        and load it into a PETL table'''
        from databundles.partition import PartitionIdentity
        import petl.fluent as petlf

        zip_file = self.filesystem.download(url)
        
        for f in self.filesystem.unzip_dir(zip_file):
            self.log("Examining zip file {}".format(f))
            if f.endswith('.txt'):
                t = petlf.fromcsv(f, delimiter="\t")
                t = t.sort()
                self.log("Processing as tsv")
         
                
            elif f.endswith('.csv'):
                self.log("Processing as csv")
                t = petlf.fromcsv(f)

            else:
                continue

            return t
        
        
    def build(self):
        import petl.fluent as petlf;
        
        urls = self.config.build.urls

        arrays = []
        for key, url in urls.items():  
            self.log("Processing {}".format(key))  
            t = self.import_csv(url)
            
            if t:
                
                if key == 'FY_05':
                    t = (t.setheader(self.header05)
                         .convert(dict(zip(self.header05, self.types05))))
                else:
                    t = (t.setheader(self.header)
                         .convert(dict(zip(self.header,  self.types)))
                         .cutout('name')
                         )                 
         
                table = self.schema.table(key)
                
                with self.database.inserter(self.schema.table(key)) as ins: 
                    for row in t.records():
                        row['county'] = row['county'].decode('latin1')
                        if 'newname' in row:
                            row['newname'] = row['newname'].decode('latin1')
                            
                        if 'oldname' in row:
                            row['oldname'] = row['oldname'].decode('latin1')
                        
                        ins.insert(row)
            else:
                self.error("Failed Processing {}".format(key))  

        
        return True
  
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
    