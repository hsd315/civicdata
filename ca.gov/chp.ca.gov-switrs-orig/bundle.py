'''
'''

from  databundles.bundle import BuildBundle
 
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(self.__class__, self)
        self.super_.__init__(directory)
 
    def build(self):
        
        from csv import DictReader
        from datetime import date, time
        from pprint import pprint
        from sqlalchemy.exc import IntegrityError

        p = self.partitions.find_or_new(table='collisions', tables=['collisions','parties', 'victims'])

        for table, url in self.config.build.urls.items():

            zip_file = self.filesystem.download(url)
            
            file_name = self.filesystem.unzip(zip_file)
            
            lr = self.init_log_rate(25000,table)

            with open(file_name) as f:
                dr = DictReader(f)
                
                p.query("DELETE FROM {}".format(table))
                
                with p.database.inserter(table, replace=True) as ins:
                    for i, uc_row in enumerate(dr):
                    
                        row = { k.lower():v for k,v in uc_row.items() }
                        
                        if table == 'collisions':

                            ct = row['collision_time']
                            row['collision_time'] = time(int(ct[0:2] ) % 24, int(ct[2:]))
                            
                            cd = row['collision_date']
                            row['collision_date'] = date(int(cd[0:4]), int(cd[4:6]), int(cd[6:8]))

                            cd = row['proc_date']
                            row['proc_date'] = date(int(cd[0:4]), int(cd[4:6]), int(cd[6:8]))

                            row['latitude'] = row['latitude'] if row['latitude'] else None
                            row['longitude'] = row['longitude'] if row['latitude'] else None
                            

                        ins.insert(row)

                        lr()

        return True

    def process_codes(self):
        
        from csv import reader
        import re
        
        pat = re.compile(r'\s*\-\s*')
        
        links = {}
        
        with open(self.filesystem.path('meta','codes.csv')) as f:
            title = None
            for row in reader(f):
                
                if row[0] == 'L':
                    # assign a link
                    links[row[0].strip().lower()] = row[1].strip().lower()
                    continue

                if len(row) < 2 or (not row[2] and not row[1]):
                    continue

                field = row[1]
                content = row[2]
                   
                parts = re.split(pat, content, 1)
                
                if field:
                    title = field
                
                if "not stated" in content.lower() or 'blank' in content.lower():
                    value = '-'
                    term = 'N/A'
                else:
                    value = parts.pop(0).decode("utf-8").encode('ascii', errors='ignore')
                    try: term = parts.pop(0)
                    except:
                        print "ERR ", value, parts, row
                 
                print "{:40s}{:4s}{:10s}".format(title, value,  term)

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)  
    