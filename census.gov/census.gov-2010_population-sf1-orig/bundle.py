'''
Bundle build file for 2000 US Census, Summary file 1

Created on Jun 10, 2012

@author: eric
'''
from  sourcesupport.us2010census import Us2010CensusBundle
    
class Bundle(Us2010CensusBundle):
    '''
    Bundle code for US 2010 Census, Summary File 1
    '''

    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
    
        
    def build(self, multi_func=None):
        return False
        super(Bundle, self).build(multi_func=run_state_tables)
        
    def prepare(self):
        '''Create the prototype database'''

        if not self.database.exists():
            self.database.create()

        self.scrape_urls()
        
        self.create_table_schema()
     
        self.make_range_map()

        if not self.schema.table('sf1geo'): # Do this only once for the database
            from databundles.orm import Column
            self.schema.schema_from_file(open(self.geoschema_file, 'rbU'))
    
            # Add extra fields to all of the split_tables
            for table in self.schema.tables:
                if not table.data.get('split_table', False):
                    continue;
            
                table.add_column('hash',  datatype=Column.DATATYPE_INTEGER,
                                  uindexes = 'uihash')
        
        self.generate_partitions()
  
        
import sys

def run_state_tables(state):
    b = Bundle()
    b.log("Building (MP) fact tables for {}".format(state))
    b.run_state_tables(state)

if __name__ == '__main__':
    import databundles.run
    #import cProfile 


    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
    