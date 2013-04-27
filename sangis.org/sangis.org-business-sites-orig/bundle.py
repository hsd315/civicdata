'''

'''

from  databundles.bundle import BuildBundle
from databundles.util import AttrDict

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)


    ### Prepare is run before building, part of the devel process.  

    def prepare(self):
        '''Create the datbase and load the schema from a file, if the file exists. '''
        from databundles.partition import PartitionIdentity
      
        if not self.database.exists():
            self.database.create()

        if self.config.build.get('schema_file', False):
            with open(self.filesystem.path(self.config.build.schema_file), 'rbU') as f:
                self.schema.schema_from_file(f)      
                self.schema.create_tables()     

        return True
    
    ### Build the final package

    def load_shapefile(self, url, table_name):
        from databundles.identity import PartitionIdentity
 
        pid = PartitionIdentity(self.identity, table=table_name)
           
        try: shape_partition = self.partitions.find(pid)
        except: shape_partition = None # Fails with ValueError because table does not exist. 
        
        if not shape_partition:
            shp_file= self.filesystem.download_shapefile(url)
            shape_partition = self.partitions.new_geo_partition( pid, shp_file)

    def build(self):

        def progress_f(i):
            if i%10000 == 0:
                self.log("Converted {} records".format(i))  

        self.load_shapefile(self.config.build.sources.addresses, 'addresses')
        self.load_shapefile(self.config.build.sources.roads, 'roads')
        self.load_shapefile(self.config.build.sources.intersections, 'intersections')
                
        self.add_indexes()
                
        return True

    def load_codes(self):
        
        codes = self.filesystem.load_yaml(self.config.build.codes)
        
        self.database.query("DELETE FROM codes")
        
        with self.database.inserter("codes") as ins:
            for group, s in codes.codes.items():
                for k, v in s.items():
                    ins.insert({
                      'group': group,
                      'key': k,
                      'value': v
                    })

    def add_indexes(self):
        
        p = self.partitions.find(table='roads')
        p.database.query('CREATE INDEX IF NOT EXISTS road_rd20name_idx ON roads (rd20name);')
        p.database.query('CREATE INDEX IF NOT EXISTS road_block_idx ON roads (l_block, r_block);')
        p.database.query('CREATE INDEX IF NOT EXISTS road_nodes_idx ON roads (fnode, tnode);')
        
        p = self.partitions.find(table='addresses')
        p.database.query('CREATE INDEX IF NOT EXISTS adr_name_idx ON addresses (addrname);')       
        p.database.query('CREATE INDEX IF NOT EXISTS adr_number_idx ON addresses (addrnmbr);')   

        p = self.partitions.find(table='intersections')
        p.database.query('CREATE INDEX IF NOT EXISTS intr_interid_idx ON intersections (interid);')       

                
    def test_geo(self):
        
        p = self.partitions.find(table='addresses')
        
        for row in p.query("select AsText(geometry) from addresses limit 5"):
            print row

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    