'''

@author: eric
'''
from  databundles.bundle import BuildBundle
import os.path
import yaml
from databundles.library import get_library
 
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
        import re, csv, collections
      
      
        if not self.database.exists():
            self.database.create()

        geo_file = self.filesystem.path(self.config.build.geoschemaFile)
     
        if len(self.schema.tables) == 0 and len(self.schema.columns) == 0:
            self.log("Loading schema from file")
            with open(geo_file, 'rbU') as f:
               self.schema.schema_from_file(f)           
        else:
            self.log("Reusing schema")

        self.schema.create_tables()

        self.database.session.commit()
        
        return True
    
    def build(self):

        source1 = self.library.dep('geo2000')
        source2 = self.library.dep('geo2010')

        template = self.config.queries.template
        data = self.config.queries.data
        
        
        for sumlev, table_name, name_remove in data:
            
            table = self.schema.table(table_name)

            sp1 = source1.bundle.partitions.find(grain=sumlev)
            sp2 = source2.bundle.partitions.find(grain=sumlev)

            s1 = self.database.attach(sp1, 'source1')
            s2 = self.database.attach(sp2, 'source2')

            s1_fields = [c.name for c in table.columns if c.data.get('in2000', False) 
                         and c.name not  in ('name', 'geoid') and not c.name.endswith('_id')]
            s2_fields = [c.name for  c in table.columns if c.name not in s1_fields 
                         and c.name not  in ('name', 'geoid') and not c.name.endswith('_id')]

            fields = s1_fields + s2_fields
            s1_fields = ['s1.'+c for c in s1_fields]
            s2_fields = ['s2.'+c for c in s2_fields]
            
            joins = [ "s1.{0} = s2.{0}".format(c.name)  
                       for  c in table.columns if c.data.get('join',False)]

            select1 = [ c.name if c.data.get('in2000',False) else 'Null as {}'.format(c.name)
                        for c in table.columns if c.name not  in ('name', 'geoid') and not c.name.endswith('_id')]
            select1.append("trim(replace(name,'{name_remove}','')) as name1".format(name_remove=name_remove));
            
            select2 = [ c.name if c.data.get('in2010',False) else 'Null as {}'.format(c.name)
                        for c in table.columns  if c.name not  in ('name', 'geoid') and not c.name.endswith('_id')]
            select2.append("trim(replace(name,'{name_remove}','')) as name2".format(name_remove=name_remove));


            q = template.format(fields=','.join(s1_fields+s2_fields+ ['name1', 'name2']), 
                                select1=','.join(select1),
                                select2=','.join(select2),
                                s1=s1, s2=s2,
                                sumlev=sumlev, 
                                joins=' AND '.join(joins))


            print q

            fields = fields + ['name']
            
            with self.database.inserter(table_name, replace=True) as ins:
                for row in  self.database.connection.execute(q):
                    
                    row = list(row)

                    name1 = row.pop()
                    name2 = row.pop()
                    
                    if name2:
                        name = name2
                    elif name1:
                        name = name1
                    else:
                        raise Exception("Missing Name")
                    
                    row.append(name)

                    drow = dict(zip(fields, row))
                    drow[table_name+'_id'] = None
                    drow['geoid'] = None
                 
                    ins.insert(drow)
        
            self.database.detach(s1)
            self.database.detach(s2)

        return True

    def load(self, year, config ):
        from databundles.identity import PartitionIdentity
        
        year_suffix = str(year)[2:]
        
        ds = get_library().get(config.source) 
        qt = config.template
          
        for qd in config.data:
            table_name = str(qd[1])+year_suffix
            
            pid = PartitionIdentity(self.identity, grain=qd[1])
            dp = self.partitions.find(pid) # Find puts id_ into partition.identity
            
            if not dp:
                dp = self.partitions.new_partition(pid)
                dp.database.create(copy_tables = False)
                #dp.create_with_tables() 
            
            sp = ds.bundle.partitions.find(grain=qd[0])
            print sp.identity.name, sp.database.path
            
            with dp.database.inserter(table_name) as ins:
                q = qt.format(*qd)
                print q
                for row in  sp.database.session.execute(q):
                    geo_id = 'foo'
                    ins.insert((None, geo_id)+tuple(row))
    
    def make_generated_geo(self):
        geo_file = self.filesystem.path(self.config.build.gengeoFile)
        with open(geo_file, 'w') as f:
            import csv
            writer = csv.writer(f)
            writer.writerow(['table','column','is_pk','type'])
            for year, cfg in  self.config.queries.items():
                self.make_generated_geo(writer, cfg.source, cfg.template, cfg.data, year)
             

    def _make_generated_geo(self, writer, dataset_name, template, data, year):
        """Create a schema file for the queries to extract data from the 
        source partition. 
        
        This method executed the queries defined in the 'meta' configuration, 
        then creates a schema for that query, based on the first row. 
        """
        from databundles.identity import PartitionIdentity
        
        ds = get_library().get(dataset_name) 
        year_suffix = str(year)[2:]
        
        for qd in data:
            
            table_name = str(qd[1])+year_suffix
            writer.writerow([table_name,qd[1]+'_id', 1, 'INTEGER'])
            writer.writerow([table_name,'geoid', None, 'TEXT'])
             
            # Source partition          
            partition = ds.bundle.partitions.find(grain=qd[0])
            
            if not partition: 
                raise Exception("Failed to get partition for grain {} from dataset {}"
                                .format(qd[0], dataset_name))
            
            row =  partition.database.session.execute(template.format(*qd)).first()
            
            for k,v in zip(row.keys(), row):
                
                try:
                    int(v)
                    type = "INTEGER"
                except:
                    type = "TEXT"
                    
                if k in ('name'):
                    type = 'INTEGER'
                    
                writer.writerow([table_name, str(k),None,type])
                        

    
import sys

if __name__ == '__main__':
    import databundles.run
    #import cProfile 

    #cProfile.run('databundles.run.run(sys.argv[1:], Bundle)')
    databundles.run.run(sys.argv[1:], Bundle)
    
