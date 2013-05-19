'''
'''

from  databundles.bundle import BuildBundle
from  databundles.orm import Table, Column
from databundles.dbexceptions import ConfigurationError
import os.path
import logging
from numpy import *
class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self, directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
        self.logger.setLevel(logging.DEBUG) 
  
    def prepare(self):
        from databundles.partition import PartitionIdentity
        
        self.database.delete()
        self.database.create()
        
        s = self.schema 
        s.clean()

        t1 = s.add_table('example')
                
        s.add_column(t1,'rand1',datatype=Column.DATATYPE_INTEGER, indexes='index1')
        s.add_column(t1,'rand2',datatype=Column.DATATYPE_REAL, indexes='index1')
        s.add_column(t1,'rand3',datatype=Column.DATATYPE_INTEGER, indexes='index1')
        s.add_column(t1,'rand4',datatype=Column.DATATYPE_REAL, indexes='index1')
        s.add_column(t1,'rand5',datatype=Column.DATATYPE_INTEGER, indexes='index1')
        s.add_column(t1,'rand6',datatype=Column.DATATYPE_REAL, indexes='index1')
        s.add_column(t1,'uuid',datatype=Column.DATATYPE_TEXT)
        s.add_column(t1,'tag',datatype=Column.DATATYPE_TEXT)
        s.add_column(t1,'flags',datatype=Column.DATATYPE_TEXT, indexes='index2')
        
        self.database.session.commit()
        
        spaces = ['space1', 'space2', 'space3', 'space4' ]
        times = ['time1','time2', 'time3','time4']
        
        spaces = ['space1','space2' ]
        times = ['time1','time2']
        
        table = self.schema.table('example')
        
        for space in spaces:
            for time in times:
                pid = PartitionIdentity(self.identity, time=time, space=space, table=table.name)
                partition = self.partitions.new_partition(pid)
                partition.database.session.commit()
                self.log('Creating: '+partition.identity.name)
                partition.create_with_tables()
                        
        return True

    ### Build the final package

       
    def build(self):
        '''Create a table full or random data'''
        import random
        import uuid
        from databundles.partition import PartitionIdentity
        
    
        tags = ['one','two','three','pizza','unicorn']
        flags = ['a','b','c','d','e']
        
        # Create data in the main bundle, using the inserter
        with self.database.inserter('example') as ins:
            for i in range(1,1000):
                ins.insert([
                       random.randint(1,10000),
                       random.random()*1000,
                       random.randint(1,10000),
                       random.random()*1000,
                       random.randint(1,10000),
                       random.random()*1000,
                       str(uuid.uuid4()),
                       random.choice(tags),
                       random.choice(flags)+random.choice(flags)
                       ])

        # make a few partitions

        
        for partition in self.partitions:

            import time
            t_start = time.time()
            with partition.database.inserter('example') as ins:
                self.log('Building partition: {}'.format(partition.identity.name))
                for i in range(1,5000):
                    ins.insert([
                           random.randint(1,10000),
                           random.random()*1000,
                           random.randint(1,10000),
                           random.random()*1000,
                           random.randint(1,10000),
                           random.random()*1000,
                           str(uuid.uuid4()),
                           random.choice(tags),
                           random.choice(flags)+random.choice(flags)
                           ])

                    if i % 1000 == 0:
                        # Prints the processing rate in 1,000 records per sec.
                        self.log(str(int( i/(time.time()-t_start)))+'/s '+str(i/1000)+"K ")
            partition.database.close()

        return True
       

    def test_extract(self, data, file_name=None):
        print "EXTRACT: {}".format(data['description'])
        file_name = data['file_name']
        with open(file_name,'w+') as f:
            f.write('hi there\n')
            
        return file_name

       
    def image_extract(self,file_name=None):
        '''Collect the street_lights into a heat map. '''
        from databundles.identity import PartitionIdentity
        from databundles.geo.density import DensityImage, LinearMatrix,GaussianMatrix
        
        import random
        from databundles.geo.util import BoundingBox
        
        
        file_ = self.filesystem.path('extracts',"test1.tiff")
        file2_ = self.filesystem.path('extracts',"test2.tiff")
        file3_ = self.filesystem.path('extracts',"test3.tiff")
    
        bin_scale = 100 # cells per degree
        matrix_size = 17
        matrix_dia = 9

        extents = BoundingBox(min_x=-1, min_y=-1, max_x=1, max_y=1)
        x_size = (extents.max_x - extents.min_x) * bin_scale
        y_size = (extents.max_y - extents.min_y) * bin_scale
        
        print "Matrix: ", matrix_size, matrix_dia
        print "Size  : ",x_size, y_size
        m = GaussianMatrix(matrix_size,matrix_dia)
        di = DensityImage(extents, bin_scale,m)
        
        m = GaussianMatrix(int(matrix_size/2)+1,int(matrix_dia/2)+1)
        di2 = DensityImage(extents, bin_scale,m)
        
        di3 = DensityImage(extents, bin_scale)
        
        print di.info()   
        

        for x in arange(extents.min_x, extents.max_x ,1./bin_scale*20,  float32):
            for y in arange(extents.min_y, extents.max_y ,1./bin_scale*20,  float32):
                di.add_matrix(x,y)
                di2.add_matrix(x,y)
                #di2.add_count(x,y, random.randint(1,10) )
    
        di.mask()
        di.unity_norm()
        
        di2.mask()
        print di2.info()
        print di.write(file_)
        di2.unity_norm()
        print di2.info()   
      
        print di2.write(file2_)
        

        di3.a = di.a - di2.a
        
        print di3.write(file3_)
        
        print self.log("WROTE: "+file_)
        return file_    


    def demo1(self):
        import databundles.library as library
        
        #r  = self.library.get("clarinova.com-us_crime_incidents-state-7ba4-ca.incidents")
        r  = self.library.get("census.gov-2010_population-geo-orig-a7d9-geofile.40")
        
        # Get the metro and non metro areas from the state file, then calculate the 
        # population densities for the different areas. 
        q= """
        SELECT me.name, me.arealand AS metro_area, me.pop100 AS metro_pop,
        ru.arealand AS rural_area, ru.pop100 AS rural_pop,
        (cast(me.pop100 as float) / me.arealand)*2590000 as metro_density,
        (cast(ru.pop100 as float) / ru.arealand)*2590000 as rural_density
        FROM geofile AS me
        LEFT JOIN geofile AS ru ON ru.state = me.state
        WHERE me.geocomp = 'C0' AND  ru.geocomp = 'G0' AND me.fileid = 'SF1ST' AND ru.fileid = 'SF1ST'
        ORDER BY metro_density DESC;
        """
        
        for row in r.partition.database.query(q):
            print row;

    def test_web(self):
        
        print self.web.schema_table('example')
        

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    