'''
'''

from databundles.bundle import BuildBundle
from dateutil.parser import *
from dateutil.relativedelta import *
import osr, ogr 
from databundles.geo.util import create_bb
import datetime

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
 
    def build(self):
        import scrape

        c = self.config.build
        bbox = "xmin={}&ymin={}&xmax={}&ymax={}".format(c.xmin, c.ymin, c.xmax, c.ymax)

        print "WHERE x BETWEEN {xmin} AND {xmax} AND y BETWEEN {ymin} AND {ymax} ".format(xmin=c.xmin,xmax=c.xmax,ymin=c.ymin,ymax=c.ymax)

        srs1 = ogr.osr.SpatialReference()
        srs1.ImportFromEPSG(3395) # wgs84 / World Mercator
        
        srs2 = ogr.osr.SpatialReference()
        srs2.ImportFromEPSG(4326) # wgs84
        
        transform = osr.CoordinateTransformation(srs1, srs2)
    
        print transform.TransformPoint(c.xmin, c.ymin)
        print transform.TransformPoint(c.xmax, c.ymax)
        
        return 
    
        template = self.config.build.template
        start = datetime.date(2012,10,1)
        
        for month in range(1,7):
            
            start = start+relativedelta(months=+1)
            end = start+relativedelta(months=+1)
            end = end+relativedelta(days=-1)
            
            url = template.format(start=start.strftime('%m/%d/%Y'), end=end.strftime('%m/%d/%Y'), bbox=bbox)
            
            p = self.partitions.find_or_new(table='incidents')
         
            with p.database.inserter(cache_size = 1) as ins:
                
                print url
                
                for i, row in enumerate(scrape.scrape(url,1)):
                    if i == 0:
                        continue

                    try: dt = parse(row[5])
                    except: dt = None
                    
                  
                    try:
                        d = { 'type': row[1],
                              'blockaddress': row[3],
                              'agency' : row[4],
                              'datetime' : dt
                              }

                        ins.insert(d)
                    except:
                        print row
        
        return True

    def link(self):
        
        in1 = self.library.dep('incidents')
        
        in2 = self.partitions.find(table='incidents')
        
        in2.database.attach(in1.partition, 'in1')
        
        print in1.database.path
        print in2.database.path
        return
        
        q = """
        SELECT  in2.type as type, in2.datetime as datetime, in2.agency as agency,  in1.blockaddress AS a1, in2.blockaddress as a2
        FROM incidents AS in2
        LEFT JOIN {in1}.incidents as in1 
            ON in1.datetime = in2.datetime AND in1.legend = in2.type and in1.agency = in2.agency
        WHERE  in2.datetime BETWEEN date('2013-03-01') AND date('2013-03-01','+1 month','-1 day' ) 
        """
        
        for row in in2.query(q):
            
            print row
            row = dict(row)
            print "{datetime}\t{agency}\t{type}\t{a1}\t{a2}".format(**row)

     
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)  
    