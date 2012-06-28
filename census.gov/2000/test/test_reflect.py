'''
Created on Jun 21, 2012

@author: eric
'''
import unittest


class Test(unittest.TestCase):


    def test_reflect(self):
       
        import sys
        import os.path
        from sqlalchemy import MetaData   
        # Reset the path so the Bundle() constructor will walk up the right directory    
        sys.path[0] = os.path.dirname(__file__)
       
        from databundles.bundle  import Bundle
       
        b = Bundle()

        engine = b.protodb.engine
        meta = MetaData()
        meta.reflect(bind=engine)
        for table in meta.tables.values():
            print """
        class %s(Base):
            __tablename__ = %s  
        """ % (table.name.capitalize(), table.name)
        
            for column in table.columns:
                prefix,sep,name = column.name.partition("_"); #@UnusedVariable
                
                data_type = str(column.type).capitalize()
                
                if name == 'id':
                    name = 'oid'
                    print """            {} = Column('{}',{}, primary_key=True)""".format(name,column.name,data_type)
                else:
                    print """            {} = Column('{}',{})""".format(name,column.name,data_type)
                
            print """
            def __init__(self,**kwargs):"""
            
            for column in table.columns:
                prefix,sep,name = column.name.partition("_"); #@UnusedVariable
                
                if name == 'id':
                    name = 'oid'
                
                data_type = str(column.type).capitalize()
                print "                self.{} = kwargs.get(\"{}\",None) """.format(name,name)
      
            print """
            def __repr__(self):
                return "<%s: {}>".format(self.id)
             """%(table.name)
             
             
             
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_reflect']
    unittest.main()