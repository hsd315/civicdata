'''
Created on Jun 7, 2012

@author: eric
'''

class ProcessHeaders:
       
    def __init__(self, bundle):
        '''
        Constructor
        '''
        self.bundle = bundle
    
    def run(self):
        
        import pprint
        import csv
        rd = self.bundle.root_dir
        
        config = rd.bundle_config.yaml
        
        header_file = rd.absolute(config['build']['headers'])

        reader  = csv.DictReader(open(header_file, 'rbU') )

        info = dict()
        headers = dict()
        tables = []
        
        for i in range(40):
            o = dict()
            o['info'] = dict()
            o['header'] = []
            o['columns'] = {}
            tables.append(o)
           

       
        for row in reader:
           
            file_name = row['SF#']

            file_number = int(file_name.replace('SF',''))
           
            field_name = row['FIELDNUM']
            if row['TABNO']:
                table_id = row['TABNO']
            table_text = row['TEXT']
            
            if( not field_name):
                tables[file_number]['info']['table'] = table_id
                if(table_id):
                    tables[file_number]['info']['title'] = table_text
                else:
                    tables[file_number]['info']['universe'] = table_text
            else:
                d = {}
                d['text'] = row['TEXT']
                d['table'] = table_id
                tables[file_number]['columns'][field_name] = d
                #tables[file_number]['header'].append(field_name)
                    
        pprint.pprint(tables)
