import csv
from lxml import etree
from pymongo import MongoClient
import pandas
import os
from pathlib import Path
from datetime import datetime
import xmltodict
import re
from const import dbconst

try:
    import simplejson as json
except ImportError:
    import json


delimeter = "|"
folderPath = '.\\TemporaryCsvs\\'

from conf import env
config = env.get_config()
address = f'{config.MONGODB_HOST}:{config.MONGODB_PORT}/'

client = MongoClient(address, maxPoolSize=50, connect=True)
db = client[config.MONGODB_DATABASE]
xml_mapping = db[dbconst.XMLMAPPING]
xml_configuration = db[dbconst.XMLCONFIGURATION]


def process_xml(filename_xml, db):
    # parse xml
    print ('Parsing XML')
    try:
         
        doc = etree.parse(filename_xml)
        print('XML well formed, syntax ok.')
    # check for file IO error
    except IOError:
        print('Invalid File')

    # check for XML syntax errors
    except etree.XMLSyntaxError as err:
        print('XML Syntax Error, see error_syntax.log')
        with open('error_syntax.log', 'w') as error_log_file:
            error_log_file.write(str(err.error_log))
        quit()

    except:
        print('Unknown error, exiting.')
        quit()

    print('Converting XML to CSV')
    
    root = doc.getroot()
     
    asset_class=os.path.basename(filename_xml).split('_')[1]
    
    mapping_collection =  xml_mapping.find({"AssetClass": {'$regex' : '.*' +  
	                                        str(asset_class.upper()) + '.*'}})
        
    if len(list(mapping_collection)) == 0:
        print('Unknown Asset Class :' + asset_class)
        csv_file_name = process_generic_xml_file(filename_xml)
        return csv_file_name

    all_fields = mapping_collection.distinct("Column")
    all_loan_data = []

    root_path = transform_xml_path(xml_configuration.find_one()['RootPath'])

    NSMAP = {'ns':xml_configuration.find_one()['NameSpace']}
    
    pcdt  = root.findtext(
	         transform_xml_path(xml_configuration.find_one()['PoolCutDatePath']), 
	         namespaces=NSMAP)
					
    edcode = root.findtext(
	          transform_xml_path(xml_configuration.find_one()['EDCodePath']), 
	          namespaces=NSMAP)

    i=0
    for xml_element in root.findall(root_path, namespaces=NSMAP):
        i=i+1
        
        data_record = {}
        
        data_record.update({'_id' : i})
        data_record.update({'POOLCUTDATE' : pcdt})
        data_record.update({'EDCODE' : edcode})
             
        for col in all_fields:
            data_record.update(
			{col : get_value_for_column(col,xml_element, NSMAP, root_path)}
			)    
    
        all_loan_data.append(data_record)
        
        
    if len(all_loan_data)>0:    
        collecion_to_csv(all_loan_data,filename_xml+'.csv')    

    return filename_xml+'.csv'

def transform_xml_path(path):    
    path = path.replace("Document", "ns:Document")
    path = path.replace("//", "//ns:")    
    return '*//'+path

def get_value_for_column(column,  xml_element, NSMAP, root_path):   

    tmp_val=''     
	
    for mapping in xml_mapping.find({"Column": column}):
        tmp_val = str(xml_element.findtext(
		transform_xml_path(mapping["XML Path"]).replace(root_path+'//',''), 
		namespaces=NSMAP))
		
        if len(tmp_val.rstrip())>0:
            break
        
    return tmp_val
    
def collecion_to_csv(mongo_docs,output_file_name):      
    docs = pandas.DataFrame(columns=[])

    for num, doc in enumerate(mongo_docs):
        doc["_id"] = str(doc["_id"])
        doc_id = doc["_id"]
        series_obj = pandas.Series( doc,name=doc_id )
        docs = docs.append(series_obj )

    docs.to_csv(output_file_name, ",", index_label="Index")
    print('*** CSV CREATED ***')
    return

def flatten_json(mp, delim=delimeter):
    ret = []
    counter = 0
    if isinstance(mp, dict):
        for k in mp.keys():
            csvs = flatten_json(mp[k], delim)
            for csv in csvs:
                ret.append(k + delim + csv)
    elif isinstance(mp, list):
        for k in mp:
            csvs = flatten_json(k, delim)            
            for csv in csvs:
                ret.append('RecordNumber#' + str(counter) + '#' + csv)
            counter = counter + 1
    else:
        ret.append(mp)

    return ret

def process_generic_xml_file(file):
    xml_file_name =  file

    if(os.path.isdir(folderPath) == False):
        print('Created folder - ' + folderPath)
        os.mkdir(folderPath)

    final_csv_file_name = folderPath + '{Final}.csv'.format(Final=Path(xml_file_name).stem)

    print('Before loading : ', datetime.now())
    with open(xml_file_name) as fd:
        doc = xmltodict.parse(fd.read())
    print('After loading : ', datetime.now())

    r = json.dumps(flatten_json(doc))

    output_dict = json.loads(r)

    header_row = []
    data_values = {}    
    for item in output_dict:    
        if('@xmlns' not in item):
            rec_number_string = re.findall("RecordNumber#[0-9]*#", item)
            rec_number = "0"
            if len(rec_number_string) == 1:
                rec_number = rec_number_string[0].split('#')[1]
            item = re.sub('RecordNumber#[0-9]*#', '', item)

            last_index = item.rfind(delimeter)
            key = str(item[:last_index])
            value = item[last_index + 1:]
            if key not in header_row:
                header_row.append(key)
            if key not in data_values:
                new_value_list = {}
                new_value_list.update({rec_number : str(value)})
                data_values.update( {key : new_value_list} )
            else:
                data_values[key].update({ rec_number : str(value) })        
    return append_data_to_csv(data_values,header_row,final_csv_file_name)

def append_data_to_csv(data_values,header_row,final_csv_file_name):
    max_length_of_list = 0
    for k in data_values:
        if(len(data_values[k]) > max_length_of_list):
            max_length_of_list = len(data_values[k]) 

    print('After populating content for CSV file : ', datetime.now())

    # Map header row fields with the mapping paths    
    xml_mappings = list(xml_mapping.find(projection={'_id': False, 'Column': True, 'XML Path': True}))

    new_header_row = []
    for value in header_row:
        print('Value before replacing delimeter - ' + value )
        value = value.replace(delimeter, "//")
        print('Value after replacing delimeter - ' + value )
        data = next((item for item in xml_mappings if item["XML Path"] == value), None)
        if data is not None:  #Above code will return a dictionary like - {'Column': 'RREL4', 'XML Path': 'BizData|Pyld|Documen...nlOblgrIdr'}
            new_header_row.append(data['Column'].replace("//", delimeter))
        else:
            new_header_row.append(value.replace("//", delimeter))

    with open(final_csv_file_name, 'w', newline='') as file:
        fieldnames = new_header_row  #header_row
        csv_writer = csv.DictWriter(file, fieldnames= fieldnames)
        csv_writer.writeheader()

        keys_of_data = data_values.keys()
        for i in range(0, max_length_of_list):
            new_row = {}
            for item in keys_of_data: 
                try: 
                    value_to_insert =  data_values[item][str(i)]        
                except:
                    value_to_insert = ""
                new_row.update({item : value_to_insert})
            csv_writer.writerow(new_row)

    print('After writing CSV file : ', datetime.now())
    return final_csv_file_name