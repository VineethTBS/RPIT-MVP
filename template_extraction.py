import pandas as pd
from openpyxl import load_workbook
import boto3
import dill

import formulas

import string
import time

class TemplateExtraction:

    def __init__(self,file_name,id):
        #self.dfInput = pd.read_excel(file_name, sheet_name='Data')
        self.dfInput = pd.read_excel(file_name)
        #self.max_key=self.find_row_with_highest_count()
        #self.dfInput = pd.read_excel(file_name, skiprows=self.max_key)
        self.id = id
        self.computed_cols = []     
        self.masterContent = None
        pass
    
    def find_row_with_highest_count(self):
        index_dict = {}
        for index, row in self.dfInput.iterrows():
            string_count = sum(isinstance(cell, str) for cell in row)
            index_dict[index] = string_count
            if index >= self.dfInput.shape[0] * 0.05:
                break
        max_key = max(index_dict, key=index_dict.get)
        return max_key


    def getDynamo(self):
        return boto3.resource(
        'dynamodb',
        aws_access_key_id     = 'ASIA4C44A42VPPEXLJCK',
        aws_secret_access_key = 'GoVvzg3s9oOQ36FhbTMpZzjmuwshyySh3pifQYoz',
        aws_session_token     = 'FwoGZXIvYXdzEFUaDM/mQFa1PAtbL4rhVSKTAtmPud5nTlYakjsAMbMeKqKkOyBFNzyrfgmy2oZtNR+vpeBjGLTzKSsXpFBa9w1jLP5serHZfsf1QgGMbfxGc42u2YyUX+eGJShz8YzLSNiYKTKrFqUWMK9LY1pAq8EXMIbqYIoANyJf/0VdvzCUDCKnTK8lb/YLHVwlKXmwYpQXXXKCbbbFCMyRJVhe7HRRGIPc8Bu86fIrWJRrUBoEvN048nNM1UCJ2nUo7ilMJlPSxmuU6378xC5HzCRL4OVqn16qnM/Qaac+0G/9/j4DhLJLgdOfRNdL5HN61fChMl1x+pNuRrGsAJf9uUiZpNY523BQ4symHEAv50uRugfqQBKRr0QUfgpN3qwlepSpha2xSN+/KLrynbUGMitWf0RjWVcRRNr0otWxxs1vGI/vkahEQrxDRnYZDg/Df2dvThEb5vX8Y0PA',
      region_name="us-east-1")
    

    
    def loadFunctionDict(self,data):
            funcDict =dict()
            for columnDef in data["columnFunctionData"]:
            
                if not columnDef["functionMetaData"]["isConstant"]:
                    func = formulas.Parser().ast(columnDef["functionFormula"])[1].compile() 
                    funcDict[columnDef["index"]]=func
            return funcDict

    def order_output_column_sequence(self):
        sequence = []
        columns_encountered = []
        for i in range(len(self.mastercontent["columnFunctionData"])):
              if    self.mastercontent["columnFunctionData"][i]["functionMetaData"]["isConstant"]:
                   return


    def col2num(self,col):
        num = 0
        for c in col:
            if c in string.ascii_letters:
                num = num * 26 + (ord(c.upper()) - ord('A')) + 1
        return num
    def paramaterDict(self,isVlookUp,Col_Name,VlooKUp_Val):
         para_dict=dict()
         para_dict["VlookUp"]=isVlookUp
         para_dict['ColName']=Col_Name
         para_dict['VlookUpVal']=VlooKUp_Val
         return para_dict




    def buildDataFromFormula(self,columnIndex):
            parameterTuple = list()
            # print("for column index  {index}".format(index=columnIndex))
            vlookupList = []
            # if columnIndex==24:
            #     print("reached")
 

            for parameter in self.masterContent["columnFunctionData"][columnIndex]["functionMetaData"]["parameter"]:
                if parameter["isVlookup"]:
                    par_dict=self.paramaterDict(True,parameter["ParameterColumnName"],dill.loads( bytes(parameter["vlookUpHash"])))
                    #vlookupList.append(dill.loads( bytes(parameter["vlookUpHash"])) )
                    #parameterTuple = parameterTuple + (vlookupList[len(vlookupList)-1],)
                    parameterTuple.append (par_dict)
                elif (parameter["ParameterColumnName"] not in  list(self.dfInput.columns)):
                     return False
                elif   (parameter["isSelfRef"]):
                    if parameter["ParameterColumnName"] in  self.dfInput.columns:
                        par_dict=self.paramaterDict(False,parameter["ParameterColumnName"],None)
                       # parameterVal =  self.dfInput[parameter["ParameterColumnName"]]  
                        #parameterVal.fillna('')
                        parameterTuple.append(par_dict)
                        #parameterTuple = parameterTuple + (parameterVal,)
                    else:
                         return False
                        # parameterVal = self.buildDataFromFormula(depth+1, columnInfo  ,dfInput,functionDict,rowVal,rowIndex,int(parameter["parameterIndex"])-1)
                        # if pd.isna(parameterVal) or len(str(parameterVal))==0:
                        #     parameterVal=''
                        # parameterTuple = parameterTuple + (parameterVal,)
                else:
                    par_dict=self.paramaterDict(False,parameter["ParameterColumnName"],None)
                    #parameterVal = self.dfInput[parameter["ParameterColumnName"]] 
                    #parameterVal.fillna('')
                    parameterTuple.append(par_dict)
            
    
            func = formulas.Parser().ast(self.masterContent["columnFunctionData"][columnIndex]["functionFormula"])[1].compile() 
            self.computed_cols.append(self.masterContent["columnFunctionData"][columnIndex]["ColumnName"])
            try:
                self.dfInput[self.masterContent["columnFunctionData"][columnIndex]["ColumnName"]] = self.dfInput.apply(lambda row: self.func_wrapper(func, row,parameterTuple), axis=1)
                self.masterContent["columnFunctionData"].remove(self.masterContent["columnFunctionData"][columnIndex])
            except Exception as ex:
                 print(ex)
                 self.dfInput = self.dfInput.fillna('')
                 
                 self.dfInput.to_excel("output-XI.xlsx")
                 end_time = time.time()
                 print("Execution time")
                 print(end_time-self.start_time)
                 exit()
            return True
    
    def func_wrapper(self,func,row,parameter_row_dict):
        parameter_tuple = tuple()
        for parameter in parameter_row_dict:
            if parameter["VlookUp"]:
                parameter_tuple = parameter_tuple+(parameter['VlookUpVal'],)
            else:
             parameter_tuple=parameter_tuple+(row[parameter['ColName']],)
        return func(*(parameter_tuple))
        
      
        
              

         
         
    def extract_output_columns(self):
        output_cols = []
        
        for col in self.masterContent["columnFunctionData"]:
              
             #if col.get("IsIntermediate")==False:
              output_cols.append(col["ColumnName"])
        return output_cols


    def SurveillanceDealData(self): 
            print(len(self.dfInput))
            dyn_resource = self.getDynamo()  
            table = dyn_resource.Table("psl-db-data-mapper-lab") 
            response = table.scan()["Items"] 
            self.masterContent = None  
            for value in response:              
                    if 'Id' not in value:
                        continue  
                    if value["dealId"]==self.id:
                            self.masterContent = value
                            break
            output_columns =  self.extract_output_columns()
            # outputDf = pd.DataFrame(columns=output_columns) 
            
            index = 0
            self.start_time = time.time()
            col_computed = False
            while len(output_columns)!= 0:
                col_computed = False
                if  index >= len(output_columns):
                     index = 0
                if self.masterContent["columnFunctionData"][index]["functionMetaData"]["isConstant"]:
                    self.dfInput[self.masterContent["columnFunctionData"][index]["ColumnName"]] = self.masterContent["columnFunctionData"][index]["functionMetaData"]["ConstantVal"]
                    self.computed_cols.append(self.masterContent["columnFunctionData"][index]["ColumnName"]) 
                    self.masterContent["columnFunctionData"].remove(self.masterContent["columnFunctionData"][index])
                    col_computed = True
                else:
                        col_computed = self.buildDataFromFormula(index)
                if col_computed:
                 output_columns = [col for col in output_columns if col not in self.computed_cols ]     
                else:
                     index+=1
            self.dfInput.to_excel("output-XII.xlsx")
            print("Data Generation Completed")
           # output_df.to_excel('output_ii.xlsx')

        
raw_poolcut_file =r"apollo_raw.xlsx"
temp  = TemplateExtraction(raw_poolcut_file,"Apollo Series 2023-1 from_Suncorp MILAN Template - HL edit ")
temp.SurveillanceDealData()
