import sys, os, csv, time, time, datetime
import pandas as pd
import ast
from pandas.io.json import json_normalize
import json
import requests
import KNG, KNMetaData, KNUpload, metadata

pd.set_option('display.max_colwidth', 10000)
pd.set_option('display.expand_frame_repr', False)

#----------------------------------------------------------------------------------------------------------------------------------------

KN = KNG.KNG()
Time = time.time()

S_Path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PY_Space = KN.Folder_Create(S_Path, 'PY_Space')
KN.Delete_All_Files(PY_Space)
KN.Logs_Folder_Path = PY_Space

ToolConfigF = os.path.join(S_Path, 'Tool Config.txt')        
if not os.path.exists(ToolConfigF):
    KN.PrintTF('\n>> Failed: "Tool Config.txt" NOT Found')
    sys.exit()

Host = ''
App_Id = ''
App_Secret = ''
Dataset_Id = ''
URL = ''

with open(ToolConfigF) as ToolConfig:
    for Line in ToolConfig:
        if str.upper(Line).startswith('HOST:'):
            Host = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('APP_ID:'):
            App_Id = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('APP_SECRET:'):
            App_Secret = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('DATASET_ID:'):
            Dataset_Id = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('URL:'):
            URL = Line[Line.find(':')+1:len(Line)].strip()            

KN.Check_Exists(ToolConfigF, [Host, App_Id, App_Secret, Dataset_Id, URL])

KN.PrintTF('\n> Host: ' + Host)

KN.Host = Host
KN.App_Id = App_Id
KN.App_Secret = App_Secret

KNUpload_ToolPath = os.path.join(S_Path, 'Tool_Knoema_Upload')            
if not os.path.exists(KNUpload_ToolPath):
    KN.PrintTF('\n>> Failed: Knoema Upload C# Tool Not Found @: ' + KNUpload_ToolPath)
    sys.exit()

KU = KNUpload.KNUpload()
KU.Host = Host
KU.App_Id = App_Id
KU.App_Secret = App_Secret
KU.Logs_Folder_Path = PY_Space    
KU.KNUpload_ToolPath = KNUpload_ToolPath

#--------------------------------------------------------------------------------------------------------------------------------------------------

Header = ['Indicator', 'Frequency','Value','Date' ]

#---------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n>> Source Retrieving')
try:
    Res = requests.get(URL)
except Exception as e:
    KN.PrintTF('\n>> Failed: Please check the link: ' + Data_URL)
    sys.exit()
    
json_data = json.loads(Res.text)
json_data = json_data[0]['chartDataValue']

Source_File_Path = os.path.join(PY_Space, 'output.csv')

List = []
for data in json_data:
    List.append(data)

DataAll = pd.DataFrame(List)
DataAll = pd.DataFrame([ast.literal_eval(x) for x in DataAll[0]])

DataAll.rename(columns = {'s1' : 'Value'} , inplace = True)
DataAll.columns = DataAll.columns.str.capitalize()

def flatten_json(y):
    out = {}
    
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

flat = flatten_json(json_data)

DataAll.drop(DataAll.columns[1],axis=1,inplace=True)
DataAll['Frequency'] = 'D'
DataAll['Indicator'] = 'KN.I2'

#----------------------------------------------------------------------------------------------------------------------------------------------------

DataDup = KN.Duplicates_CrossCheck(DataAll, ['Date','Value'])
if len(DataDup) > 0:
  sys.exit(1)

DataAll['Value'] = DataAll['Value'].astype(str)
Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataAll, 'Value')
if len(Non_Numeric) > 0:
    sys.exit(1)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
DataAll.to_csv(Export_PathF, index=False, encoding='utf-8-sig')

Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)


