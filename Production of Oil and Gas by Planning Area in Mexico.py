import sys, os, time, time, datetime
import pandas as pd
import numpy as np
import SourceData
import requests
import csv, xlrd
import KNG, KNMetaData, KNUpload, SourceData

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
KN.PrintTF('\n> DataSet ID: ' + Dataset_Id)
KN.PrintTF('\n> Source URL: ' + URL)

KN.Host = Host
KN.App_Id = App_Id
KN.App_Secret = App_Secret

KNUpload_ToolPath = os.path.join(S_Path, 'Tool_Knoema_Upload')            
if not os.path.exists(KNUpload_ToolPath):
    KN.PrintTF('\n>> Failed: Knoema Upload C# Tool Not Found @: ' + KNUpload_ToolPath)
    sys.exit(1)

KU = KNUpload.KNUpload()
KU.Host = Host
KU.App_Id = App_Id
KU.App_Secret = App_Secret
KU.Logs_Folder_Path = PY_Space    
KU.KNUpload_ToolPath = KNUpload_ToolPath

Mapping_File_Path = os.path.join(S_Path, 'Mapping_File.xlsx')
if not os.path.exists(Mapping_File_Path):
  raise Exception('Failed: Mapping file "Mapping_File.xlsx" NOT found')
  sys.exit(1)

#--------------------------------------------------------------------------------------------------------------------------------------------------

Dimensions = ['Indicator', 'Type of Store']
Header = ['ID', 'Title', 'Description', 'Province', 'Number of Deaths',	'Number of Injuries', 'Number of Employees', 'Industry', 'Type',
          'Number of Person Punished', 'Financial Penalty', 'Latitude', 'Longitude', 'Date']

#---------------------------------------------------------------------------------------------------------------------------------------------------

obj = SourceData.Download()
obj.PY_Space = PY_Space
obj.URL = URL

start_time = time.time()
obj.Source_Data_Download()

KN.PrintTF('\n>> Source File succesfullly Downloaded')

#---------------------------------------------------------------------------------------------------------------------------------------------------

Header = ['Planning Area', 'Indicator', 'Frequency', 'Value', 'Date']
Mapping_File = pd.read_excel(Mapping_File_Path, dtype=object)

Planning_area = {'Month Total':'KN.P1', 'Central GOM':'KN.P2','Western GOM':'KN.P3'}
Planning_area = dict((k.upper(), v.upper()) for k,v in Planning_area.items())

Indicator_map = Mapping_File.set_index('INDName')['INDCode'].to_dict()
Indicator_map = dict((k.upper(), v.upper()) for k,v in Indicator_map.items())

Unit_map = Mapping_File.set_index('INDCode')['Unit'].to_dict()
Unit_map = dict((k.upper(), v.upper()) for k,v in Unit_map.items())
Month_Map  = {'1':'M01','2':'M02','3':'M03','4':'M04','5':'M05','6':'M06','7':'M07','8':'M08','9':'M09','10':'M10','11':'M11','12':'M12'}

#---------------------------------------------------------------------------------------------------------------------------------------------------

def file_rename(newname, Folder , time_to_wait=60):
    time_counter = 0
    filename = max([f for f in os.listdir(PY_Space)], key=lambda xa :   os.path.getctime(os.path.join(PY_Space,xa)))
    while '.part' in filename:
        time.sleep(1)
        time_counter += 1
        if time_counter > time_to_wait:
            raise Exception('Waited too long for file to download')
    filename = max([f for f in os.listdir(PY_Space)], key=lambda xa :   os.path.getctime(os.path.join(PY_Space,xa)))
    os.rename(os.path.join(PY_Space, filename), os.path.join(PY_Space, newname))

file_rename('Source_file.xls',PY_Space)

File_Path = os.path.join(PY_Space , 'Source_file.xls')
try:
    if os.stat(File_Path).st_size > 0:
        KN.PrintTF("\n>>Time Taken to Download the Source File --- %s seconds ---" % (time.time() - start_time))
        KN.PrintTF("\n>> Source File found in the Directory: "+ File_Path) 
    else:
        KN.PrintTF('\n>> Failed: Empty file Detected, Please Check: '+ File_Path)
        sys.exit(1)
except OSError:
    KN.PrintTF('\n>> Failed: No such files found in the Directory: ')
    sys.exit(1)
print('-'*75)

#-------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n<> Data Processing started for : , Source_file.xls ')
wb = xlrd.open_workbook(File_Path, logfile=open(os.devnull, 'w'))
Data = pd.read_excel(wb)

Data.rename(columns={Data.columns[0]: "Date" }, inplace = True)
Data[['first','second']] = Data.Date.str.split("M" , expand = True,)
Data.loc[Data["Planning Area"].isnull(),'Planning Area'] ='M'+ Data["second"]

Data['Date'] = Data['Date'].map(lambda x: ''.join([i for i in x if i.isdigit()]))
Data['Year'] = Data['Date'].str[-4:].str.strip()
Data['month'] = Data['Date'].str[:-4]

Data.drop(columns = ['first', 'second', 'Date'], inplace = True)
Data['month'] = Data['month'].str.strip().map(Month_Map)

Data['Year'] = Data['Year'].replace('', np.nan).ffill(axis = 0)
Data['month'] = Data['month'].replace('', np.nan).ffill(axis = 0)

Data['Date'] = Data['Year'].astype(str) + Data['month'].astype(str)
Data.drop(columns = ['Year', 'month'],inplace = True)

DataT = pd.melt(Data, id_vars=['Planning Area','Date']).sort_values(['Planning Area','Date'])
DataT.rename(columns = {'variable' : 'Indicator'},inplace = True)
DataT.reset_index(drop = True)

KN.PrintTF('\n<> Mapping dimension code, Indiactor and units to the Dataframe')

DataT['Planning Area'] = DataT['Planning Area'].str.upper().str.strip().map(Planning_area)
DataT['Indicator'] = DataT['Indicator'].str.upper().str.strip().map(Indicator_map)
DataT['Unit'] = DataT['Indicator'].str.upper().str.strip().map(Unit_map)

DataT['Frequency'] = 'M'

KN.PrintTF('\n<> CrossChecking for null Values in Mapped Columns')
Columns = 'Indicator', 'Planning Area', 'Unit'
for column in Columns:
    if DataT[column].isnull().any():
        KN.PrintTF('\n>> Failed: Code Not Mapped properly, Please Check Column : '+ column)
        sys.exit()
    else:
        KN.PrintTF('\n>> Null values Not found in Column :' + column)

DataT = DataT.drop_duplicates(subset = ['Date','Indicator','Planning Area'],keep = False)
DataT.rename(columns = {'value':'Value'},inplace = True)
DataT.dropna(subset = ['Value'],inplace = True)

Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataT, 'Value')
if len(Non_Numeric) > 0:
    sys.exit(1)

DataDup = KN.Duplicates_CrossCheck(DataT ,  ['Indicator', 'Date', 'Planning Area'])
if len(DataDup) > 0:
    sys.exit(1)

KN.TimeSeries_Count(DataT, ['Planning Area','Indicator','Frequency'], PY_Space)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
DataT.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

KN.PrintTF('\n Data File Exported to Path : ' + UploadFiles_Path)
Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)
