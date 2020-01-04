import sys, os, time, time, datetime
import socket
import pandas as pd
import numpy as np
import csv, xlrd
import urllib.request
import KNG, KNMetaData, KNUpload

pd.set_option('display.max_colwidth', 10000)
pd.set_option('display.expand_frame_repr', False)

#----------------------------------------------------------------------------------------------------------------------------------------

print('\nHost Machine :',socket.gethostname())
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

KN.PrintTF('\n<> Extracting Source Data from the URL : '+ URL)
File_Name = URL.split('/')[-1]
File_Path = os.path.join(PY_Space, File_Name)

try:
    Content = urllib.request.urlopen(URL)
    
except Exception as error:
    KN.PrintTF('\n>> Found error in the URL : ' + repr(error))
    sys.exit(1)

KN.PrintTF('\n>> Data Extracted, Writing Data to csv file')
if os.path.exists(PY_Space):
    with Content as file, open(File_Path, 'wb') as f:
        f.write(file.read())
        KN.PrintTF('\n>> Data Exported as csv file to path :'+ File_Path)
        KN.PrintTF("\n>> File Size :" + str(os.stat(File_Path).st_size/1000000) +'MB')
else:
    KN.PrintTF('\n>>> No such directory found : '+ PY_Space)
    sys.exit()

print('-'*75)
#---------------------------------------------------------------------------------------------------------------------------------------------------

Dimensions = ['Variable', 'Asset Size Group']
Header = ['Variable', 'Asset Size Group', 'Unit', 'Frequency', 'Date', 'Value']

Variable_Map = pd.read_excel(Mapping_File_Path, sheet_name = 'Variable', dtype = object).set_index('Name')['Code'].to_dict()
Variable_Map = dict((k.upper(), v.upper()) for k,v in Variable_Map.items())

Assest_Size_Group_Map = pd.read_excel(Mapping_File_Path, sheet_name = 'Asset Size Group', dtype = object).set_index('Name')['Code'].to_dict()
Assest_Size_Group_Map = dict((k.upper(), v.upper()) for k,v in Assest_Size_Group_Map.items())

Unit_Map = pd.read_excel(Mapping_File_Path, sheet_name = 'Unit', dtype = object).set_index('Code')['Unit'].to_dict()
Unit_Map = dict((k.upper(), v.upper()) for k,v in Unit_Map.items())

#---------------------------------------------------------------------------------------------------------------------------------------------------

def file_rename(newname, Folder , time_to_wait=60):
    time_counter = 0
    filename = max([f for f in os.listdir(PY_Space)], key=lambda xa : os.path.getctime(os.path.join(PY_Space,xa)))
    while '.part' in filename:
        time.sleep(1)
        time_counter += 1

    os.rename(os.path.join(PY_Space, filename), os.path.join(PY_Space, newname))

file_rename('Source_file.csv', PY_Space)
File_Path = os.path.join(PY_Space , 'Source_file.csv')

#---------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n<> Data Processing for : '+ File_Path)

wb = xlrd.open_workbook(File_Path, logfile=open(os.devnull, 'w'))
Data = pd.read_excel(wb ,header = None, sheet_name = 'DATA' , dtype = object)

Data['Asset Size Group'] = Data[1]
Data[1] = Data[1].str.upper().str.strip().map(Variable_Map)
Data[['first','Variable']] = Data[1].str.split("@" , expand = True,)

Data = Data.drop(columns = ['first'])
Data['Variable'] = Data['Variable'].replace('', np.nan).ffill(axis = 0)

KN.PrintTF('\n<> Checking Null Column in Dataframe')
print('\n>> Null mean value in column 0 is = ', Data[0].isin([' ','NULL',0]).mean(),'%')

if Data.loc[:, Data[0].isin([' ','NULL',0]).mean() < .6].all():
    Data.drop(columns = [0],inplace = True)  
    KN.PrintTF('\n>> Found null value Column , Null Columns are dropped')
else:
    KN.PrintTF('\n>> Failed: Found Values in column 0, DataFrame May have changed : Please Check')
    sys.exit()

Data.drop(columns = [1], inplace = True)
Data['Asset Size Group'] = Data['Asset Size Group'].str.upper().str.strip().map( Assest_Size_Group_Map )

Data.dropna(subset = [8,20,30,50,100,120,135] ,how = 'all' , inplace = True)
Data.columns = Data.iloc[0]
Data = Data.reset_index(drop = True)
Data = Data.drop(Data.index[0])

Data = Data.rename(columns={np.nan: 'Asset Size Group','KN.A1': 'Variable'})

KN.PrintTF('\n<> CrossChecking for null Values in Mapped Columns')
Data = Data.dropna(subset =['Asset Size Group'])
Columns = 'Asset Size Group', 'Variable'
for column in Columns:
    if Data[column].isnull().any():
        KN.PrintTF('\n>> Failed: Code Not Mapped properly, Please Check Column : '+ column)
        sys.exit()
    else:
        KN.PrintTF('>> Null values Not found in Column :' + column)

print('\n>> Transposing the DataFrame') 	
DataT = pd.melt(Data, id_vars=['Asset Size Group','Variable']).sort_values(['Asset Size Group','Variable'])

DataT = DataT.rename(columns = {4:'Date', 'value':'Value'})
DataT = DataT.reset_index(drop = True)
print(DataT.head(5))

DataT.loc[DataT['Date'].str.contains('Q'), 'Frequency'] = 'Q'
DataT['Unit'] = DataT['Variable'].astype(str).str.upper().str.strip().map(Unit_Map)

DataT = DataT.replace('',np.nan).dropna(subset = ['Value'])

Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataT, 'Value')
if len(Non_Numeric) > 0:
    sys.exit(1)

DataDup = KN.Duplicates_CrossCheck(DataT ,  ['Asset Size Group', 'Date', 'Variable'])
if len(DataDup) > 0:
    sys.exit(1)

KN.TimeSeries_Count(DataT, ['Asset Size Group','Variable','Frequency'], PY_Space)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
DataT.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

KN.PrintTF('\n Data File Exported to Path : ' + UploadFiles_Path)
Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)













