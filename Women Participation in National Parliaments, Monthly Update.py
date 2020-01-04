import pandas as pd
import requests
import csv
import numpy as np
import KNG, KNMetaData, KNUpload
import wget
import sys, os, time, time, datetime,re

pd.set_option('display.max_colwidth', 10000)
pd.set_option('display.expand_frame_repr', False)

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
KU.Flat_Input = 'Append'  # For Flat Dataset - Append Only

Mapping_File_Path = os.path.join(S_Path, 'Mapping_File.xlsx')
if not os.path.exists(Mapping_File_Path):
  KN.PrintTF('\n>> Failed: Mapping file "Mapping_File.xlsx" NOT found')
  sys.exit()

#----------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n>> Veryfying todays date, Please wait')
start_time = time.time()

todayDate = datetime.date.today()
if todayDate.day > 25:
  todayDate += datetime.timedelta(7)
FD_MNTH = todayDate.replace(day=1)

Todays_Date = datetime.datetime.today().strftime('%Y-%m-%d')
KN.PrintTF('\n>> Todays Date: ' +Todays_Date)
if Todays_Date == FD_MNTH:
  pass
else:
  KN.PrintTF('\n>> Failled: Tool Execution Stopped, Try running the tool on First day of the Month only')
  sys.exit(1)
    
#----------------------------------------------------------------------------------------------------------------------------------------

M = datetime.date.today().strftime('%m')
Y = datetime.date.today().strftime('%Y')

URL = URL.format(M, Y)

#-----------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n>> Checking whether URL exists or Not')
request = requests.get(URL)
if request.status_code == 200:
    KN.PrintTF('\n>> URL exists')
else:
    KN.PrintTF('\n>> Failed: URL doesnt Exist, Please check:' + URL)

FilePath = wget.download(URL , PY_Space)
KN.PrintTF('\n>> File downloaded, Exported to : ' + FilePath)

#-----------------------------------------------------------------------------------------------------------------------------------------

DataAll = pd.DataFrame()
Header = ['Country', 'Region', 'Sub-region', 'House', 'Number of seats', 'Number of women', 'Percentage of women participation',
          'Situation as of', 'Elections']

Month_Map = {1:'M1', 2:'M2', 3:'M3', 4:'M4', 5:'M5', 6:'M6', 7:'M7', 8:'M8', 9:'M9', 10:'M10', 11:'M11', 12:'M12'}
Mapping_File = pd.read_excel(Mapping_File_Path, dtype=object)
Region_Dict = Mapping_File.set_index('Country')['Region'].to_dict()
Sub_Region_Dict = Mapping_File.set_index('Country')['Sub-region'].to_dict()

#------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF("\n> Reading Source Data: " + FilePath)

Data = pd.read_csv(FilePath,header =None ,dtype=object)
DataA = Data.loc[:, '1':'5']
DataB = Data.loc[:, '6':]

DataA = DataA.dropna(subset = [1, 2, 3], how='all')
DataA.reset_index(drop=True)

lists = 2, 3, 4, 5   # Tuple
for list in lists:
  DataA[list] = pd.to_numeric(DataA[list], errors='coerce')
DataA.dropna(subset = [2, 3, 4, 5], how = 'all', inplace=True)

DataA.rename(columns = {1:'Country', 2:'Elections', 3:'Number of seats', 4:'Number of women', 5:'Percentage of women participation'}, inplace=True)
DataA["House"] = 'Lower or Single House'
DataAll = DataAll.append(DataA[['Country', 'House', 'Number of seats', 'Number of women', 'Percentage of women participation', 'Elections']])

#--------------------------------------------------------------------------------------------------------------------------------------------

DataB = DataB.dropna(subset = [6,7,8])
DataB['Country'] = DataA['Country']

lists = 6, 7, 8, 9
for list in lists:
  DataB[list] = pd.to_numeric(DataB[list], errors='coerce')
DataB.dropna(subset = [6, 7, 8, 9], how='all', inplace=True)
  
DataB.rename(columns = {6:'Elections', 7:'Number of seats', 8: 'Number of women', 9:'Percentage of women participation'}, inplace=True)
DataB["House"] = 'Upper House or Senate'
DataAll = DataAll.append(DataB[['Country', 'House', 'Number of seats', 'Number of women', 'Percentage of women participation', 'Elections']])

Check_list = 'Number of seats', 'Number of women', 'Percentage of women participation'
DataAll.drop_duplicates(subset = ['Country', 'Number of seats', 'Number of women', 'Percentage of women participation'], inplace=True)

DataDup = KN.Duplicates_CrossCheck(DataAll, ['Country', 'Number of seats', 'Number of women', 'Percentage of women participation'])
if len(DataDup) > 0:
  sys.exit(1)

DataAll['Situation as of'] = FD_MNTH
DataAll['Situation as of'] = pd.to_datetime(DataAll['Situation as of']).dt.strftime('%Y-%m-%d')
DataAll.reset_index(drop = True)

DataAll.Elections = DataAll.Elections.astype(str)
DataAll.Elections = DataAll.Elections.apply(lambda x : None if x=="NaT" else x)

DataAll['Country'] = DataAll['Country'].replace('\d+', '')
DataAll['Country'] = DataAll['Country'].map(lambda x: x.strip('1'))

DataAll["Country"]= DataAll["Country"].replace('Bolivia (Plurinational State of)', "Bolivia")
DataAll["Country"]= DataAll["Country"].replace('Gambia (The)', "Gambia")
DataAll["Country"]= DataAll["Country"].replace("Côte d'Ivoire", "Cote d'Ivoire")

Unw_char = [';', ':', '!', "*" , '^' ,'_']
for I in Unw_char : 
    DataAll['Country'] = DataAll['Country'].replace(I, '') 

DataAll['Country'] = DataAll['Country'].str.strip()

Region_Dict = dict((Key.upper(), Value.upper()) for Key, Value in Region_Dict.items())
DataAll['Region'] = DataAll['Country'].apply(str.upper).map(Region_Dict)

Sub_Region_Dict = dict((Key.upper(), Value.upper()) for Key, Value in Sub_Region_Dict.items())
DataAll['Sub-region'] = DataAll['Country'].apply(str.upper).map(Sub_Region_Dict)

DataAll[['Month','Year']] = DataAll['Elections'].astype(str).str.split("." , expand =True,)
DataAll['Month'] = DataAll['Month'].replace(Month_Map)
DataAll.drop(columns = ['Elections'] , inplace=True)  # Deleting old column
DataAll['Elections'] = DataAll['Year'].astype(str) + 'M' + DataAll['Month'].astype(str)
DataAll = DataAll.replace('NoneMnan', '')

Mapped_data = 'Region','Sub-region'
KN.PrintTF('\n> Cross checking Mapped Values')
for Map_data in Mapped_data:
  if DataAll[Map_data].isnull().any():
    KN.PrintTF("\n>> Failed: Values Not Mapped Properly, Please Cross Check file: " + Mapping_File_Path)
    sys.exit(1)
  else:
    pass
  
KN.PrintTF('\n>> Null Values Not found, Exporting the Data for Upload')
      
UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
DataAll.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

##Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)

