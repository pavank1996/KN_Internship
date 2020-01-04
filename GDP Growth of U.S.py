import sys, os, time, time, datetime
import socket
import pandas as pd
import numpy as np
import csv, xlrd
import urllib.request
from bs4 import BeautifulSoup
import requests
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

KN.PrintTF('\n<> Exctracting The Download link from the URL page Source')
Links = []

Res = requests.get(URL)
soup = BeautifulSoup(Res.text, 'lxml')
H3_tag = soup.findAll("h3")

for H3 in H3_tag:
    for link in H3.find_all('a'):
        Links.append(link['href'])

fileLink = []
for link in Links:
    linkA = link.endswith('.xlsx')
    if linkA == True:
        file_link = link
        fileLink.append(file_link)
        KN.PrintTF('\n>> Found link with extension (.xlsx) \n'+ file_link)

KN.PrintTF('\n>>> Total number of URL found :'+ str(len(fileLink)))
if len(fileLink) > 1:
    URL = 'https://www.bea.gov/'+ fileLink[0]
else:
    URL = 'https://www.bea.gov/'+ str(fileLink)

KN.PrintTF('\n>> Source File url: '+ URL)

#--------------------------------------------------------------------------------------------------------------------------------------------------
  
KN.PrintTF('\n<> Extracting Source Data from the URL : '+ URL)
File_Name = URL.split('/')[-1]
File_Path = os.path.join(PY_Space, File_Name)

try:
    Content = urllib.request.urlopen(URL)
    if len(Content) > 0:
        if os.path.exists(PY_Space):
            with Content as file, open(File_Path, 'wb') as f:
                f.write(file.read())
                KN.PrintTF('\n>> Data Exported as csv file to path :'+ File_Path)
                KN.PrintTF("\n>> File Size :" + str(os.stat(File_Path).st_size/1000000) +'MB')
        else:
            KN.PrintTF('\n>>> No such directory found : '+ PY_Space)
            sys.exit(1)
    
except Exception as error:
    KN.PrintTF('\n>> Found error while retrieving Data, urllib request Failed : ' + repr(error))
    KN.PrintTF('>> Importing Requests Module, Aproaching Requests to extract Data from URL')
    
    response = requests.get(URL)
    File_Path = os.path.join(PY_Space, "Source_File.csv")
    with open(File_Path, 'wb') as f:
        f.write(response.content)
        KN.PrintTF('\n>> Data Extracted,  Exported as csv file to path :  '+ File_Path)
        KN.PrintTF("\n>> File Size :" + str(os.stat(File_Path).st_size/1000000) +'MB')
        if len(response.content) == None:
            KN.PrintTF('\n Approach Failed, Please check the URL')
            sys.exit(1)

print('-'*75)

#---------------------------------------------------------------------------------------------------------------------------------------------------

Header = ['Indicator', 'Date', 'Unit', 'Frequency', 'Value']

Indicator_Map = pd.read_excel(Mapping_File_Path, dtype = object).set_index('Indicator Name')['Indicator'].to_dict()	
Indicator_Map = dict((k.upper(), v.upper()) for k,v in Indicator_Map.items())

#---------------------------------------------------------------------------------------------------------------------------------------------------

def file_rename(newname, Folder , time_to_wait=60):
    time_counter = 0
    filename = max([f for f in os.listdir(PY_Space)], key=lambda xa : os.path.getctime(os.path.join(PY_Space,xa)))
    while '.part' in filename:
        time.sleep(1)
        time_counter += 1

    os.rename(os.path.join(PY_Space, filename), os.path.join(PY_Space, newname))

file_rename('Source_File.csv', PY_Space)
File_Path = os.path.join(PY_Space , 'Source_File.csv')

#---------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n<> Data Processing for : '+ File_Path)

wb = xlrd.open_workbook(File_Path, logfile=open(os.devnull, 'wb'))
Data = pd.read_excel(wb ,header = None ,sheet_name = 'Table 1', engine = 'xlrd')

Data = Data.dropna(subset = [8, 12 ,15 ,17], how = 'all')
Data = Data[~Data[8].isin(['Seasonally adjusted at annual rates'])] #Droping unwanted row by specifying string value 
Data = Data.reset_index(drop = True)

Data.columns = Data.iloc[0,:].astype(str) + '|' + Data.iloc[1,:].astype(str)
Data = Data.drop(index = [0,1])
Data.rename(columns={ Data.columns[0]: "Number" }, inplace = True)

Data['nan|nan'] = Data['nan|nan'].str.replace('\d+', '')
Data['Indicator'] = Data['Number'].astype(str)+Data['nan|nan']
Data.drop(columns = ['Number','nan|nan'], inplace = True)

DataT = pd.melt(Data, id_vars=['Indicator'], var_name='Date', value_name='Value')
DataT[['first','second']] = DataT['Date'].str.split("|" , expand = True,)

DataT.loc[DataT['second'].str.contains('20'), 'second'] = '' 
DataT['second'] = DataT['second'].str.strip('r')
DataT = DataT.replace(np.nan, "", regex = True) 
DataT['Date'] = DataT['first'].astype(str) + DataT['second'].astype(str)

unwanted_Indicator = '14Change in private inventories', '15Net exports of goods and services'
for item in unwanted_Indicator:
    DataT = DataT[~DataT['Indicator'].isin([item])]

DataT['Indicator'] = DataT['Indicator'].str.upper().str.strip().map(Indicator_Map)

KN.PrintTF('\n<> CrossChecking for null Values in Columns')
Columns = 'Indicator','Date'
for column in Columns:
    if DataT[column].isnull().any():
        KN.PrintTF('\n>> Failed: Code Not Mapped properly, Please Check Column : '+ column)
        sys.exit()
    else:
        KN.PrintTF('>> Null values Not found in Column :' + column)

DataT['Unit'] = '%'
DataT.loc[DataT['Date'].str.contains('Q'), 'Frequency'] = 'Q'
DataT['Frequency'] = DataT['Frequency'].replace(np.nan,'A',regex = True)

DataT = DataT.drop(columns = ['first', 'second'])
Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataT, 'Value')
if len(Non_Numeric) > 0:
    sys.exit(1)

DataDup = KN.Duplicates_CrossCheck(DataT ,['Indicator','Date'])
if len(DataDup) > 0:
    sys.exit(1)

KN.TimeSeries_Count(DataT, ['Indicator','Frequency'], PY_Space)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
DataT.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

KN.PrintTF('\n Data File Exported to Path : ' + UploadFiles_Path)
#Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)






