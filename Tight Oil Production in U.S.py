import pandas as pd
import KNG, KNMetaData, KNUpload
import sys, os, time, csv, time, datetime
import wget
import requests
import numpy as np

pd.set_option('display.max_colwidth', 10000)
pd.set_option('display.expand_frame_repr', False)

KN = KNG.KNG()
Time = time.time()

S_Path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PY_Space = KN.Folder_Create(os.path.join(S_Path), 'PY_Space')
KN.Delete_All_Files(PY_Space)
KN.Logs_Folder_Path = PY_Space

ToolConfigF = os.path.join(S_Path, 'Tool Config.txt')        
if not os.path.exists(ToolConfigF):
    KN.PrintTF('\n>> Failed: "Tool Config.txt" NOT Found')
    sys.exit()

Host = ''
App_Id = ''
App_Secret = ''
URL = ''
Dataset_Id = ''
    
with open(ToolConfigF) as ToolConfig:
    for Line in ToolConfig:
        if str.upper(Line).startswith('HOST:'):
            Host = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('APP_ID:'):
            App_Id = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('APP_SECRET:'):
            App_Secret = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('URL:'):
            URL = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('DATASET_ID:'):
            Dataset_Id = Line[Line.find(':')+1:len(Line)].strip()

KN.Check_Exists(ToolConfigF, [Host, App_Id, App_Secret, Dataset_Id, URL])

KN.PrintTF('\n> Host: ' + Host)
KN.PrintTF('\n> Dataset Id: ' + Dataset_Id)

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

Mapping_File_Path = os.path.join(S_Path, 'KnoxFile.xlsx')
if not os.path.exists(Mapping_File_Path):
  KN.PrintTF('\n>> Failed: Mapping file "Mapping_File.xlsx" NOT found')
  sys.exit()

#---------------------------------------------------------------------------------------------------------------------------------------------------

query = {'Data': 'Oil Production'}
response = requests.post(URL, data=query)
print('\n<> URL Status Code : %s '%(response.status_code))

KN.PrintTF('\n<> Downloading Source File from URL :' + URL)

FileName = wget.download(URL , PY_Space)
KN.PrintTF('\n> File Downloaded from the Source, Exported to Path : ' + FileName)

#----------------------------------------------------------------------------------------------------------------------------------------------------

Indicator_Map_File = pd.read_excel(Mapping_File_Path, dtype=object, delim_whitespace=True)
Indicator_Map = Indicator_Map_File.set_index('Indicator Name')['Indicator'].to_dict()
Indicator_Map = dict((k.upper(), v.upper()) for k,v in Indicator_Map.items())

Header = ['Indicator', 'Unit', 'Frequency', 'Date', 'Value']
Dimensions = ['Indicator','Value']
#-----------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n>> Procesing file :' + FileName)
Data = pd.read_excel(r"C:\Pavan$uvarna\TRY\USROP2019\US-tight-oil-production.xlsx"  ,dtype=object)

KN.PrintTF('\n<> Extracting Unit')
DataUnit = Data.loc[1 , 'Unnamed: 12']

if len(DataUnit) > 0:
    KN.PrintTF('\n<> Unit of Data : '+ DataUnit)
elif DataUnit != 'million bbl per day':
    print('\n> Unit Changed')
elif len(DataUnit) == 0:
    DataUnit = 'million bbl per day'
else:
    KN.PrintTF('\n>> Unit Not found in the Dataframe')
    sys.exit()
    
Data = Data.loc[:, ~Data.columns.str.contains('^Unnamed')]

Data = pd.melt(Data, id_vars=['Date'],var_name='Indicator', value_name='Value')
Data['Indicator'] = Data['Indicator'].str[:4].str.upper().str.strip().map(Indicator_Map)

KN.PrintTF('\n>> CrossChecking for null Values in Indiacator Column')
if Data['Indicator'].isnull().any():
    KN.PrintTF('\n>> Failed: Indicator Code Not Mapped properly, Please Check')
    sys.exit()
else:
    KN.PrintTF('\n>> Null values Not found')

Data['Date'] = pd.to_datetime(Data.Date)  # Changing the Date format : 'YMD'

Data.reset_index(inplace=True)
Data.index = pd.to_datetime(Data.index)
Data['month'] = Data['Date'].dt.month

Data['year'] = Data['Date'].dt.year
Data['Date'] = Data['year'].astype(str) + 'M' + Data['month'].astype(str)

Data['Unit'] = DataUnit
Data['Frequency'] = 'M'

Non_Numeric = KN.NonNumeric_CrossCheck_V2(Data, 'Value')
if len(Non_Numeric) > 0:
    sys.exit(1)

DataDup = KN.Duplicates_CrossCheck(Data , Dimensions + ['Date'])
if len(DataDup) > 0:
    sys.exit(1)

KN.TimeSeries_Count(Data, ['Indicator','Frequency'], PY_Space)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
Data.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

KN.PrintTF('\n Data File Exported to Path : ' + UploadFiles_Path)

Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)







