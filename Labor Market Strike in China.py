import sys, os, time, time, datetime
import pandas as pd
from googletrans import Translator
import requests
import csv
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
  KN.PrintTF('\n>> Failed: Mapping file "Mapping_File.xlsx" NOT found')
  sys.exit(1)

#--------------------------------------------------------------------------------------------------------------------------------------------------

Dimensions = ['Indicator', 'Type of Store']
Header = ['ID', 'Title', 'Description', 'Province', 'Number of Deaths',	'Number of Injuries', 'Number of Employees', 'Industry', 'Type',
          'Number of Person Punished', 'Financial Penalty', 'Latitude', 'Longitude', 'Date']

#---------------------------------------------------------------------------------------------------------------------------------------------------

obj = SourceData.Download()
obj.PY_Space = PY_Space
obj.URL = URL

obj.Source_Data_Download()

#---------------------------------------------------------------------------------------------------------------------------------------------------

Mapping_File = pd.read_excel(Mapping_File_Path, dtype=object)
RegionID_Dict = Mapping_File.set_index('Province')['RegionId'].to_dict()
	
#---------------------------------------------------------------------------------------------------------------------------------------------------

File_Path = os.path.join(PY_Space, 'Export.xlsx')
Data = pd.read_excel(File_Path , dtype = object)

Data[['Province', 'Industry','Participants']] = Data[['Province', 'Industry','Participants']].fillna(value='')
Data.dropna(how = 'all',subset = ['Province', 'Industry','Participants'] ,inplace = True)
Data.drop(columns = ['Latitude' ,'Longitude'] ,inplace = True)

RegionID_Dict = dict((Key.upper(), Value.upper()) for Key, Value in RegionID_Dict.items())
DataAll['RegionID'] = DataAll['RegionID'].apply(str.upper).map(RegionID_Dict)

KN.PrintTF('\n<> Cross checking Mapped Values')
if DataAll['RegionID'].isnull().any():
    KN.PrintTF("\n>> Failed: Values Not Mapped Properly, Please Cross Check file: " + Mapping_File_Path)
    sys.exit(1)
else:
    KN.PrintTF('\n>>Null Values Not Found In the Column ,Exporting Flat File for Upload Process')
      
UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
Data.to_csv(Export_PathF, index=False, encoding='utf-8-sig')

Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)


