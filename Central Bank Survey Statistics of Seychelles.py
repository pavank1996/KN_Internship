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

Script_Path = os.path.dirname(os.path.realpath(__file__))
PY_Space = KN.Folder_Create(os.path.dirname(Script_Path), 'PY_Space_')
KN.Delete_All_Files(PY_Space)
KN.Logs_Folder_Path = PY_Space


ToolConfigF = os.path.join(os.path.dirname(Script_Path), 'Tool Config.txt')        
if not os.path.exists(ToolConfigF):
    KN.PrintTF('\n>> Failed: "Tool Config.txt" NOT Found')
    sys.exit()

Host = ''
App_Id = ''
App_Secret = ''
URL_1 = ''
Dataset_Id = ''
    
with open(ToolConfigF) as ToolConfig:
    for Line in ToolConfig:
        if str.upper(Line).startswith('HOST:'):
            Host = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('APP_ID:'):
            App_Id = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('APP_SECRET:'):
            App_Secret = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('URL_1:'):
            URL_1 = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('URL_2:'):
            URL_2 = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('DATASET_ID:'):
            Dataset_Id = Line[Line.find(':')+1:len(Line)].strip()

KN.Check_Exists(ToolConfigF, [Host, App_Id, App_Secret, Dataset_Id, URL_1])

KN.PrintTF('\n> Host: ' + Host)

KN.Host = Host
KN.App_Id = App_Id
KN.App_Secret = App_Secret

KNUpload_ToolPath = os.path.join(Script_Path, 'Tool_Knoema_Upload')            
if not os.path.exists(KNUpload_ToolPath):
    KN.PrintTF('\n>> Failed: Knoema Upload C# Tool Not Found @: ' + KNUpload_ToolPath)
    sys.exit()

KU = KNUpload.KNUpload()
KU.Host = Host
KU.App_Id = App_Id
KU.App_Secret = App_Secret
KU.Logs_Folder_Path = PY_Space    
KU.KNUpload_ToolPath = KNUpload_ToolPath

KN.PrintTF('\n> Downloading Source Files')
URL = URL_1

def source_download(URL):
    wget.download(URL , PY_Space)
    KN.PrintTF('\n> File Downloaded from the Source : ' + URL)

source_download(URL)

Downloaded_Files = ['Monetary Survey.xlsx']

#------------------------------------------------------------------------------------------------------------------------------------------------------

Source_File_Name = [File for File in Downloaded_Files if File.startswith('Monetary Survey')]
Source_File_Name = ''.join(Source_File_Name)

KN.PrintTF("\n<2> Reading Data from file : " + Source_File_Name)
Source_File_Path = os.path.join(PY_Space, Source_File_Name)

Data = pd.read_excel(Source_File_Path, sheet_name = 'Central Bank Survey' , header = None ,dtype = object)
Data.dropna(subset = [2,50,100] , inplace = True)

Data.columns = Data.iloc[0]  
Data.drop(Data.index[0])

Data = Data.reset_index(drop = True)
Data.drop([0] , inplace = True)

Data = Data.rename(columns={np.nan: 'Indicator'})
Data.columns = Data.columns.fillna('Indicator')

Data = pd.melt(Data , id_vars=['Indicator'],var_name='Date', value_name='Value')

KN.PrintTF("\nMapping the Indicator code")

Source_File_Path = os.path.join(Script_Path, "knox.xlsx")
Code_data = pd.read_excel(Source_File_Path, sheet_name = 'Indicator' ,usecols = ["Name" , "Code"] ,dtype = object , index = False)
Code_data = Code_data.set_index('Name')['Code'].to_dict()

Code_data = dict((Key.upper(), Value.upper()) for Key, Value in Code_data.items())
Data['Indicator'] = Data['Indicator'].str.strip().str.upper().map(Code_data)

Data[['Year','month','day']] = Data.Date.astype(str).str.split("-" , expand =True,)
Data['Date'] = Data['Year'].astype(str) +"M" + Data['month'].astype(str)

Data.drop(columns = ['Year', 'month','day'] , inplace = True)
Data.loc[Data['Date'].str.contains('M'), 'Frequency'] = 'M'
Data["unit"] = "SCR million, EOP"

KN.PrintTF('\n> %s - Data: %s'%(Source_File_Name, len(Data)))

Data = Data.loc[~Data['Value'].isin(['...', ''])]
Non_Numeric = KN.NonNumeric_CrossCheck_V2(Data, 'Value')
if len(Non_Numeric) > 0:
    sys.exit()

DataDup = KN.Duplicates_CrossCheck(Data, ['Indicator', 'Date'])
if len(DataDup) > 0:
    sys.exit()
    
DataAll = pd.DataFrame()
DataAll.append(Data[['Indicator', 'Frequency', 'Date', 'Value']])

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
DataAll.to_csv(Export_PathF, index=False, encoding='utf-8-sig')

Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)






