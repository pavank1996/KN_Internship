import pandas as pd
import regex as re 
import csv
import KNG, KNMetaData, KNUpload
import datetime, time
import os,sys,csv
from bs4 import BeautifulSoup
import requests
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

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

#---------------------------------------------------------------------------------------------------------------------------------------------------

cookies = {
    'ASP.NET_SessionId': 'lswqqprulnnbiwaogfdr4gwm',
    'TS0150405d_28': '01637e37f25282f82ade210ba547d80a84eec306ae482b02970c0abf42121713d81f9542bda8cf1201086f223d14bba7cfcce93cf3',
    '__utma': '23215407.1077924861.1577531617.1577531617.1577531617.1',
    '__utmc': '23215407',
    '__utmz': '23215407.1577531617.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
    'TS0150405d': '01b61ca5dd600357b599595716d5f51ae05793bd76c76cfb349561b68c8af4b61a7457db3203499821f2378aa4d705f8dcdcf9549c19e698681a5ec36c03de3fdd99856da6',
    '__utmt': '1',
    '__utmb': '23215407.5.10.1577531617',
}

headers = {
    'Connection': 'keep-alive',
    'Origin': 'https://www.data.bsee.gov',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Accept': '*/*',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://www.data.bsee.gov/Production/OCSProduction/Default.aspx',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8,he;q=0.7',
}

data = {
  '__EVENTTARGET': '',
  '__EVENTARGUMENT': '',
  '__VIEWSTATE': '/wEPDwUJLTEzNjg1MDkxD2QWAmYPZBYCAgMPZBYMAg0PPCsABAEADxYCHgVWYWx1ZQUuT3V0ZXIgQ29udGluZW50YWwgU2hlbGYgT2lsIGFuZCBHYXMgUHJvZHVjdGlvbmRkAg8PPCsABAEADxYEHwAFLGRhdGEgbGFzdCB1cGRhdGVkOiAgMTItMDItMjAxOSAxMDo1NiBBTShDU1QpHgdWaXNpYmxlZ2RkAhEPFCsABA8WAh8BZ2RkZDwrAAUBABYCHghJbWFnZVVybAUfL2JzZWUvaW1hZ2VzL2ljb25zL3R1dG9yaWFsLnBuZ2QCEw88KwAEAQAPFgQfAAUEbm9uZR8BaGRkAhUPPCsABAEADxYCHwFoZGQCGw9kFgICAQ9kFgJmD2QWAgIBDzwrABMCDhQrAAJkZBIUKwACZGRkGAEFHl9fQ29udHJvbHNSZXF1aXJlUG9zdEJhY2tLZXlfXxYEBTZjdGwwMCRDb250ZW50UGxhY2VIb2xkZXIxJEFTUHhEb2N1bWVudFZpZXdlcjEkU3BsaXR0ZXIFQ2N0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjEkQVNQeERvY3VtZW50Vmlld2VyMSRTcGxpdHRlciRUb29sYmFyJE1lbnUFXGN0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjEkQVNQeERvY3VtZW50Vmlld2VyMSRTcGxpdHRlciRUb29sYmFyJE1lbnUkSVRDTlQ1JFRDJFBhZ2VOdW1iZXIkREREBV1jdGwwMCRDb250ZW50UGxhY2VIb2xkZXIxJEFTUHhEb2N1bWVudFZpZXdlcjEkU3BsaXR0ZXIkVG9vbGJhciRNZW51JElUQ05UMTEkVEMkU2F2ZUZvcm1hdCRERETbZJk56ZSqsovdX30xSaDVrByvbw0WKagiU8imDl5u7w==',
  '__VIEWSTATEGENERATOR': '92AE0737',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_CS': '[{"st":"px","s":30,"c":0,"spt":0,"spl":0},{"i":[{"s":100,"st":"%","c":0,"spt":0,"spl":0},{"st":"px","s":195,"c":1,"i":[{},{"c":1}]}],"s":234,"st":"px","c":0}]',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_Toolbar_Menu_ITCNT5_PageNumber_VI': '',
  'ctl00$ContentPlaceHolder1$ASPxDocumentViewer1$Splitter$Toolbar$Menu$ITCNT5$TC$PageNumber': '',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_Toolbar_Menu_ITCNT5_PageNumber_DDDWS': '0:0:-1:-10000:-10000:0:-10000:-10000:1:0:0:0',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_Toolbar_Menu_ITCNT5_PageNumber_DDD_LDeletedItems': '',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_Toolbar_Menu_ITCNT5_PageNumber_DDD_LInsertedItems': '',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_Toolbar_Menu_ITCNT5_PageNumber_DDD_LCustomCallback': '',
  'ctl00$ContentPlaceHolder1$ASPxDocumentViewer1$Splitter$Toolbar$Menu$ITCNT5$TC$PageNumber$DDD$L': '',
  'ctl00$ContentPlaceHolder1$ASPxDocumentViewer1$Splitter$Toolbar$Menu$ITCNT6$TC$PageCount': '1',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_Toolbar_Menu_ITCNT11_SaveFormat_VI': 'pdf',
  'ctl00$ContentPlaceHolder1$ASPxDocumentViewer1$Splitter$Toolbar$Menu$ITCNT11$TC$SaveFormat': 'Pdf',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_Toolbar_Menu_ITCNT11_SaveFormat_DDDWS': '0:0:-1:-10000:-10000:0:-10000:-10000:1:0:0:0',
  'ctl00$ContentPlaceHolder1$ASPxDocumentViewer1$Splitter$Toolbar$Menu$ITCNT11$TC$SaveFormat$DDD$L': 'pdf',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_ViewerDXCurrentPageIndex': '0',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_ViewerDXCacheKey': '',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_ViewerDXParameters': '',
  'ContentPlaceHolder1_ASPxDocumentViewer1_Splitter_ViewerDXRemote': '',
  'DXScript': '1_171,1_94,9_13,9_10,9_8,1_164,1_163,9_12,1_91,1_156,1_162,1_147,1_114,1_121,1_113,1_154,1_116,9_9',
  'DXCss': '1_4,1_12,1_5,1_3,9_1,9_0,1_10,1_1,9_15,/bsee/images/icons/favicon.ico,/bsee/css/bsee-omega.normalize.css,/bsee/css/bsee-data.styles.css,/bsee/css/shared2.css,/bsee/css/print-home.css',
  '__CALLBACKID': 'ctl00$ContentPlaceHolder1$ASPxDocumentViewer1',
  '__CALLBACKPARAM': 'c0:page=',
  '__EVENTVALIDATION': '/wEdAATKS2LB9FVlH2d4YygEMTsmyfTLKEoPzNfdNhCl7u7HwTUjb+lHE27hg3/NFaBFsDfbbKHXuEdfelZR8GZlxl53eRZ6HcPvKP/62aL4PyBUNwxqU80mnvmx3iUhwGUrLFM='
}

KN.PrintTF('\n>> Exctracting Data using Beautiful Soup for :'+ URL)

try:
    res = requests.post(URL, headers=headers, cookies=cookies, data=data)
except:
    KN.PrintTF('\n<> ! Source Issue found ,Process Stoped')
    sys.exit(1)
    
print('\n<> URL present status Code = ',(res.status_code))
soup = BeautifulSoup(res.text,"lxml")
table = soup.find('table')

list_of_rows = []
for row in table.findAll('tr'):
    list_of_cells = []
    for cell in row.findAll(["td"]):
        text = cell.text
        list_of_cells.append(text)
    list_of_rows.append(list_of_cells)

for item in list_of_rows:
    ' '.join(item)
  
Data = pd.DataFrame(list_of_rows)
Export_Path = os.path.join(PY_Space, 'Source_Data.csv')
KN.PrintTF('\n>> Raw Data Exported to path: '+ Export_Path)

Data.to_csv(Export_Path,index = False)
print('-'*75)

#------------------------------------------------------------------------------------------------------------------------------------------------------

Header = ['Region', 'Indicator', 'Frequency', 'Value', 'Date']
Dimensions = ['Indicator','Region','Value']

Region_map = {'Tot':'KN.R1','Ala':'KN.R2','Pac':'KN.R3','Gul':'KN.R4'}
Region_map = dict((k.upper(), v.upper()) for k,v in Region_map.items())

Month_Map  = {'jan':'M01','feb':'M02','mar':'M03','apr':'M04','may':'M05','jun':'M06','jul':'M07','aug':'M08','sep':'M09','oct':'M10','nov':'M11','dec':'M12'}
Month_Map = dict((k.upper(),v.upper()) for k,v in Month_Map.items())

DataAll = pd.DataFrame()

#------------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n<> Data Processing for, *** Anual Data ***')
File_Path = Export_Path
Data = pd.read_csv(File_Path ,header = None , dtype=object)

Data = Data.loc[:18, 1:6]
Data.dropna(subset = [2,5], how = 'all', inplace =True)

Data = Data.drop_duplicates(subset=[3,5], keep=False)
Data.dropna(subset = [1], inplace =True)
if Data[1].isnull().any():
    Data = Data.drop(columns = [1])
    
Data.drop(Data.index[0],inplace = True)
Data.reset_index(drop = True)

New_Header = Data.iloc[0] 
Data = Data[1:] 
Data.columns = New_Header #Changing the Header

Data = pd.melt(Data, id_vars=['Year'],var_name='Region', value_name='Value')
Data.rename(columns = {'Year': 'Date'}, inplace = True)
Data.dropna(subset = ['Value'] , inplace = True)
Data['Indicator'] = 'KN.I1'
Data['Frequency'] = 'A'

Data['Region'] = Data['Region'].str[:3].str.upper().str.strip().map(Region_map)

DataAll = DataAll.append(Data)
print(Data.head(3))
print('-'*75)

#-----------------------------------------------------------------------------------------------------------------------------------------------------

Data = pd.read_csv(File_Path ,header = None , dtype=object)

Data = Data.loc[19:34, 1:6]
Data.dropna(subset = [2,5], how = 'all', inplace =True)

Data = Data.drop_duplicates(subset=[3,5], keep=False)
Data.dropna(subset = [1], inplace =True)
if Data[1].isnull().any():
    Data = Data.drop(columns = [1])
Data.reset_index(drop = True)

New_Header = Data.iloc[0] 
Data = Data[1:] 
Data.columns = New_Header

Data = pd.melt(Data, id_vars=['Year'],var_name='Region', value_name='Value')
Data.rename(columns = {'Year': 'Date'}, inplace = True)
Data.dropna(subset = ['Value'] , inplace = True)
Data['Indicator'] = 'KN.I2'
Data['Frequency'] = 'A'

Data['Region'] = Data['Region'].str[:3].str.upper().str.strip().map(Region_map)

DataAll = DataAll.append(Data)
print(Data.head(3))
print('-'*75)

#---------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n<> Data Processing for, *** Monthly Data ***')
Data = pd.read_csv(File_Path ,header = None , dtype=object)

Data = Data.loc[35:, 1:6]
Data.dropna(subset = [2,5], how = 'all', inplace =True)

Data = Data.drop_duplicates(subset=[3,5], keep=False)
Data = Data.reset_index(drop = True)
                 
New_Header = Data.iloc[1] 
Data = Data[2:] 
Data.columns = New_Header 

Data = pd.melt(Data, id_vars=['Month'],var_name='Region', value_name='Value')
Data.dropna(subset = ['Value'] , inplace = True)
Data['Indicator'] = 'KN.I1'
Data['Frequency'] = 'M'

Data['Region'] = Data['Region'].str[:3].str.upper().str.strip().map(Region_map)

Data['Month'].replace(u'\xa0',u'', regex=True, inplace=True)
Data[['month','year']] = Data.Month.str.split("2" , expand = True,)                             
Data['month'] = Data['month'].str[:3].str.upper().str.strip().map(Month_Map)

Data['Date'] = '2'+ Data['year'].astype(str) + Data['month'].astype(str)

columns = 'Month', 'month', 'year'
for column in columns:
    Data.drop(columns = [column], inplace = True)

DataAll = DataAll.append(Data)
print(Data.head(3))
print('-'*75)

#-------------------------------------------------------------------------------------------------------------------------------------------------

Data = pd.read_csv(File_Path ,header = None , dtype=object)

Data = Data.loc[35:, 7:14]
Data.dropna(subset = [7,10,14], how = 'all', inplace =True)
Data = Data.drop_duplicates(subset=[7,10,14], keep=False)
    
Data = Data.reset_index(drop = True)
                 
New_Header = Data.iloc[0] 
Data = Data[1:] 
Data.columns = New_Header 

Data = pd.melt(Data, id_vars=['Month'],var_name='Region', value_name='Value')
Data.dropna(subset = ['Value'] , inplace = True)
Data['Indicator'] = 'KN.I2'
Data['Frequency'] = 'M'

Data['Region'] = Data['Region'].str[:3].str.upper().str.strip().map(Region_map)

Data['Month'].replace(u'\xa0',u'', regex=True, inplace=True)
Data[['month','year']] = Data.Month.str.split("2" , expand = True,)                             
Data['month'] = Data['month'].str[:3].str.upper().str.strip().map(Month_Map)

Data['Date'] = '2'+ Data['year'].astype(str) + Data['month'].astype(str)

columns = 'Month', 'month', 'year'
for column in columns:
    Data.drop(columns = [column], inplace = True)

DataAll = DataAll.append(Data)
print(Data.head(3))
print('-'*75)

#---------------------------------------------------------------------------------------------------------------------------------------------------

DataAll.dropna(subset = ['Value'],inplace = True)
DataAll['Value'] = DataAll['Value'].str.replace(',', '')

KN.PrintTF('\n<> CrossChecking for null Values in Indiacator Column')
if Data['Region'].isnull().any():
    KN.PrintTF('\n>> Failed: Region Code Not Mapped properly, Please Check')
    sys.exit()
else:
    KN.PrintTF('\n>> Null values Not found')

Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataAll, 'Value')
if len(Non_Numeric) > 0:
    sys.exit(1)

DataDup = KN.Duplicates_CrossCheck(DataAll , Dimensions + ['Date'])
if len(DataDup) > 0:
    sys.exit(1)

KN.TimeSeries_Count(DataAll, ['Region','Indicator','Frequency'], PY_Space)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
DataAll.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

KN.PrintTF('\n Data File Exported to Path : ' + UploadFiles_Path)
#Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)
print('-'*75)

