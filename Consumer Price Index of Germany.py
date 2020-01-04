
import sys, os, time, csv, time, datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import KNG, KNMetaData, KNUpload

pd.set_option('display.max_colwidth', 10000)
pd.set_option('display.expand_frame_repr', False)

#---------------------------------------------------------------------------------------------------------------------------------------------------

KN = KNG.KNG()
Time = time.time()

Script_Path = os.path.dirname(os.path.realpath(__file__))
PY_Space = KN.Folder_Create(os.path.dirname(Script_Path), 'PY_Space')
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
URL_2 = ''
URL_3 = ''
URL_4 = ''
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
        if str.upper(Line).startswith('URL_3:'):
            URL_3 = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('URL_4:'):
            URL_4 = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('DATASET_ID:'):
            Dataset_Id = Line[Line.find(':')+1:len(Line)].strip()
        
        
KN.Check_Exists(ToolConfigF, [Host, App_Id, App_Secret, Dataset_Id, URL_1, URL_2, URL_3, URL_4 ])

KN.PrintTF('\n> Host: ' + Host)

##KN.Host = Host
##KN.App_Id = App_Id
##KN.App_Secret = App_Secret
##
##KNUpload_ToolPath = os.path.join(Script_Path, 'Tool_Knoema_Upload')            
##if not os.path.exists(KNUpload_ToolPath):
##    KN.PrintTF('\n>> Failed: Knoema Upload C# Tool Not Found @: ' + KNUpload_ToolPath)
##    sys.exit()
##
##KU = KNUpload.KNUpload()
##KU.Host = Host
##KU.App_Id = App_Id
##KU.App_Secret = App_Secret
##KU.Logs_Folder_Path = PY_Space    
##KU.KNUpload_ToolPath = KNUpload_ToolPath

Chrome_Driver_Path = os.path.join(Script_Path, 'chromedriver.exe')
if not os.path.exists(Chrome_Driver_Path):
    KN.PrintTF('\n>> Failed: "chromedriver.exe" NOT Exist in Script Location')
    sys.exit()

KN.PrintTF('\n> Download Source Files')
URLs = [URL_1, URL_2, URL_3, URL_4]

Options = Options()
Options.add_experimental_option("prefs", {"download.default_directory": PY_Space,
                                          "download.prompt_for_download": False,
                                          "download.directory_upgrade": True,
                                          "safebrowsing.enabled": True
                                          })

Driver = webdriver.Chrome(executable_path = Chrome_Driver_Path, chrome_options=Options)
for URL in URLs:
    Driver.get(URL)
    button = Driver.find_element_by_xpath('//*[@id="wrapperContent"]/div[2]/div/div[2]/div[1]/form/div/input')
    button.click()
    KN.PrintTF('\n> Data downloaded from the source:' + URL)
    time.sleep(10)
Driver.quit()

Downloaded_Files = ['61111-0001.xlsx','61111-0002.xlsx', '61111-0003.xlsx' , '61111-0004.xlsx']

#-----------------------------------------------------------------------------------------------------------------------------------------

Month_Map = {'Jan':'M01', 'Feb':'M02', 'Mär':'M03', 'Mar':'M03', 'Apr':'M04', 'Mai':'M05', 'May':'M05',
             'Jun':'M06', 'Jul':'M07', 'Aug':'M08', 'Sep':'M09', 'Okt':'M10', 'Oct':'M10', 'Nov':'M11', 'Dez':'M12', 'Dec':'M13'}

Dimensions = ['Indicator', 'Measure']
Header = ['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value']

Month_Map = dict((Key.upper(), Value.upper()) for Key, Value in Month_Map.items()) # Dictinary Upper Case
DataAll = pd.DataFrame()

#-----------------------------------------------------------------------------------------------------------------------------------------

Source_File_Name = [File for File in Downloaded_Files if File.startswith('61111-0002')] # # Get Recent Files
Source_File_Name = ''.join(Source_File_Name)
KN.PrintTF("\n<1> Reading Data for Consumer Price Index, File: " + Source_File_Name)

Source_File_Path = os.path.join(PY_Space, Source_File_Name)
Data = pd.read_excel(Source_File_Path, dtype=object)
DataN = Data.iloc[:, 0:3] # Keep only first 3 Columns
DataN.columns = ['Year', 'Month', 'Value']
DataN = DataN.dropna(subset=['Month'])

DataN = DataN.reset_index(drop=True)
DataN = DataN.iloc[0:].fillna(method='ffill')
DataN['M'] = DataN['Month'].str[:3].apply(str.upper).map(Month_Map)
DataN['Date'] = DataN['Year'].astype(str) + DataN['M'].astype(str)

DataN.loc[DataN['Date'].str.contains('M'), 'Frequency'] = 'M'
KN.Export_Dimension_ListItems(DataN, ['Year', 'Month', 'M', 'Date','Frequency'], PY_Space, 'Date_F1')

DataN = DataN.loc[~DataN['Value'].isin(['...'])]
Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataN, 'Value')
if len(Non_Numeric) > 0:
    sys.exit()

DataN['Indicator'] = 'CC13-99'    

DataDup = KN.Duplicates_CrossCheck(DataN, ['Indicator', 'Date'])
if len(DataDup) > 0:
    sys.exit()

KN.PrintTF('\n> %s - Data: %s'%(Source_File_Name, len(DataN)))
DataAll = DataAll.append(DataN[['Indicator', 'Frequency', 'Date', 'Value']])

del Data
del DataN

#----------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n' + '-'*150)
Source_File_Name = [File for File in Downloaded_Files if File.startswith('61111-0004')]
Source_File_Name = ''.join(Source_File_Name)

KN.PrintTF("\n<2> Reading Data for Other Indicators, File: " + Source_File_Name)
Source_File_Path = os.path.join(PY_Space, Source_File_Name)

Data = pd.read_excel(Source_File_Path, index = False, header = None)
Data.drop(columns=[1], inplace = True) # Delete Second Column
DataN = Data.iloc[4:]
DataN = DataN.reset_index(drop=True)

DataN.iloc[0:1] = DataN.iloc[0:1].fillna(method='ffill', axis=1) # Fill previous column value
DataN.fillna('', inplace=True)
DataN.columns = DataN.iloc[0,:] + '|' + DataN.iloc[1,:]

DataN.rename(columns={DataN.columns[0]:'Indicator'}, inplace = True)
DataN = DataN.iloc[2:,]
DataN.dropna(subset=['Indicator'],inplace = True)

DataT = pd.melt(DataN, id_vars=['Indicator'], var_name='DateS', value_name='Value')
DataT.dropna(subset=['Value'],inplace = True)

DataT['Year'] = [s.split('|')[0] for s in DataT['DateS']]
DataT['Month'] = [s.split('|')[1] for s in DataT['DateS']]
DataT['Month'] = DataT['Month'].str.strip()
DataT['Month'] = DataT['Month'].str.upper()

Month_Map = dict((Key.upper(), Value.upper()) for Key, Value in Month_Map.items())
DataT['M'] = DataT['Month'].str[:3].apply(str.upper).map(Month_Map)
DataT['Date'] = DataT['Year'].astype(str) + DataT['M'].astype(str)

DataT.loc[DataT['Date'].str.contains('M'), 'Frequency'] = 'M'
KN.Export_Dimension_ListItems(DataT, ['DateS', 'Year', 'Month', 'M', 'Date','Frequency'], PY_Space, 'Date_F2')

DataT = DataT.loc[~DataT['Value'].isin(['...', ''])]
Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataT, 'Value')
if len(Non_Numeric) > 0:
    sys.exit()

DataDup = KN.Duplicates_CrossCheck(DataT, ['Indicator', 'Date'])
if len(DataDup) > 0:
    sys.exit()

KN.PrintTF('\n> %s - Data: %s'%(Source_File_Name, len(DataT)))

DataAll = DataAll.append(DataT[['Indicator', 'Frequency', 'Date', 'Value']])

#----------------------------------------------------------------------------------------------------------------------------------------

DataAll['Measure'] = 'KN.M2'
DataAll['Unit'] = 'Index, 2015=100'

DataDup = KN.Duplicates_CrossCheck(DataAll , Dimensions + ['Date'])
if len(DataDup) > 0:
    sys.exit()

KN.TimeSeries_Count(DataAll, Dimensions + ['Frequency'], PY_Space)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
DataAll.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

#Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

#KN.TPE(Time)

#-----------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n' + '-'*150)
print('Processing started from Annual Data >>Please Wait\n')
#Source_File_Name = [File for File in Downloaded_Files if File.startswith('61111-0003')]
Source_File_Name = [File for File in Downloaded_Files if File.endswith('3.xlsx')]
Source_File_Name = ''.join(Source_File_Name)
KN.PrintTF("\n<1> Reading Annual Data for Consumer Price Index, File: " + Source_File_Name)

Source_File_Path = os.path.join(PY_Space, Source_File_Name)

DataA = pd.read_excel(Source_File_Path , dtype = object)
DataA.drop(columns = ['Unnamed: 1'], inplace = True)
DataA.dropna(subset=['Unnamed: 2','Unnamed: 3','Unnamed: 4','Unnamed: 5','Unnamed: 6'],inplace = True)

DataA.columns = DataA.iloc[0]  #changing the column header
DataA.reset_index(drop = True , inplace = True)

DataA = DataA.drop(0)        #when droping rows pls assign it to the same  variable name

DataTA = pd.melt(DataA, id_vars=['Verwendungszwecke des Individualkonsums'],var_name='Date', value_name='Value')
DataTA.rename(columns = {'Verwendungszwecke des Individualkonsums' : 'Indicator'} , inplace = True)

DataTA1 = DataTA[['Indicator' , 'Date', 'Value']]
del DataTA
                            
DataTA1.drop_duplicates(keep = False, inplace = True)
DataTA1 = DataTA1.loc[~DataTA1['Value'].isin(['.', ''])]
                            
#---------------------------------------------------------------------------------------------------------------------------------------
                            
KN.PrintTF('\n' + '-'*150)                            
#Source_File_Name = [File for File in Downloaded_Files if File.startswith('61111-0001')] #  Get Recent Files
Source_File_Name = [File for File in Downloaded_Files if File.endswith('1.xlsx')]
Source_File_Name = ''.join(Source_File_Name)
KN.PrintTF("\n<1> Reading Annual Data for Consumer Price Index, File: " + Source_File_Name)
Source_File_Path = os.path.join(Source_File_Name)

DataA = pd.read_excel(Source_File_Path , dtype = object)
DataA.drop(columns = ['Unnamed: 2'], inplace = True)
DataA.dropna(subset=['Verbraucherpreisindex (inkl. Veränderungsraten):\nDeutschland, Jahre','Unnamed: 1'],inplace = True)

DataA.columns = DataA.iloc[0]  #changing the column header
DataA.reset_index(drop = True , inplace = True)

DataA = DataA.drop(0)        #when droping rows pls assign it to the same  variable name
DataA.rename(columns = {'Jahr' : 'Date' , 'Verbraucherpreisindex' : 'Value'} , inplace = True)
DataA['Indicator'] = 'CC13-99'

##DataA['Frequency'] = 'A'
##DataA['Unit'] =  'Index, 2015=100'
##DataA['Measure'] = 'KN.M2'

#DataA1 = DataA[['Indicator' , 'Frequency', 'Date', 'Value' , 'Measure', 'Unit']]
DataA1 = DataA[['Indicator' , 'Date', 'Value' ]]    #changing the order of columns
del DataA
DataA1.drop_duplicates(keep = False, inplace = True) 

print("Combining the DataFrames\n>>")  
DataAll2 = pd.concat([DataA1,DataTA1],ignore_index = True)

DataAll2['Frequency'] = 'A'
DataAll2['Unit'] =  'Index, 2015=100'
DataAll2['Measure'] = 'KN.M2'

DataAll2 = DataAll2.loc[~DataAll2['Value'].isin(['-', ''])]
Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataAll2, 'Value')
if len(Non_Numeric) > 0:
    KN.PrintTF('Non numeric values was  Found in Value , please Check\n')
    sys.exit()
else:
    KN.PrintTF('Non numeric value was not found')

DataDup = KN.Duplicates_CrossCheck(DataAll2 , Dimensions + ['Date'])
if len(DataDup) > 0:
    KN.PrintTF('Duplicate Values found , Please Check\n')
    sys.exit()
else:
    KN.PrintTF('Duplicate Values was not found in Data\n')

KN.TimeSeries_Count(DataAll2, Dimensions + ['Frequency'], PY_Space)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll2.csv')
DataAll2.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

##Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

##KN.TPE(Time)


