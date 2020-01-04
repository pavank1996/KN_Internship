import pandas as pd
import KNG, KNMetaData, KNUpload
import sys, os, time, csv, time, datetime

pd.set_option('display.max_colwidth', 10000)
pd.set_option('display.expand_frame_repr', False)

#----------------------------------------------------------------------------------------------------------------------------------------

KN = KNG.KNG()
Time = time.time()

S_Path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PY_Space = KN.Folder_Create(S_Path, 'PY_Space')
KN.Logs_Folder_Path = PY_Space

ToolConfigF = os.path.join(S_Path, 'Tool Config.txt')        
if not os.path.exists(ToolConfigF):
    KN.PrintTF('\n>> Failed: "Tool Config.txt" NOT Found')
    sys.exit()

Host = ''
App_Id = ''
App_Secret = ''
Dataset_Id = ''
    
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

#----------------------------------------------------------------------------------------------------------------------------------------

Month_Map = {'Jan':'M01', 'Ene':'M01', 'Feb':'M02', 'Mar':'M03', 'Apr':'M04', 'Abr':'M04' , 'May':'M05',
             'Jun':'M06', 'Jul':'M07', 'Aug':'M08','Ago':'M08', 'Sep':'M09', 'Oct':'M10', 'Nov':'M11', 'Dic':'M12', 'Dec':'M12'}

Map_Dict = {'TI%':'KN.M1', 'TT%':'KN.N2'}
Dimensions = ['Indicator','Type of Store']
Header = ['Indicator','Type of Store', 'Unit', 'Frequency', 'Date', 'Value']

#-----------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF("\n> Reading HTML Table from the Source: " + URL)
Table = pd.read_html(URL)

Source_File_Path = os.path.join(PY_Space, 'Retail_Sales_Statistics.csv')
for Table in Table:
    Table.to_csv(Source_File_Path, ',')

Data = pd.read_csv(Source_File_Path, header=None, dtype=object)
Data.drop(columns = [0], inplace = True )

Data = Data.dropna(subset= [2, 3])
Data.rename(columns = {1:'YearS', 2:'MonthS', 3:'TI%', 4:'TT%'}, inplace = True)
Data.drop([0], inplace = True)

Month_Map = dict((Key.upper(), Value.upper()) for Key, Value in Month_Map.items())
Data['Month'] = Data['MonthS'].str[:3].apply(str.upper).map(Month_Map)
Data['Year'] = [s.split('.')[0] for s in Data['YearS']]

Data['Date'] = Data['Year'].astype(str) + Data['Month'].astype(str)
KN.Export_Dimension_ListItems(Data, ['YearS', 'MonthS', 'Year', 'Month', 'Date'], PY_Space, 'YearF')

Data.drop(columns = ['YearS', 'MonthS', 'Month', 'Year'], inplace = True)

DataT = pd.melt(Data, id_vars=['Date'], var_name='Type of Store', value_name='Value')
DataT.dropna(subset=['Value'],inplace = True)
DataT.replace({"Type of Store": Map_Dict}, inplace = True)

DataT['Indicator'] = 'KN.N1'
DataT['Unit'] = '%'
DataT['Frequency'] = 'M'

#---------------------------------------------------------------------------------------------------------------------------------------------

Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataT, 'Value')
if len(Non_Numeric) > 0:
    sys.exit()

DataDup = KN.Duplicates_CrossCheck(DataT ,Dimensions + ['Date'])
if len(DataDup) > 0:
    sys.exit()

KN.TimeSeries_Count(DataT, Dimensions + ['Frequency'], PY_Space)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'Data.csv')
DataT.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)

#----------------------------------------------------------------------------------------------------------------------------------------------


