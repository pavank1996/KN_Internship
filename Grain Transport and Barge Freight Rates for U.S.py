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

KN.Check_Exists(ToolConfigF, [Host, App_Id, App_Secret, Dataset_Id, URL_1, URL_2])

KN.PrintTF('\n> Host: ' + Host)

KN.Host = Host
KN.App_Id = App_Id
KN.App_Secret = App_Secret

##KNUpload_ToolPath = os.path.join(Script_Path, 'Tool_Knoema_Upload')            
##if not os.path.exists(KNUpload_ToolPath):
##    KN.PrintTF('\n>> Failed: Knoema Upload C# Tool Not Found @: ' + KNUpload_ToolPath)
##    sys.exit()

##KU = KNUpload.KNUpload()
##KU.Host = Host
##KU.App_Id = App_Id
##KU.App_Secret = App_Secret
##KU.Logs_Folder_Path = PY_Space    
##KU.KNUpload_ToolPath = KNUpload_ToolPath

KN.PrintTF('\n> Downloading Source Files')
URLs = [URL_1, URL_2]

def source_download(URLs):
    for url in URLs:
        wget.download(url , PY_Space)
        KN.PrintTF('\n> File Downloaded from the Source : ' + url)

source_download(URLs)

Downloaded_Files = ['GTRTable1.xlsx', 'GTRTable9.xlsx']

#--------------------------------------------------------------------------------------------------------------------------------------------------

Indicator_Map = {'tru':'KN.A10', 'uni':'KN.A11', 'shu':'KN.A12', 'bar':'KN.A13', 'gul':'KN.A14', 'pac':'KN.A15','pnw' : 'KN.A15'}

UnitDict ={ 'KN.M5':'ton', 'KN.M3':'index, 2000 = 100', 'KN.M6':'index, 1976 = 100 ', 'KN.M9':'%, avg index ', 'KN.M10':'%, avg index ', 'KN.M11':'%, change' ,
            'KN.M12':'%, change' ,'KN.M8':' index, 1976 = 100','KN.M7':'index, 1976 = 100' , 'KN.M14':'$/ton'
            }
             
Dimensions = ['Indicator', 'Measure']
Header = ['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value']

Indicator_Map = dict((Key.upper(), Value.upper()) for Key, Value in Indicator_Map.items()) 
DataAll = pd.DataFrame()

#--------------------------------------------------------------------------------------------------------------------------------------------------

Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable1')]
Source_File_Name = ''.join(Source_File_Name)

KN.PrintTF('\nReading source file>>' + Source_File_Name + '\tSheetname = Data')
Source_File_Path = os.path.join(PY_Space, Source_File_Name)
Data_1 = pd.read_excel(Source_File_Path,sheet_name = 0 ,header = None ,dtype=object)
Data_1.drop(columns=[0, 1,2, 3, 4, 5, 6,7],inplace = True)
Data_1 = Data_1[[8, 9, 10, 11, 12, 13, 14]]

Data_1.dropna(subset=[9, 10, 11, 12, 13, 14], inplace = True)
Data_1.fillna('', inplace=True)

Data_1.columns = Data_1.iloc[0]  
Data_1.drop(Data_1.index[0])

DataT = pd.melt(Data_1, id_vars=[''],var_name='indicator', value_name='Value')
DataT.rename(columns = {'' : 'Date'} , inplace = True)
DataT.dropna(subset = ['Date'] , inplace = True )

DataT['indicator'] = DataT['indicator'].str.strip()
DataT['indicator'] = DataT['indicator'].str.upper()

Indicator_Map = dict((Key.upper(), Value.upper()) for Key, Value in Indicator_Map.items())
DataT['Indicator'] = DataT['indicator'].str[:3].apply(str.upper).map(Indicator_Map)
DataT.drop(columns = ['indicator'], axis = 1,inplace = True)

DataT['Frequency'] = 'W'
DataT['Measure'] = 'KN.M3'
DataT['latest'] = '1'

#UnitDict = dict((Key.upper(), Value.upper()) for Key, Value in UnitDict.items())
DataT['Unit'] = DataT.apply(lambda row: row.Measure , axis = 1)  #Making copy of Measure column with column name as Unit and replacing their values 
DataT.replace({"Unit": UnitDict}, inplace = True)

DataAll = DataAll.append(DataT[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])

#---------------------------------------------------------------------------------------------------------------------------------------------------

UnitDictS ={ 'KN.A10':'$/gallon','KN.A11':'$/car','KN.A12':'$/car','KN.A13':'% of tarrif rate','KN.A14':'$/metric ton','KN.A15':'$/metric ton'}					

Data_2 = pd.read_excel(Source_File_Path,sheet_name = 0 ,header = None ,dtype=object)    
Data_2.drop(columns=[0, 1,2, 3, 4, 5, 6,7],inplace = True)
Data_2 = Data_2[[8,16,17,18,19,20,21]]
Data_2.reset_index(drop = True)               

Data_2.dropna(subset=[16, 17, 18, 19, 20, 21],inplace = True)             
Data_2.fillna('', inplace=True)

Data_2.columns = Data_2.iloc[0]  
Data_2.drop(Data_2.index[0])
             
DataN = pd.melt(Data_2, id_vars=[''],var_name='indicator', value_name='Value')
DataN.rename(columns = {'' : 'Date'} , inplace =True)
DataN.dropna(subset = ['Date'] , inplace = True)

DataN['indicator'] = DataN['indicator'].str.strip()
DataN['indicator'] = DataN['indicator'].str.upper()

Indicator_Map = dict((Key.upper(), Value.upper()) for Key, Value in Indicator_Map.items())
DataN['Indicator'] = DataN['indicator'].str[:3].apply(str.upper).map(Indicator_Map)              

#UnitDictS = dict((Key.upper(), Value.upper()) for Key, Value in UnitDictS.items())
DataN['Unit'] = DataN.apply(lambda row: row.Indicator , axis = 1)
DataN.replace({"Unit": UnitDictS}, inplace = True)

DataN.drop(columns = ['indicator'], axis = 1,inplace = True)

DataN['Frequency'] = 'W'
DataN['Measure'] = 'KN.M2'
DataN['latest'] = '2'

DataAll = DataAll.append(DataN[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])
 
#---------------------------------------------------------------------------------------------------------------------------------------------------


Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)
    
KN.PrintTF('\nReading source file>>' + Source_File_Name + '\tSheetname = Figure 8_data')

Source_File_Path = os.path.join(PY_Space, Source_File_Name)

IndDict = {'twc':'KN.A22','mm ':'KN.A23','ill':'KN.A24', 'st ':	'KN.A25',' cin ':   'KN.A26', 'loh':'KN.A27' , 'car' : 'KN.A28' , 'mem' : 'KN.A29'}

Data = pd.read_excel(Source_File_Path ,sheet_name = 'Figure 8_data'  ,header = None ,dtype=object)
Data = Data[[0,1,2,3,7,8]]
Data.rename(columns = {0 : 'Date' },inplace = True)

Data[2] = pd.to_numeric(Data[2], errors='coerce') 
Data[3] = pd.to_numeric(Data[3], errors='coerce')
Data[7] = pd.to_numeric(Data[7], errors='coerce')
Data[8] = pd.to_numeric(Data[8], errors='coerce')

##Data[2,3,7,8] = pd.to_numeric(Data[2], errors='coerce') 

Data.loc[0] = pd.Series({1:'KN.A24', 2:'KN.A24', 3:'KN.A24', 7:'KN.A24', 8:'KN.A24'})  #filling the rows
Data.loc[1] = pd.Series({1:'KN.M6', 2:'KN.M9', 3:'KN.M10', 7:'KN.M12', 8:'KN.M11'})
Data.loc[2] = pd.Series({1:'index, 1976 = 100', 2:'%, avg index', 3:'%, avg index', 7:'%, change', 8:'%, change'})

Data.fillna('',inplace = True)

Data.columns = Data.iloc[0,:] + '|' + Data.iloc[1,:] + '|' + Data.iloc[2,:]
Data.rename(columns = {'||' : 'Date'}, inplace = True)

DataT = pd.melt(Data, id_vars=['Date'],var_name='Indicator', value_name='Value')
DataT["Value"] = pd.to_numeric(DataT["Value"], errors='coerce')
DataT.dropna(subset = ['Value'] , inplace = True)

DataT[['Indicator','Measure','Unit']] = DataT.Indicator.str.split("|" , expand =True,)

DataT['Frequency'] = 'W'
DataT['latest'] = '3'

DataAll = DataAll.append(DataT[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])

#----------------------------------------------------------------------------------------------------------------------------------------------------

Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)

KN.PrintTF('\nReading source file>>' + Source_File_Name + '\tSheetname = NXTMONTH')
Source_File_Path = os.path.join(PY_Space, Source_File_Name)

IndDictA = {'tw':'KN.A22','mm':'KN.A23','il':'KN.A24', 'st':'KN.A25','ci':'KN.A26', 'lo':'KN.A27' , 'ca' : 'KN.A28' , 'me' : 'KN.A29'}
														    
DataA = pd.read_excel(Source_File_Path  ,sheet_name = 'NXTMONTH'  ,dtype=object)

DataA = DataA[['DATE','MONTH','TWC','MM','ILL','ST LOUIS','CINC','LOH','CAR-MEM','MEM-SO']]
DataA.drop(columns = ['MONTH'] ,inplace = True)
DataA.rename(columns = {'DATE' : 'Date' } , inplace = True)

DataT = pd.melt(DataA, id_vars=['Date'], var_name='Indicator', value_name='Value')

IndDictA = dict((Key.upper(), Value.upper()) for Key, Value in IndDictA.items())
DataT['Indicator'] = DataT['Indicator'].astype(str).str[:2].apply(str.upper).map(IndDictA)

DataT['Measure'] = 'KN.M7'
DataT['Frequency'] = 'W'
DataT['latest'] = '4'

DataT['Unit'] = DataT.apply(lambda row: row.Measure , axis = 1)
DataT.replace({"Unit": UnitDict}, inplace = True)

DataT.fillna('' , inplace = True)

DataAll = DataAll.append(DataT[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])

#-----------------------------------------------------------------------------------------------------------------------------------------------------
Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)

KN.PrintTF('\nReading source file>>' + Source_File_Name + '\tSheetname = THREEMONTH')
Source_File_Path = os.path.join(PY_Space, Source_File_Name)

DataB = pd.read_excel(Source_File_Path,sheet_name = 'THREEMONTH'  ,dtype=object)
DataB = DataB[['DATE','TWC','MM','ILL','ST LOUIS','CINC','LOH','CAR-MEM','MEM-SO']]
DataA.rename(columns = {'DATE' : 'Date' } , inplace = True)

DataT2 = pd.melt(DataA, id_vars=['Date'], var_name='indicator', value_name='Value')
IndDictA = dict((Key.upper(), Value.upper()) for Key, Value in IndDictA.items())
DataT2['Indicator'] = DataT2['indicator'].astype(str).str[:2].apply(str.upper).map(IndDictA)
DataT2.drop(columns = ['indicator'] , inplace = True)

DataT2['Measure'] = 'KN.M8'
DataT2['Frequency'] = 'W'
DataT2['latest'] = '5'

DataT2['Unit'] = DataT2.apply(lambda row: row.Measure , axis = 1)  
DataT2.replace({"Unit": UnitDict}, inplace = True)

DataT2.fillna('' , inplace = True)

DataAll = DataAll.append(DataT2[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])

#----------------------------------------------------------------------------------------------------------------------------------------------------

print("Reading sheet>> Table 9_data\n")

IndDictA = {'TWC':'KN.A22','MM':'KN.A23','ILL':'KN.A24', 'ST LOUIS':'KN.A25','CINC':'KN.A26', 'LOH':'KN.A27' , 'CAR-MEM' : 'KN.A28' , 'MEM-SO' : 'KN.A29'}
Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)

KN.PrintTF('\nReading source file>>' + Source_File_Name + '\tSheetname = Table 9_data')
Source_File_Path = os.path.join(PY_Space, Source_File_Name)
														    
DataS = pd.read_excel(Source_File_Path ,sheet_name = 'Table 9_data' ,header = None ,dtype=object)

DataS = DataS[[0,1,2,3,4,5,6,7,8]]

DataS.columns = DataS.iloc[2]     #changing the column header
DataS.drop(DataS.index[2])

DataS['MEM-SO'] = pd.to_numeric(DataS['MEM-SO'], errors='coerce')
DataS.dropna(subset = ['MEM-SO'] ,inplace = True)

DataS.rename(columns = { 'All Points' : 'Date' } , inplace = True)
DataT = pd.melt(DataS, id_vars=['Date'],var_name='Indicator', value_name='Value')

IndDictA = dict((Key.upper(), Value.upper()) for Key, Value in IndDictA.items())
DataT.replace({"Indicator": IndDictA}, inplace = True)
DataT = DataT.loc[~DataT['Value'].isin([ 0 , ' '])]      #drop zero values

DataT['Measure'] = 'KN.M6'
DataT['Frequency'] = 'W'
DataT['latest'] = '6'

DataT['Unit'] = DataT.apply(lambda row: row.Measure , axis = 1)
DataT.replace({"Unit": UnitDict}, inplace = True)

DataAll = DataAll.append(DataT[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])
#------------------------------------------------------------------------------------------------------------------------------------------------

Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)

Source_File_Path = os.path.join(PY_Space, Source_File_Name )

Data = pd.read_excel(Source_File_Path ,sheet_name = 'Table 9_data' ,header = None ,dtype=object)
Data = Data[[0,10,11,12,13,14,15,16,17]]

Data.columns = Data.iloc[2]     
Data.drop(Data.index[2])

Data['MEM-SO'] = pd.to_numeric(Data['MEM-SO'], errors='coerce')
Data.dropna(subset = ['MEM-SO'] ,inplace = True)

Data.rename(columns = { 'All Points' : 'Date' } , inplace = True)
DataT = pd.melt(Data, id_vars=['Date'],var_name='Indicator', value_name='Value')

IndDictA = dict((Key.upper(), Value.upper()) for Key, Value in IndDictA.items())
DataT.replace({"Indicator": IndDictA}, inplace = True)

DataT['Measure'] = 'KN.M9'
DataT['Frequency'] = 'W'
DataT['latest'] = '7'

#UnitDict = dict((Key.upper(), Value.upper()) for Key, Value in UnitDict.items())
DataT['Unit'] = DataT.apply(lambda row: row.Measure , axis = 1)
DataT.replace({"Unit": UnitDict}, inplace = True)

DataAll = DataAll.append(DataT[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])
#------------------------------------------------------------------------------------------------------------------------------------------------

Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)

Source_File_Path = os.path.join(PY_Space, Source_File_Name)

Data = pd.read_excel(Source_File_Path , sheet_name = 'Table 9_data' ,header = None ,dtype=object)
Data = Data[[0,19,20,21,22,23,24,25,26]]

Data.columns = Data.iloc[2]     
Data.drop(Data.index[2])

Data['MEM-SO'] = pd.to_numeric(Data['MEM-SO'], errors='coerce')
Data.dropna(subset = ['MEM-SO'] ,inplace = True)

Data.rename(columns = { 'All Points' : 'Date' } , inplace = True)
DataT = pd.melt(Data, id_vars=['Date'],var_name='Indicator', value_name='Value')

IndDictA = dict((Key.upper(), Value.upper()) for Key, Value in IndDictA.items())
DataT.replace({"Indicator": IndDictA}, inplace = True)

DataT['Measure'] = 'KN.M10'
DataT['Frequency'] = 'W'
DataT['latest'] = '8'

#UnitDict = dict((Key.upper(), Value.upper()) for Key, Value in UnitDict.items())
DataT['Unit'] = DataT.apply(lambda row: row.Measure , axis = 1)
DataT.replace({"Unit": UnitDict}, inplace = True)

DataAll = DataAll.append(DataT[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])
#------------------------------------------------------------------------------------------------------------------------------------------------

Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)

Source_File_Path = os.path.join(PY_Space, Source_File_Name )

Data = pd.read_excel(Source_File_Path ,sheet_name = 'Table 9_data' ,header = None ,dtype=object)
Data = Data[[0,29,30,31,32,33,34,35,36]]

Data.columns = Data.iloc[2]     
Data.drop(Data.index[2])

Data['MEM-SO'] = pd.to_numeric(Data['MEM-SO'], errors='coerce')
Data.dropna(subset = ['MEM-SO'] ,inplace = True)

Data.rename(columns = { 'All Points' : 'Date' } , inplace = True)
DataT = pd.melt(Data, id_vars=['Date'],var_name='Indicator', value_name='Value')

IndDictA = dict((Key.upper(), Value.upper()) for Key, Value in IndDictA.items())
DataT.replace({"Indicator": IndDictA}, inplace = True)

DataT['Measure'] = 'KN.M11'
DataT['Frequency'] = 'W'
DataT['latest'] = '9'

#UnitDict = dict((Key.upper(), Value.upper()) for Key, Value in UnitDict.items())
DataT['Unit'] = DataT.apply(lambda row: row.Measure , axis = 1)
DataT.replace({"Unit": UnitDict}, inplace = True)

DataAll = DataAll.append(DataT[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])

#------------------------------------------------------------------------------------------------------------------------------------------------
Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)

Source_File_Path = os.path.join(PY_Space, Source_File_Name )

Data = pd.read_excel(Source_File_Path ,sheet_name = 'Table 9_data' ,header = None ,dtype=object)
Data = Data[[0,39,40,41,42,43,44,45,46]]

Data.columns = Data.iloc[2]     
Data.drop(Data.index[2])

Data['MEM-SO'] = pd.to_numeric(Data['MEM-SO'], errors='coerce')
Data.dropna(subset = ['MEM-SO'] ,inplace = True)

Data.rename(columns = { 'All Points' : 'Date' } , inplace = True)
DataT = pd.melt(Data, id_vars=['Date'],var_name='Indicator', value_name='Value')

IndDictA = dict((Key.upper(), Value.upper()) for Key, Value in IndDictA.items())
DataT.replace({"Indicator": IndDictA}, inplace = True)

DataT['Measure'] = 'KN.M12'
DataT['Frequency'] = 'W'
DataT['latest'] = '10'

#UnitDict = dict((Key.upper(), Value.upper()) for Key, Value in UnitDict.items())
DataT['Unit'] = DataT.apply(lambda row: row.Measure , axis = 1)
DataT.replace({"Unit": UnitDict}, inplace = True)

DataAll = DataAll.append(DataT[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])
#-------------------------------------------------------------------------------------------------------------------------------------------------

print('\nProcessing Annual Data from sheet>> New Table 9')
Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)

KN.PrintTF('\nReading source file>>' + Source_File_Name + '\tSheetname = New Table 9')
Source_File_Path = os.path.join(PY_Space, Source_File_Name)

AnnDict = {'Tw':'KN.A22','Mm': 'KN.A23' ,'Il':'KN.A24', 'St': 'KN.A25','Ci':'KN.A26',	'Lo':'KN.A27','Ca':'KN.A28'}
						
Data = pd.read_excel(r"E:\Knoema_Work_Dataset\USGTBFR2019\GTRTable9.xlsx" ,sheet_name = 'New Table 9' ,header = None ,dtype=object)

Data = Data.iloc[22:24]
Data = Data[[1,2,3,4,5,6,7,8]]
Data[1] = '1976'
Data.rename(columns = {1: 'Date'} , inplace = True)
Data = Data.reset_index(drop=True)

Data.columns = Data.iloc[0]     
Data = Data.drop(Data.index[0])
Data.rename(columns={ Data.columns[0]: "Date" }, inplace = True)

DataT = pd.melt(Data, id_vars=['Date'],var_name='indicator', value_name='Value')

AnnDict = dict((Key.upper(), Value.upper()) for Key, Value in AnnDict.items())
DataT['Indicator'] = DataT['indicator'].astype(str).str[:2].apply(str.upper).map(AnnDict)
DataT.drop(columns = ['indicator'] , inplace = True)

DataT['Measure'] = 'KN.M5'
DataT['Frequency'] = 'A'
DataT['latest'] = '11'

#UnitDict = dict((Key.upper(), Value.upper()) for Key, Value in UnitDict.items())
DataT['Unit'] = DataT.apply(lambda row: row.Measure , axis = 1)
DataT.replace({"Unit": UnitDict}, inplace = True)

DataAll = DataAll.append(DataT[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])
DataAll.reset_index(drop=True)


#-----------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n>>> Calculating Values--- [index*Weight / 100]')

AnnDict = {'TWC':'KN.A22','MM': 'KN.A23' ,'ILL':'KN.A24', 'ST LOUIS': 'KN.A25','CINC':'KN.A26',	'LOH':'KN.A27','CAR-MEM':'KN.A28'}

Data = pd.read_excel(Source_File_Path ,sheet_name = 'New Table 9' ,header = None ,dtype=object)

Data = Data.iloc[22:24]
Data = Data[[1,2,3,4,5,6,7,8]]
Data[1] = '1976'
Data.rename(columns = {1: 'Date'} , inplace = True)
Data['Date'] = ''
Data = Data.reset_index(drop = True)

Data.columns = Data.iloc[0]     
Data.drop(Data.index[0])
Data.drop([0] , inplace = True)

Data.rename(columns = {''  : "Date"} , inplace = True)

print("Reading sheet>> Table 9_data\n")

IndDictA = {'TWC':'KN.A22','MM':'KN.A23','ILL':'KN.A24', 'ST LOUIS':'KN.A25','CINC':'KN.A26', 'LOH':'KN.A27' , 'CAR-MEM' : 'KN.A28' , 'MEM-SO' : 'KN.A29'}

Source_File_Name = [File for File in Downloaded_Files if File.startswith('GTRTable9')]
Source_File_Name = ''.join(Source_File_Name)

KN.PrintTF('\nReading source file>>' + Source_File_Name)
Source_File_Path = os.path.join(PY_Space, Source_File_Name)  
														    
DataS = pd.read_excel(Source_File_Path ,sheet_name = 'Table 9_data' ,header = None ,dtype=object)

DataS = DataS[[0,1,2,3,4,5,6,7,8]]

DataS.dropna(subset = [8] ,inplace = True)

DataS.drop(columns = [8] , inplace = True)
DataS.rename(columns = {0:'Date', 1:'TWC', 2:'MM', 3:'ILL',4:'ST LOUIS',5:'CINC',6:'LOH',7:'CAR-MEM'} , inplace = True)
DataC = pd.concat([Data, DataS],sort = True)
DataC = (DataC[['Date','TWC', 'MM','ILL', 'ST LOUIS', 'CINC', 'LOH', 'CAR-MEM']])
						
DataC = DataC.reset_index(drop = True)
DataC.columns = DataC.iloc[0,:].astype(str) + '|' + DataC.iloc[1,:].astype(str)
DataC = DataC.drop([0,1])
DataC.rename(columns={ DataC.columns[0]: "Date" }, inplace = True)

DataC = pd.melt(DataC, id_vars=["Date"],var_name='Annual_value', value_name='Values')
DataC[['Annual','Indicator']] = DataC.Annual_value.str.split("|" , expand =True,)
DataC.drop(columns = ['Annual_value'] , inplace = True)

DataC[DataC.eq(0)] = np.nan
DataC.dropna(subset = ['Values'] ,inplace = True)

DataC.loc[~DataC['Values'].isin(['-100',' '])]

IndDictA = dict((Key.upper(), Value.upper()) for Key, Value in IndDictA.items())
DataC.replace({"Indicator": IndDictA}, inplace = True)

DataC['Values'] = pd.to_numeric(DataC['Values'], errors='coerce')
DataC.dropna(subset = ['Values'] , inplace = True)
DataC = DataC.reset_index(drop = True)

DataC["Annual"] = DataC["Annual"].astype(float)
DataC["Values"] = DataC["Values"].astype(float)

DataC["Value"] = DataC.Annual * DataC.Values /100

DataC["Frequency"] = "W"
DataC["Measure"] = "KN.M14"
DataC['latest'] = '12'

#UnitDict = dict((Key.upper(), Value.upper()) for Key, Value in UnitDict.items())
DataC['Unit'] = DataC.apply(lambda row: row.Measure , axis = 1)
DataC.replace({"Unit": UnitDict}, inplace = True)

DataC.drop(columns = ['Annual' , 'Values'] , inplace =True)
DataC = DataC.loc[~DataC['Value'].isin(['-100','0'])]


DataAll = DataAll.append(DataC[['Indicator', 'Measure', 'Unit', 'Frequency', 'Date', 'Value','latest']])  

#---------------------------------------------------------------------------------------------------------------------------------------------------
DataAll.drop(columns = ['latest'] ,inplace = True)

DataAll = DataAll[DataAll.Value != -100]
DataAll["Date"]= pd.to_datetime(DataAll["Date"])
DataAll = DataAll.sort_values('Date').drop_duplicates('Value',keep='last')
         
DataAll['Value'] = pd.to_numeric(DataAll['Value'], errors='coerce') 
DataAll.dropna(subset=['Value'],inplace = True)

DataAll.fillna(' ' ,inplace = True)
#DataAll.drop_duplicates(keep = False, inplace = True)

Non_Numeric = KN.NonNumeric_CrossCheck_V2(DataAll, 'Value')
if len(Non_Numeric) > 0:
    sys.exit()

##DataDup = KN.Duplicates_CrossCheck(DataAll , Dimensions + ['Date'])
##if len(DataDup) > 0:
##    sys.exit()

KN.TimeSeries_Count(DataAll, Dimensions + ['Frequency'], PY_Space)

UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
DataAll.to_csv(Export_PathF, columns=Header, index=False, encoding='utf-8-sig')

##Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

##KN.TPE(Time)

#------------------------------------------------------------------------------------------------------------------------------------------------------


















                               
                               

