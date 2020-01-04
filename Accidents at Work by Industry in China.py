import sys, os, time, time, datetime
import pandas as pd
import json
import requests
import csv
import KNG, KNMetaData, KNUpload, metadata

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
Data_URL = ''
MetaData_URL = ''

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
        if str.upper(Line).startswith('METADATA_URL:'):
            MetaData_URL = Line[Line.find(':')+1:len(Line)].strip()
        if str.upper(Line).startswith('DATA_URL:'):
            Data_URL = Line[Line.find(':')+1:len(Line)].strip()            

KN.Check_Exists(ToolConfigF, [Host, App_Id, App_Secret, Dataset_Id, Data_URL, MetaData_URL])

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

#--------------------------------------------------------------------------------------------------------------------------------------------------

Dimensions = ['Indicator', 'Type of Store']
Header = ['ID', 'Title', 'Description', 'Province', 'Number of Deaths',	'Number of Injuries', 'Number of Employees', 'Industry', 'Type',
          'Number of Person Punished', 'Financial Penalty', 'Latitude', 'Longitude', 'Date']

#---------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n>> Getting MetaData from the Source')

MD = metadata.MetaData_Download()
MD.URL = MetaData_URL
MD.PY_Space = PY_Space

Is_Downloaded = MD.Data_Retrieval()
if Is_Downloaded is None:
    KN.PrintTF('\n>> Failed: MetaData download Issue')
    sys.exit()

#---------------------------------------------------------------------------------------------------------------------------------------------------

YYYYMM = datetime.date.today().strftime('%Y%m')
Data_URL = Data_URL.format(YYYYMM)

KN.PrintTF('\n>> Source Retrieving')

try:
    Res = requests.get(Data_URL)
except Exception as e:
    KN.PrintTF('\n>> Failed: Please check the API link: ' + Data_URL)
    KN.PrintTF('>> Try Hint: The formated Datetime doesnt seem to be intimated in the URL, Inspect Page, Please Check for the URL')
    sys.exit()
    
JsonData = json.loads(Res.text)
Source_File_Path = os.path.join(PY_Space, 'output.csv')

with open(Source_File_Path, "w", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(['ID', 'Latitude', 'Longitude','Title', 'Description', 'Date', 'Province', 'Number of Deaths', 'Number of Injuries',
                     'Number of Employees', 'Industry', 'Type', 'Number of Person Punished', 'Financial Penalty'])		

    for item in JsonData["accidents"]:
        writer.writerow([item["id"],
                          item["lat"],
                          item["lng"],
                          item["title"],
                          item["desc"],
                          item["date"],
                          item["province"],
                          item["num_death"],
                          item["num_injuries"],
                          item["num_employees"],
                          item["industry"],
                          item["type"],
                          item["num_punished"],
                          item["fin_penalty"]])

KN.PrintTF("\n>> Raw Data Exported")

#----------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n>> Reading the Source File')

try:
    if os.stat(Source_File_Path).st_size > 0:
       KN.PrintTF(">> Source File found in the Directory: " + PY_Space)
    else:
       KN.PrintTF('\n>> Failed: Empty file Detected, Please Check: ' + PY_Space)
       sys.exit()
except OSError:
    KN.PrintTF('\n>> Failed: No such files found in the Directory: ' + PY_Space)
    sys.exit()

Data = pd.read_csv(Source_File_Path)
Data.dropna(subset = ['Latitude', 'Longitude'], inplace = True)

#----------------------------------------------------------------------------------------------------------------------------------------------------

Map_Dict = {0:'Not Available', '0':'Not Available', '1':'0', '2':'1-9', '3':'10-29', '4':'30-49', '5':'50+'}
Ind_Dict = {'0':'Not Available'}
Type_Dict = {'0' :'Not Categorized'}

#----------------------------------------------------------------------------------------------------------------------------------------------------

KN.PrintTF('\n>> Mapping the Data from MetaData to the DataFrame')
MetaData_FPath = os.path.join(PY_Space, 'MetaData')

Code_data = pd.read_csv(os.path.join(MetaData_FPath, "MetaData_Province.csv"), usecols = ['id', 'Province'], dtype=object)
Code_data = Code_data.set_index('id')['Province'].to_dict()
Data["Province"] = Data["Province"].astype(str).replace(Code_data)
Data["Province"] = Data["Province"].astype(str).replace(Ind_Dict)
           
Data["Number of Deaths"] = Data["Number of Deaths"].astype(str).replace(Map_Dict)
Data["Number of Injuries"] = Data["Number of Injuries"].astype(str).replace(Map_Dict)

Code_data = pd.read_csv(os.path.join(MetaData_FPath, "MetaData_num_employees.csv"), usecols = ['value', 'display_value'], dtype=object)
Code_data = Code_data.set_index('value')['display_value'].to_dict()
Data["Number of Employees"] = Data["Number of Employees"].astype(str).replace(Code_data)
Data["Number of Employees"] = Data["Number of Employees"].astype(str).replace({'0': 'Not Available'})

Code_data = pd.read_csv(os.path.join(MetaData_FPath, "MetaData_industies.csv"), usecols = ['id', 'name'], dtype=object)
Code_data = Code_data.set_index('id')['name'].to_dict()
Data["Industry"] = Data["Industry"].astype(str).replace(Code_data)
Data["Industry"] = Data["Industry"].astype(str).replace(Ind_Dict)

Code_data = pd.read_csv(os.path.join(MetaData_FPath, "MetaData_fin_penalty.csv"), usecols = ['value', 'display_value'], dtype=object)
Code_data = Code_data.set_index('value')['display_value'].to_dict()
Data["Financial Penalty"] = Data["Financial Penalty"].astype(str).replace(Code_data)

Code_data = pd.read_csv(os.path.join(MetaData_FPath, "MetaData_accident_type.csv"), usecols = ['value', 'display_value'], dtype=object)
Code_data = Code_data.set_index('value')['display_value'].to_dict()
Data["Type"] = Data["Type"].astype(str).replace(Code_data)
Data["Type"] = Data["Type"].astype(str).replace(Type_Dict)
        
UploadFiles_Path = KN.Folder_Create(PY_Space, 'UploadFiles')
Export_PathF = os.path.join(UploadFiles_Path, 'DataAll.csv')
Data.to_csv(Export_PathF, index=False, encoding='utf-8-sig')

Is_Upload_OK = KU.CSharpClient_Upload(Dataset_Id, UploadFiles_Path)

KN.TPE(Time)


