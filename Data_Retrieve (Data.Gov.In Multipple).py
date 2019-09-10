import os, sys, time, datetime
import requests, json
import pandas as pd

Resource_Ids_Path = r"E:\Knoema_Work\Knoema Dataset\Important\Data.Gov.in\Resource_Id_List.txt"

Resource_Id_List = pd.read_csv(Resource_Ids_Path, header=None, names=['Resource_Id'], dtype=object)
Resource_Id_List = Resource_Id_List['Resource_Id'].tolist()

API_Key = 'XXXXYYYYZZZ'

URL_Part_1 = 'https://api.data.gov.in/resource/'
URL_Part_2 = '?api-key='
URL_Part_3 = '&format=json&offset=0'
URL_Part_4 = '&limit='

for Resource_Id in Resource_Id_List:   
    Export_Path = os.path.dirname(os.path.realpath(__file__))
    Export_Path = os.path.join(Export_Path, Resource_Id)
    if not os.path.exists(Export_Path):
        os.makedirs(Export_Path)

    #:- To Get Total Records
    URL = URL_Part_1 + Resource_Id + URL_Part_2 + API_Key + URL_Part_3
    Response = requests.get(URL)
    if Response.status_code == 200:
        try:
            Json_Data_Dict = json.loads(Response.content.decode('utf-8'))  #.loads for parsing
            Total_Records = Json_Data_Dict['total']
            print('> Total_Records:', str(Total_Records))
        except Exception as e:
            print('\n> Failed:', str(e))
    else:
        print('\n> Failed: ', Resource_Id)
        sys.exit()

    #:- Data Retrieve
    URL = URL_Part_1 + Resource_Id + URL_Part_2 + API_Key + URL_Part_3 + URL_Part_4 + str(Total_Records)
    Response = requests.get(URL)
    if Response.status_code == 200:
        Json_Data_Dict = json.loads(Response.content.decode('utf-8'))

        #:- For Meta_Data
        Org = Json_Data_Dict['org']  # List
        Org = ', '.join(Org)    # List to String
        Json_Data_Dict.pop('org')   # Remove "org"

        #:- For Meta_Data
        Sector = Json_Data_Dict['sector']    # List
        Sector = ', '.join(Sector)  # List to String
        Json_Data_Dict.pop('sector')   # Remove "org"

        if 'target_bucket' in Json_Data_Dict:
            Target_Bucket = Json_Data_Dict['target_bucket'] # Dict
            Json_Data_Dict.pop('target_bucket')
            Target_Bucket_DF = pd.DataFrame([Target_Bucket])

            Export_Path_F = os.path.join(Export_Path, 'target_bucket.csv')
            Target_Bucket_DF.to_csv(Export_Path_F, encoding='utf-8-sig', index=False)        

        #:- 'records' column names
        Field = Json_Data_Dict['field']  # List
        Data_Columns_Info = pd.DataFrame(Field)
        Export_Path_F = os.path.join(Export_Path, 'Data_Columns_Info.csv')
        Data_Columns_Info.to_csv(Export_Path_F, encoding='utf-8-sig', index=False)           
        Json_Data_Dict.pop('field') # Remove "field"

        #:- Data Points
        Records = Json_Data_Dict['records']  # List
        Json_Data_Dict.pop('records')
        Records_DF = pd.DataFrame(Records)

        if len(Records_DF) < Total_Records:
            print(('\n> All Data Not Retrieved: %s| Total_Records: %s| Retrieved: %s')%(Resource_Id, Total_Records, str(len(Records_DF))))

        if len(Records_DF) == 0:
            sys.exit()

        #:- Rename columns
        try:
            D = dict(zip(Data_Columns_Info['id'].str.upper(), Data_Columns_Info['name']))
            Records_DF.columns = Records_DF.columns.str.upper()
            Records_DF = Records_DF.rename(columns=D)   # Rename Column Name by mapping with other Dataframe
        except Exception as e:
            print('\n> Failed:', str(e))
            sys.exit()

        Export_Path_F = os.path.join(Export_Path, 'Data Points.csv')
        Records_DF.to_csv(Export_Path_F, encoding='utf-8-sig', index=False)
        print('> Data:', str(len(Records_DF)))
        print('>> Data File Exported to:', Export_Path)

        Meta_Data = pd.DataFrame([Json_Data_Dict])
        Meta_Data['org'] = Org
        Meta_Data['sector'] = Sector
        
        Export_Path_F = os.path.join(Export_Path, 'Meta_Data.csv')
        Meta_Data.to_csv(Export_Path_F, encoding='utf-8-sig', index=False)        
    else:
        print('\n> Failed: ', Resource_Id)
        sys.exit()
