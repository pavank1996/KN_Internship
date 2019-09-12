import os, sys, time
import pandas as pd
import requests, json
from bs4 import BeautifulSoup

Export_Path = r"G:\sample progrmes"
URL = 'https://data.gov.in/ogpl_apis?page=0'

DataAll = pd.DataFrame()
Data_List = []

Response = requests.get(URL)
if Response.status_code == 200:
    Soup = BeautifulSoup(Response.content, 'html.parser')

    Title_Text = None
    Ministry_Text = None
    Sector_Text = None
    Link_URL = None
    Created_Text = None
    Last_Updated_Text = None
    
    for Class_L1 in Soup.findAll('div', {'class': 'apifrom_dataset'}):
        for Title in Class_L1.findAll('a'):
            Title_Text = Title.get('title')
            if Title_Text is not None: break

        for Ministry in Class_L1.findAll('p'):
            Ministry_Text = Ministry.text
            if Ministry_Text is not None: break

        for Class_L2 in Class_L1.findAll('div', {'class': 'sector_api'}):
            for Sector in Class_L2.findAll('p'):
                Sector_Text = Sector.text
                if Sector_Text is not None: break
                
        for Class_L3 in Class_L1.findAll('div', {'class': 'visualizatonapi_inline'}):   
            for Class_L4 in Class_L3.findAll('div', {'class': 'visualizaton_datasets'}):
                for Class_L5 in Class_L4.findAll('span', {'class': 'datasets'}):
                    for Link in Class_L5.findAll('a', href=True):
                        Link_URL = Link.get('href')
                        if Link_URL is not None: break
                        
            for Class_L6 in Class_L3.findAll('div', {'class': 'created_date'}):
                for Created in Class_L6.findAll('span', {'class': 'count-datasets'}):
                    Created_Text = Created.text
                    if Created_Text is not None: break

        for Class_L7 in Class_L1.findAll('div', {'class': 'updated_date'}):
            for Last_Updated in Class_L7.findAll('span', {'class': 'count-datasets'}):
                Last_Updated_Text = Last_Updated.text
                if Last_Updated_Text is not None:
                    Last_Updated_Text = Last_Updated_Text.strip()
                    break                      

        Data_List.append({'Title':Title_Text, 'Ministry':Ministry_Text, 'Sector':Sector_Text, 'Link':Link_URL,
                          'Created':Created_Text, 'Last Updated':Last_Updated_Text})

DataAll = pd.DataFrame(Data_List)
Export_Path_F = os.path.join(Export_Path, 'APIs on Datasets.csv')
DataAll.to_csv(Export_Path_F, encoding='utf-8-sig', index=False) 
    

