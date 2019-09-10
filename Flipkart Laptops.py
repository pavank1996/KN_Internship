import os, sys, time
import pandas as pd
import requests, json
from bs4 import BeautifulSoup

Export_Path = r"E:\knoema Webscraping"
URL = 'https://www.flipkart.com/laptops/~buyback-guarantee-on-laptops-/pr?sid=6bo%2Cb5g&uniqBStoreParam1=val1&wid=11.productCard.PMU_V2'

DataAll = pd.DataFrame()

products=[] 
prices=[] 
ratings=[]

Response = requests.get(URL)
if Response.status_code == 200:
   Soup = BeautifulSoup(Response.content, 'html.parser')
    
for a in Soup.findAll('a',href=True, attrs={'class':'_31qSD5'}):
    name=a.find('div', attrs={'class':'_3wU53n'})
    price=a.find('div', attrs={'class':'_1vC4OE _2rQ-NK'})
    rating=a.find('div', attrs={'class':'niH0FQ'})
    products.append(name.text)
    prices.append(price.text)
    ratings.append(rating.text)

    DataAll= pd.DataFrame({'Product Name':products,'Price':prices,'Rating':ratings})
    Export_Path_F = os.path.join(Export_Path, 'flipkart.csv')
    DataAll.to_csv(Export_Path_F, encoding='utf-8-sig', index=False) 
    

    
     
