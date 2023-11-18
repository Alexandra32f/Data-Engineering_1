# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 18:09:48 2023

@author: alex-
"""

import requests
from bs4 import BeautifulSoup as BS
import json
from time import sleep
import pandas as pd
import collections

# list_card_url = []
headers = {'User-Agent' : 
           'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'}

    
def get_url():
    
    url = 'https://fashionroomstore.uds.app/c/goods'
    response = requests.get(url, headers = headers)

    soup = BS(response.text, 'lxml')

    data = soup.find_all('div', class_='goods-card_card__ujqS6')  
    
    for i in data:
        card_url = 'https://fashionroomstore.uds.app' +i.find('a', class_ = 'goods-card_image__YGJs2').get('href')
        # print(card_url)
        yield card_url
        # name = i.find('div', class_ = 'goods-card_name__78asi text-200').text

        # price = i.find('div', class_= 'goods-card-price_primaryPrice__1vnui').text

        # url_img = i.find('img', class_ = 'goods-card-image_img__qbD1i').get('srcset')
        # # print(url_img) 


        # print(name + "\n" + price + "\n" + url_img + "\n\n")
    



items = []   
def doubble():
    for card_url in get_url():
        response = requests.get(card_url, headers = headers)
        # sleep(3)

        soup = BS(response.text, 'lxml')
        
        data = soup.find('div', class_='price-detail_container__r6IU4')  

        name = data.find('div', class_ = 'price-detail_title__B0F2o').text

        price = int(data.find('div', class_= 'goods-card-price_primaryPrice__1vnui price-form_primaryPrice__fQQSA goods-card-price_isDetail__BhsJK').text.replace('₽', '').replace(' ', ''))
        
        text = data.find('span', class_ = 'beautify-text_text_wrap__hQI1N').text
        
        # url_img = data.find('img').get('srcset')

        # print(url_img) 


        # k = name + "\n" + price + "\n" + text + "\n\n"
        item = {'name' : name,
                 'price' : price,
                 'text' : text }
        items.append(item)


    with open('result_all_15.json', 'w', encoding = 'utf-8') as f:
        f.write(json.dumps(items, ensure_ascii=False))
    
doubble()

sort = sorted(items, key = lambda x: x['price'], reverse = True)

filtered_items = []
for tool in items:
    if tool['price'] > 3000:
        filtered_items.append(tool)      


result = []

df = pd.DataFrame(items)
pd.set_option('display.float_format', '{:.1f}'.format)

stats = df['price'].agg(['max', 'min', 'mean', 'median', 'std']).to_dict()
result.append(stats)

# print(result)

cloth = [item['name'] for item in items]
f1 = collections.Counter(cloth)
result.append(f1)

print(result)      
