# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 13:53:23 2023

@author: alex-
"""

from bs4 import BeautifulSoup
import re
import json
import math
import collections
import pandas as pd


def handle_file(file_name):
    items = list()
    with open(file_name, encoding = 'utf-8') as file:
        text = ""
        for row in file.readlines():
            text += row
        

        site = BeautifulSoup(text, 'html.parser')
        products = site.find_all('div', attrs={'class' : 'product-item'})
        # print(len(products))
        
        
        for product in products:
            item = dict()
            item['id'] = product.a['data-id']
            item['link'] = product.find_all('a')[1]['href']
            item['img'] = site.find_all('img')[0]['src']
            item['name'] = product.find_all('span')[0].get_text().strip()
            item['price'] = int(product.price.get_text().replace('₽', '').replace(' ', '').strip())
            item['bonus'] = int(product.strong.get_text().replace('+ начислим ', '').replace(' бонусов', '').strip())
        
            
            props = product.ul.find_all('li')
            
            for prop in props:
                item[prop['type']] = prop.get_text().strip()
                
            items.append(item)
            
            # print(item)
    return items
    
# handle_file('33.html')

items = []

for i in range(1,71):
  file_name = f'./var_15/{i}.html'
  items += handle_file(file_name)
  

items = sorted(items, key = lambda x: x['bonus'], reverse = True)

filtered_items = []
for smartphone in items:
    if smartphone['price'] > 50000:
        filtered_items.append(smartphone)

# print(filtered_items[1:10])

# print(len(items))
# print(len(filtered_items))

    
result = []


df = pd.DataFrame(items)
pd.set_option('display.float_format', '{:.1f}'.format)

stats = df['price'].agg(['max', 'min', 'mean', 'median', 'std']).to_dict()
result.append(stats)

phone = [item['name'] for item in items]
f1 = collections.Counter(phone)
result.append(f1)

# print(result)


with open('result_all_15_2.json', 'w', encoding = 'utf-8') as f:
    f.write(json.dumps(items, ensure_ascii=False))



with open("result_2.json", "w", encoding = 'utf-8') as file:
    file.write(json.dumps(result, ensure_ascii=False))

