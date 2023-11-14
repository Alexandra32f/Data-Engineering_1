# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 15:28:50 2023

@author: alex-
"""

from bs4 import BeautifulSoup
import re
import json
import math
import collections
import pandas as pd


def handle_file(file_name):

    with open(file_name, encoding = 'utf-8') as file:
        text = ""
        for row in file.readlines():
            text += row
            
    
        item = dict()

        site = BeautifulSoup(text, 'xml').star
        for i in site.contents:
            if i.name == "radius":
                item[i.name] = int(i.get_text().strip())
            
            elif i.name is not None:
                item[i.name] = i.get_text().strip()
                
        
        
        # item['name'] = site.find_all('name')[0].get_text().strip()
        # item['constellation'] = site.find_all('constellation')[0].get_text().strip()
        # item['spectral-class'] = site.find_all('spectral-class')[0].get_text().strip()
        # item['radius'] = int(site.find_all('radius')[0].get_text().strip())
        # item['rotation'] = site.find_all('rotation')[0].get_text().strip()
        # item['age'] = site.find_all('age')[0].get_text().strip()
        # item['distance'] = site.find_all('distance')[0].get_text().strip()
        # item['absolute-magnitude'] = site.find_all('absolute-magnitude')[0].get_text().strip()
        # print(item)
        return item


        
# handle_file('./var_15/493.xml')


items = []

for i in range(1,500):
  file_name = f'./var_15/{i}.xml'
  result = handle_file(file_name)
  items.append(result) 
  # if i < 100:
  #     print(result)

# print(items[1:50])

items = sorted(items, key = lambda x: x['radius'], reverse = True)

# print(items[1:10])

filtered_items = []
for star in items:
    if star['constellation'] != 'Овен':
        filtered_items.append(star)

    
result = []


df = pd.DataFrame(items)
pd.set_option('display.float_format', '{:.1f}'.format)

stats = df['radius'].agg(['max', 'min', 'mean', 'median', 'std']).to_dict()
result.append(stats)

star = [item['constellation'] for item in items]
f1 = collections.Counter(star)
result.append(f1)

# print(result)


with open('result_all_15_3.json', 'w', encoding = 'utf-8') as f:
    f.write(json.dumps(items, ensure_ascii=False))



with open("result_3.json", "w", encoding = 'utf-8') as file:
    file.write(json.dumps(result, ensure_ascii=False))





