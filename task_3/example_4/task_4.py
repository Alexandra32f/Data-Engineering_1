# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 12:17:44 2023

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

        root = BeautifulSoup(text, 'xml')
        
        for clothing in root.find_all("clothing"):
            item = {}
            for i in clothing.contents:
                if i.name is None:
                    continue
                elif i.name == "price" or i.name == "reviews":
                    item[i.name] = int(i.get_text().strip())
                elif i.name == "price" or i.name == "rating":
                    item[i.name] = float(i.get_text().strip())
                elif i.name == "new":
                    item[i.name] = i.get_text().strip() == '+'
                elif i.name == "exclusive" or i.name == "sporty":
                    item[i.name] = i.get_text().strip() == 'yes'
                else:
                    item[i.name] = i.get_text().strip()
            items.append(item)
            # print(items)
    return items


        
# print(handle_file('./var_15/93.xml'))


items = []

for i in range(1,100):
  file_name = f'./var_15/{i}.xml'
  result = handle_file(file_name)
  items.append(result) 

df = pd.DataFrame(items)
pd.set_option('display.float_format', '{:.1f}'.format)

# print(items[1:50])

items = sorted(items, key = lambda x: x['reviews'], reverse = True)

# # print(items[1:10])

filtered_items = []
for cloth in items:
    if cloth['color'] != 'Фиолетовый':
        filtered_items.append(cloth)
        
# # print(len(items))
# # print(len(filtered_items))

# result = list()



cloth = df['reviews'].agg(['max', 'min', 'mean', 'median', 'std']).to_dict()
result.append(cloth)

smart = items['sporty']
f1 = collections.Counter(smart)
result.append(f1)

print(result)


with open('result_all_15_4.json', 'w', encoding = 'utf-8') as f:
    f.write(json.dumps(items, ensure_ascii=False))

with open("result_4.json", "w") as file:
    file.write(json.dumps(result, ensure_ascii=False))           