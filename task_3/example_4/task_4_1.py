# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 11:42:39 2023

@author: alex-
"""
import xml.etree.ElementTree as ET
import json
import statistics
from bs4 import BeautifulSoup
import re
import json
import math
import collections
import pandas as pd
import io


tree = ET.parse('./var_15/93.xml')
root = tree.getroot()
items = []

    
for clothing in root:
    item = {}
    item['id'] = clothing.find('id').text.strip()
    item['name'] = clothing.find('name').text.strip()
    item['category'] = clothing.find('category').text.strip()
    item['size'] = clothing.find('size').text.strip()
    item['color'] = clothing.find('color').text.strip()
    item['material'] = clothing.find('material').text.strip()
    item['price'] = float(clothing.find('price').text)
    item['quantity'] = int(clothing.find('quantity').text) if clothing.find('quantity') is not None else 0
    item['onsale'] = clothing.find('onsale').text if clothing.find('onsale') is not None else 0
    item['label'] = clothing.find('label').text if clothing.find('label') is not None else 0
    # print(item['id'])
    items.append(item)
# print(items)   

# for i in range(1,100):
#   file_name = f'./var_15/{i}.xml'
#   result = handle_file(file_name)
#   items.append(result) 


sort = sorted(items, key=lambda x: x['id'], reverse=True)


filtered = [item for item in items if item['color'].strip() != 'Фиолетовый']
# print(filtered)

# df = pd.DataFrame(items)
# # pd.set_option('display.float_format', '{:.1f}'.format)

result = list()

prices = [item['price'] for item in items]
cloth_price = {
    'sum': sum(prices),
    'min': min(prices),
    'max': max(prices),
    'mean': statistics.mean(prices),
    'median': statistics.median(prices),
    'std': statistics.stdev(prices)}

result.append(cloth_price)

colors = []
for item in items:
    color = item['color'].strip()
    colors.append(color) 
f1 = collections.Counter(colors)
result.append(f1)

print(result)


with open('result_all_15_4.json', 'w', encoding = 'utf-8') as f:
    f.write(json.dumps(items, ensure_ascii=False))
  