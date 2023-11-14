# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 13:26:04 2023

@author: alex-
"""
from bs4 import BeautifulSoup
import re
import json
import math
import collections
import pandas as pd


# file_name = '10.html'

def handle_file(file_name):
    with open(file_name, encoding = 'utf-8') as file:
        text = ""
        for row in file.readlines():
            text += row
        
        item = dict()

        site = BeautifulSoup(text, 'html.parser')
        #print(site.prettify())
        item['category'] = site.find_all("span", string = re.compile('Категория:'))[0].get_text().split(':')[1].strip()
        item['title'] = site.find_all('h1')[0].get_text().strip()
        item['author'] = site.find_all("p", attrs = {'class' : 'author-p'})[0].get_text().strip()
        item['pages'] = int(site.find_all("span", attrs = {'class' : 'pages'})[0].get_text().split(':')[1].replace(" страниц", "").strip())
        item['year'] = int(site.find_all('span', attrs = {'class': 'year'})[0].get_text().replace("Издано в ", "").strip())
        item['ISBN'] = site.find_all('span', string = re.compile('ISBN'))[0].get_text(). split(':')[1].strip()
        item['description'] = site.find_all("p")[1].get_text().replace("Описание\n", "").strip()
        item['img'] = site.find_all('img')[0]['src']
        item['rate'] = float(site.find_all('span', string = re.compile('Рейтинг:'))[0].get_text().split(':')[1].strip())
        item['views'] = int(site.find_all('span', string = re.compile('Просмотры:'))[0].get_text().split(':')[1].strip())
        
        
        return item

# handle_file('./var_15/996.html')

items = []

for i in range(1,999):
  file_name = f'./var_15/{i}.html'
  result = handle_file(file_name)
  items.append(result) 
  # if i < 100:
  #     print(result)

# print(items[1:50])

items = sorted(items, key = lambda x: x['views'], reverse = True)

# print(items[1:10])

filtered_items = []
for book in items:
    if book['category'] == 'фэнтези':
        filtered_items.append(book)
        
# print(len(items))
# print(len(filtered_items)) 
  
result = []

df = pd.DataFrame(items)
pd.set_option('display.float_format', '{:.1f}'.format)

stats = df['pages'].agg(['max', 'min', 'mean', 'median', 'std']).to_dict()
result.append(stats)

# print(result)

book = [item['category'] for item in items]
f1 = collections.Counter(book)
result.append(f1)

# print(result)


with open('result_all_15.json', 'w', encoding = 'utf-8') as f:
    f.write(json.dumps(items, ensure_ascii=False))



with open("result.json", "w", encoding = 'utf-8') as file:
    file.write(json.dumps(result, ensure_ascii=False))





