# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 13:17:28 2023

@author: alex-
"""

import json
import requests
from bs4 import BeautifulSoup

base_url = 'https://static-basket-01.wb.ru/vol0/data/stores-data.json'

response = requests.get(base_url)
data = response.json()

#soup = BeautifulSoup('<html><body><table></table></body></html>', 'html.parser')
soup = BeautifulSoup("""<table>
    <tr>
        <th>id</th>
        <th>name</th>
        <th>type</th>
    </tr>
</table>""", "html.parser")
table = soup.table
tr = soup.new_tag('tr')
table.append(tr)
for title in data[0].keys():
        th = soup.new_tag('th')
        th.string = title
        tr.append(th)

for line in data:
    tr = soup.new_tag('tr')
    table.append(tr)
    for col in line.values():
        td = soup.new_tag('td')
        td.string = str(col)
        tr.append(td)

with open("text_6_var_15.html", "w", encoding="utf-8") as result:
    result.write(soup.prettify())
    result.write("\n")