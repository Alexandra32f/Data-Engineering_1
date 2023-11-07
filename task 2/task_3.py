# -*- coding: utf-8 -*-
"""
Created on Mon Nov  6 13:20:51 2023

@author: alex-
"""

import json
import msgpack
import os

with open('./products_15.json') as f:
    data = json.load(f)
    #print(data)
    
    
    products = dict()
    for item in data:
        if item['name'] in products:
            products[item['name']].append(item['price'])
        else:
            products[item['name']] = list()
            products[item['name']].append(item['price'])
            
    #print(products)
    
    result = list()
    
    for name,prices in products.items():
        sum_price = 0
        max_price = prices[0]
        min_price = prices[0]
        size = len(prices)
        for price in prices:
            sum_price += price
            max_price = max(max_price, price)
            min_price = min(min_price, price)
            
        result.append({
            'name': name,
            'max': max_price,
            'min': min_price,
            'avr': sum_price / size
            })
            
    print(result)
    
    with open('products_result.json', 'w') as res:
        res.write(json.dumps(result))
        
    with open('products_result.msgpack', 'wb') as r_msgpack:
        r_msgpack.write(msgpack.dumps(result))
       

print(f"json = {os.path.getsize('products_result.json')}")
print(f"msgpack = {os.path.getsize('products_result.msgpack')}")