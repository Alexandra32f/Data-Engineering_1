# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 13:05:53 2023

@author: alex-
"""


import json
import sqlite3
import pickle
import msgpack
import pandas as pd

with open('task_4_var_15_update_data.pkl', 'rb') as f:
    data = pickle.load(f)
print(data[0:3]) 

with open('task_4_var_15_product_data.msgpack', 'rb') as f:
    data = msgpack.unpack(f)
print(data[0:3]) 

def get_data_from_pickle(file_name):
    with open(file_name, "rb") as f:
        data = pickle.load(f)
    return data

data = get_data_from_pickle('task_4_var_15_update_data.pkl')

def get_data_from_msgpack(file_name):
    with open(file_name, "rb") as f:
        data = msgpack.unpack(f)
    return data
msgpack_data = get_data_from_msgpack('task_4_var_15_product_data.msgpack')

def connect_to_db(file):
    connection = sqlite3.connect(file)
    connection.row_factory = sqlite3.Row # не просто кортежи, но и айди, для создания словарей.
    return connection

def create_table(conn: sqlite3.Connection)-> None:
    # conn = sqlite3.connect('four.db')
    # conn.text_factory =str()
    c = conn.cursor()
    c.execute(''' CREATE TABLE IF NOT EXISTS time (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT,
    price       REAL,
    quantity    INTEGER,
    category    TEXT,
    fromCity    TEXT,
    isAvailable TEXT,
    views       INTEGER,
    count       INTEGER
)''')
    conn.commit()
    conn.close()
    
def insert_data(conn: sqlite3.Connection, msgpack_data: list[dict])-> None:
    # conn = sqlite3.connect('four.db')
    cursor = conn.cursor()
    for data in msgpack_data:
        data['category'] = 'no'
        cursor.execute("""INSERT INTO time (name, price, quantity, category, fromCity, 
                           isAvailable, views, count) 
                  VALUES (?, ?, ?, ?, ?, ?, ?, 0)""",
                  (data['name'], 
                   data['price'], 
                   data['quantity'],
                   data['category'], 
                   data['fromCity'], 
                   data['isAvailable'], 
                   data['views']))
    
    conn.commit()
    conn.close()     

    
db = connect_to_db('four.db')
create_table(db)
insert_data(db, msgpack_data)

# def load_update(file_name):
#     items = []
#     with open(file_name, 'rb') as f:
#         data = pickle.load(f)
        
#         for row in data:
#             if len(row) == 0: continue
#             item = dict()
#             item['name'] = row[0]
#             item['method'] = row[1]
#             item['param'] = row[2]
#             items.append(item)
            
            
#     return items 
# load_update('task_4_var_15_update_data.pkl')



def update_data(data):
    conn = sqlite3.connect('four.db')
    cursor = conn.cursor()
    for d in data:

        if d['method'] == 'available':
            response = cursor.execute("UPDATE time SET isAvailable = ?, count = count + 1 WHERE name == ?", ['True' if d['param'] else 'False', d['name']])
        elif d['method'] == 'quantity_sub':
            param = d['param']
            response = cursor.execute("UPDATE time SET quantity = MAX(quantity - ?, 0), count = count + 1 WHERE name = ? AND ((quantity - ?) > 0)", [param, d['name'], param])
        elif d['method'] == 'quantity_add':
            response = cursor.execute("UPDATE time SET quantity = quantity + ? WHERE name = ?", [d['param'], d['name']])
            if response.rowcount > 0:
                cursor.execute("UPDATE time SET count = count + 1 WHERE name = ?", [d['name']])
        elif d['method'] == 'price_percent':
            response = cursor.execute("UPDATE time SET price = ROUND(price * (1 + ?), 2), count = count + 1 WHERE name = ?", [d['param'], d['name']])
        elif d['method'] == 'remove':
            response = cursor.execute("DELETE FROM time WHERE name = ?" ,[d['name']])
        elif d['method'] == 'price_abs':
            param = d['param']
            response = cursor.execute("UPDATE time SET price = MAX(price + ?, 0) WHERE name = ? AND ((price + ?) > 0)", [param, d['name'], param])
            if response.rowcount > 0:
                cursor.execute("UPDATE time SET count = count + 1 WHERE name = ?", [d['name']])
        else:
            print("UNK method ", d['method'])

    conn.commit()
    
update_data(data)  

def top_10_products(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("SELECT name, count FROM time ORDER BY count DESC LIMIT 10")
    data = cursor.fetchall()
    conn.close()
    
    top_10_updated = [{'name': row[0], 'count': row[1]} for row in data]
    
    with open('top_10_products.json', 'w', encoding='utf-8') as file:
        json.dump(top_10_updated, file, ensure_ascii=False)
        
top_10_products(db)

def analyze_prices(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""SELECT category, 
              SUM(price) AS total_price, 
              MIN(price) AS min_price, 
              MAX(price) AS max_price, 
              AVG(price) AS avg_price, 
              COUNT(*) AS num_products 
              FROM time GROUP BY category""")
    data = cursor.fetchall()
    conn.close()
    
    price_analysis = [{'category': row[0],
                       'all_price': row[1],
                       'min_price': row[2],
                       'max_price': row[3],
                       'avg_price': row[4],
                       'num_products': row[5]} for row in data]
    
    with open('avg_price.json', 'w', encoding='utf-8') as file:
        json.dump(price_analysis, file, ensure_ascii=False)
    
analyze_prices(db)    
    

def analyze(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""SELECT category, 
                   SUM(quantity) AS total_quantity,
                   MIN(quantity) AS min_quantity, 
                   MAX(quantity) AS max_quantity,
                   AVG(quantity) AS avg_quantity 
                   FROM time GROUP BY category""")
    data = cursor.fetchall()
    conn.close()
    
    quantity_analysis = [{'category': row[0], 
                          'total_quantity': row[1], 
                          'min_quantity': row[2], 
                          'max_quantity': row[3], 
                          'avg_quantity': row[4]} for row in data]
    
    with open('analysis_4.json', 'w', encoding='utf-8') as file:
        json.dump(quantity_analysis, file, ensure_ascii=False)
    
analyze(db)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
