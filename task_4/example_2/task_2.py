# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 15:03:02 2023

@author: alex-
"""
import umsgpack
import json
import sqlite3

# def load_data(file_name):
#     items = []
    
#     with open(file_name, 'r', encoding = 'utf-8') as f:
#         data = json.load(f)
#         data.__next__() # skip header
        
#         for row in data:
#             if len(row) == 0: continue
#             item = dict()
#             item['name'] = row[0]
#             item['place'] = row[1]
#             item['priice'] = row[2]
#             print(item)
#             items.append(item)
            
#     return items

with open('./task_2_var_15_subitem.json', 'r', encoding = 'utf-8') as f:
    json_data = json.load(f)
    print(json_data[0:3])

def connect_to_db(file):
    connection = sqlite3.connect(file)
    connection.row_factory = sqlite3.Row # не просто кортежи, но и айди, для создания словарей.
    return connection
   
def create_table():
    conn = sqlite3.connect('first')
    conn.text_factory =str()
    c = conn.cursor()
    c.execute(''' CREATE TABLE IF NOT EXISTS suitem (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    tours_id         REFERENCES tours (id),
    place    INTEGER,
    prise    INTEGER)''')
    conn.commit()
    conn.close()
    
def insert_data():
    db = sqlite3.connect('first')
    cursor = db.cursor()
    
    with open('./task_2_var_15_subitem.json', 'r', encoding = 'utf-8') as f:
        json_data = json.load(f)


    for item in json_data:
        cursor.execute("INSERT OR IGNORE INTO suitem (tours_id, place, prise) VALUES ((SELECT id FROM tours WHERE name =:name), :place, :prise)",
                        {
                        'name':item['name'],
                        'place': item['place'],
                        'prise': item['prise']})
    db.commit()
    db.close()

db = sqlite3.connect('first')
create_table()
connect_to_db('./task_2_var_15_subitem.json')
insert_data()


def first_query(db, name):
    cursor = db.cursor()
    res = cursor.execute("""
                          SELECT *
                          FROM suitem
                          WHERE tours_id = (SELECT id FROM tours WHERE name = ?)
                          """, [name])
    items = []
    for row in res.fetchall():
        item = {
            'id': row[0],
            'tours_id': row[1],
            'place': row[2],
            'prise': row[3]}
        items.append(item)
    # res = [dict(row) for row in cursor.fetchall()]
    with open('filtered_Vien.json', 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent = 4)
    cursor.close()
    return items

first_query(db, 'Вена 1961')

def second_query(db, name):
    cursor = db.cursor()
    res = cursor.execute("""
                          SELECT 
                              AVG(place) as avg_place,
                              AVG(prise) as avg_prise
                          FROM suitem
                          WHERE tours_id = (SELECT id FROM tours WHERE name = ?)
                          
                          """, [name])
    items = []
    for row in res.fetchall():
        item = {
            'AVG(place)': row[0],
            'AVG(prise)': row[1]}
        items.append(item)


    # res = [dict(row) for row in cursor.fetchall()]
    with open('filtered_Vien_avg.json', 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent = 4)
    cursor.close()
    return items

second_query(db, 'Вена 1961')
    
              
def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
                          SELECT name,
                          COUNT(*) as count
                          FROM tours
                          GROUP BY name
                          LIMIT 10
                          """)
    items = []
    for row in res.fetchall():
        item = {
            'NAME': row[0],
            'count': row[1]}
        items.append(item)


    # res = [dict(row) for row in cursor.fetchall()]
    with open('filtered_count.json', 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent = 4)
    cursor.close()
    return items

third_query(db)              
              
              
              
              # (SELECT COUNT(*) FROM suitem WHERE id = tours_id) as count
              
              




