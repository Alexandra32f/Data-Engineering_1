# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 11:55:42 2023

@author: alex-
"""
import umsgpack
import json
import sqlite3



with open('./task_1_var_15_item.msgpack', 'rb') as f:
    data = umsgpack.unpack(f)
# print(data[0:3])

# with open('data.json', 'w', encoding = 'utf-8') as res:
#     res.write(json.dumps(data,  indent =4, ensure_ascii=False))

def connect_to_db(file):
    connection = sqlite3.connect(file)
    connection.row_factory = sqlite3.Row # не просто кортежи, но и айди, для создания словарей.
    return connection

def create_table():
    conn = sqlite3.connect('first')
    conn.text_factory =str()
    c = conn.cursor()
    c.execute(''' CREATE TABLE IF NOT EXISTS tours (
    id           INTEGER    PRIMARY KEY AUTOINCREMENT,
    name         TEXT (256),
    city         TEXT (256),
    begin        REAL,
    system       TEXT (256),
    tours_count  INTEGER,
    min_rating   INTEGER,
    time_on_game INTEGER)''')
    conn.commit()
    conn.close()
    
def insert_data():
    db = sqlite3.connect('first')
    cursor = db.cursor()
    with open('./task_1_var_15_item.msgpack', 'rb') as f:
        data = umsgpack.unpack(f)
        
    rows = []
    for item in data:
        row = {
            'name': item['name'],
            'city': item['city'],
            'begin': item['begin'],
            'system': item['system'],
            'tours_count': item['tours_count'],
            'min_rating': item['min_rating'],
            'time_on_game': item['time_on_game']
        }
        rows.append(row)
        
    cursor.executemany("INSERT INTO tours (name, city, begin, system, tours_count, min_rating, time_on_game) VALUES (:name, :city, :begin, :system, :tours_count, :min_rating, :time_on_game)", rows)
    db.commit()
    db.close()


def get_top_by_tours_count(db, limit):
    # db = sqlite3.connect('first')
    cursor = db.cursor()
    cursor.execute("SELECT * FROM tours ORDER BY tours_count LIMIT ?", [limit]) 
    res = [dict(row) for row in cursor.fetchall()]
    with open('get_top_by_tours_count.json', 'w', encoding='utf-8') as f:
        json.dump(res, f, ensure_ascii=False, indent = 4)
    cursor.close()
    return res
    
def get_stat_by_time_on_game(db):
    cursor = db.cursor()
    cursor.execute("""
        SELECT
            SUM(time_on_game) as sum,
            AVG(time_on_game) as avg,
            MIN(time_on_game) as min,
            MAX(time_on_game) as max
        FROM tours """)
    res = dict(cursor.fetchone())
    with open('get_stat_by_time_on_game.json', 'w', encoding='utf-8') as f:
        json.dump(res, f, ensure_ascii=False, indent = 4)
    cursor.close()
    return [] 

def get_freq_by_system(db):
    cursor = db.cursor()
    cursor.execute("""SELECT 
                          CAST(count(*) as REAL) / (SELECT COUNT(*) FROM tours) as count, system
                          FROM tours 
                          GROUP BY system""")
    res = [dict(row) for row in cursor.fetchall()]
    with open('get_freq_by_system.json', 'w', encoding='utf-8') as f:
        json.dump(res, f, ensure_ascii=False, indent = 4)
    cursor.close()
    return res

def filtered_by_time_on_game(db, min_time_on_game, limit =25):
    cursor = db.cursor()
    cursor.execute("""
                         SELECT *
                         FROM tours
                         WHERE begin > ?
                         ORDER BY tours_count DESC
                         LIMIT ?
                         """, [min_time_on_game, limit])
    res = [dict(row) for row in cursor.fetchall()]
    with open('filtered_by_time_on_game.json', 'w', encoding='utf-8') as f:
        json.dump(res, f, ensure_ascii=False, indent = 4)
    cursor.close()
    return res
    

 


db = connect_to_db('./first')
create_table()
insert_data()
get_top_by_tours_count(db, 25)
get_stat_by_time_on_game(db)
get_freq_by_system(db)
filtered_by_time_on_game(db, 25)


   
# def parse_data(file_name):
#     items = []
#     with open(file_name, 'r', encoding = "utf-8") as f:
#         lines = f.readlines()
#         item = dict()
#         # print(lines)
#         for line in lines:
#             # if line == "}\n":
#             #     items.append(item)
#             #     item = dict()
#             # else:
#             print(line)
#             line = line.strip()
#             splitted = line.split(":")
#             if splitted[0] in ['tours_count','min_rating','time_on_game']: 
#                 item[splitted[0]] = int(splitted[1])
#             elif splitted[0] == "id":
#                 continue
#             elif len(splitted) >= 2:
#                 item[splitted[0]] = splitted[1]
#     # print(items)
#     return items
        
    
    
    

# # cursor = connection.cursor() # помогает выполеять запросы в самой базе данных
# items = parse_data("./data.json")
# print(len(items))
# db = connect_to_db('./first')

# insert_data(db, items)
# result = db.cursor().execute("SELECT * FROM tours")

# print(result.fetchall())






