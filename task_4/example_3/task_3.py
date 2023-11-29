# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 15:54:07 2023

@author: alex-
"""

import csv
import json
import sqlite3
import re
import pandas as pd

mus = pd.read_csv('task_3_var_15_part_2.csv', encoding = 'windows-1251', sep = ';')
# print(mus[0:5])
mus = mus.drop(['energy', 'key'], axis=1)
# mus = mus.rename(columns = {"    artist": "artist"})
mus = mus.to_dict('records')
# print(mus[0:5])

def parse_data(file_name):
    items = []
    with open(file_name, 'r', encoding = "utf-8") as f:
        lines = f.readlines()
        item = dict()
        # print(lines)
        for line in lines:
            if line == "=====\n":
                items.append(item)
                item = dict()
            else:
                # print(line)
                line = line.strip()
                splitted = line.split("::")
                if splitted[0] in ['duration_ms','year']: 
                    item[splitted[0]] = int(splitted[1])
                elif splitted[0] in ['tempo','loudness']:
                    item[splitted[0]] = float(splitted[1])
                elif splitted[0] in ['instrumentalness','explicit']: continue
                else:
                    item[splitted[0]] = splitted[1]
            
    # print(items)
    return items

parse_data('./task_3_var_15_part_1.text')

# data = mus + parse_data('./task_3_var_15_part_1.text')

# def connect_to_db(file):
#     connection = sqlite3.connect(file)
#     connection.row_factory = sqlite3.Row # не просто кортежи, но и айди, для создания словарей.
#     return connection
   
def create_table():
    conn = sqlite3.connect('sec.db')
    conn.text_factory =str()
    c = conn.cursor()
    c.execute(''' CREATE TABLE IF NOT EXISTS music (
        id          INTEGER    PRIMARY KEY AUTOINCREMENT,
        artist      TEXT,
        song        TEXT,
        duration_ms INTEGER,
        year        INTEGER,
        tempo       REAL,
        genre       TEXT,
        loudness    REAL
    )''')
    conn.commit()
    conn.close()

create_table()

def insert_data():
    conn = sqlite3.connect('sec.db')
    c = conn.cursor()
    csv_reader = mus
    for item in csv_reader:
        c.execute("INSERT INTO music (artist, song, duration_ms, year, tempo, genre, loudness) VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre, :loudness)",
                  {'artist': item['artist'],
                   'song': item['song'],
                   'duration_ms': item['duration_ms'],
                   'year': item['year'],
                   'tempo': item['tempo'],
                   'genre': item['genre'],
                   'loudness': item['loudness']})
    data = parse_data('./task_3_var_15_part_1.text')
    for item in data:
        c.execute("INSERT INTO music (artist, song, duration_ms, year, tempo, genre, loudness) VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre, :loudness)",
                  {'artist': item['artist'], 
                   'song': item['song'], 
                   'duration_ms': item['duration_ms'], 
                   'year': item['year'], 
                   'tempo': item['tempo'], 
                   'genre': item['genre'],
                   'loudness': item['loudness']})
    conn.commit()
    conn.close()

insert_data()

def sort_by_duration_ms():
    conn = sqlite3.connect('sec.db')
    c = conn.cursor()
    c.execute("SELECT * FROM music ORDER BY duration_ms LIMIT 25")
    result = c.fetchall()
    with open('sorted_duration_ms.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent =4)

    conn.close()

sort_by_duration_ms()  

def statistics():
    conn = sqlite3.connect('sec.db')
    c = conn.cursor()

    c.execute("""SELECT 
              SUM(duration_ms) as sum, 
              MIN(duration_ms) as min, 
              MAX(duration_ms) as max, 
              AVG(duration_ms) as avg
              FROM music""")
    
    res = c.fetchone()
    with open('statistics_duration_ms.json', 'w', encoding='utf-8') as file:
        json.dump(res, file, ensure_ascii=False, indent =4)

    conn.close()

    return res

statistics()

def artist_frequency():
    conn = sqlite3.connect('sec.db')
    c = conn.cursor()

    c.execute("SELECT artist, COUNT(*) FROM music GROUP BY artist")
    res = c.fetchall()
    
    with open('artist_frequency.json', 'w', encoding='utf-8') as file:
        json.dump(res, file, ensure_ascii=False, indent =4)

    conn.close()

    return res

artist_frequency()


def filter_and_sort_by_tempo():
    conn = sqlite3.connect('sec.db')
    c = conn.cursor()

    c.execute("SELECT * FROM music WHERE tempo > 90 ORDER BY tempo LIMIT 30")
    res = c.fetchall()

    with open('filter_and_sort_3.json', 'w', encoding='utf-8') as file:
        json.dump(res, file, ensure_ascii=False, indent = 4)

    conn.close()
    return res
    
filter_and_sort_by_tempo() 



