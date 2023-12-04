# -*- coding: utf-8 -*-
"""
Created on Mon Dec  4 09:52:01 2023

@author: alex-
"""

import json
import sqlite3
import csv
import pandas as pd

salar = pd.read_csv('salaries.csv', encoding = 'windows-1251', sep =',')

salar = salar.to_dict('records')
print(salar[0:3])

def get_data_from_json(file_name):
    with open(file_name, "r") as f:
        json_data = json.load(f)
    return json_data


def connect_to_db(file):
    connection = sqlite3.connect(file)
    connection.row_factory = sqlite3.Row # не просто кортежи, но и айди, для создания словарей.
    return connection

def create_table(conn: sqlite3.Connection)-> None:
    # conn = sqlite3.connect('four.db')
    # conn.text_factory =str()
    c = conn.cursor()
    c.execute(''' CREATE TABLE IF NOT EXISTS salaries_category (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        salary          INTEGER,
        salary_currency TEXT,
        salary_in_usd   INTEGER,
        count           INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS salries_exp (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        work_year        INTEGER,
        experience_level TEXT,
        employment_type  TEXT,
        job_title        TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS location (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_residence TEXT,
        remote_ratio       INTEGER,
        company_location   TEXT,
        company_size       TEXT)''')
    conn.commit()
    conn.close()
    
def insert_data(conn: sqlite3.Connection, json_data: list[dict])-> None:
    # conn = sqlite3.connect('salary.db')
    c = conn.cursor()
    csv_reader = salar
    for item in csv_reader:
        c.execute("""INSERT INTO salaries_category (salary, salary_currency, salary_in_usd) 
                  VALUES (?, ?, ?)""",
                  {'salary': item['salary'],
                   'salary_currency': item['salary_currency'],
                   'salary_in_usd': item['salary_in_usd']})
        c.execute("""INSERT INTO salries_exp (work_year, experience_level, employment_type, 
                  job_title) 
                  VALUES (?, ?, ?, ?)""",
                  {'work_year': item['work_year'],
                   'experience_level': item['experience_level'],
                   'employment_type': item['employment_type'],
                   'job_title': item['job_title']})
        c.execute("""INSERT INTO location (employee_residence, 
                  remote_ratio,company_location, company_size) 
                  VALUES (?, ?, ?, ?)""",
                  {'remote_ratio': item['remote_ratio'],
                   'company_location': item['company_location'],
                   'company_size': item['company_size']})
    
    for item in json_data:
        c.execute("""INSERT INTO salaries_category (salary, salary_currency, salary_in_usd) 
                  VALUES (?, ?, ?)""",
                  {'salary': item['salary'],
                   'salary_currency': item['salary_currency'],
                   'salary_in_usd': item['salary_in_usd']})
        c.execute("""INSERT INTO salries_exp (work_year, experience_level, employment_type, 
                  job_title) 
                  VALUES (?, ?, ?, ?)""",
                  {'work_year': item['work_year'],
                   'experience_level': item['experience_level'],
                   'employment_type': item['employment_type'],
                   'job_title': item['job_title']})
        c.execute("""INSERT INTO location (employee_residence, 
                  remote_ratio,company_location, company_size) 
                  VALUES (?, ?, ?, ?)""",
                  {'remote_ratio': item['remote_ratio'],
                   'company_location': item['company_location'],
                   'company_size': item['company_size']})
        
    conn.commit()
    conn.close()   
    
def sorted_by_work_year():
    # в зависимости от года отсортировала тип занятости
    conn = sqlite3.connect('salary.db')
    c = conn.cursor()
    c.execute("""SELECT employment_type, work_year
              FROM salries_exp ORDER BY work_year DESC LIMIT 20""")
    result = c.fetchall()
    with open('sorted_by_work_year.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent =4)
    c.close()    

def sorted_by_salary():
    # где расположены компании, в которых у работников зарплата больше 100000 usd
    conn = sqlite3.connect('salary.db')
    c = conn.cursor()
    c.execute("""SELECT location.company_location, salaries_category.salary_in_usd
              FROM location 
              JOIN salary_in_usd ON salaries_category.id == location.id
              WHERE salary_in_usd > 100000""")
    result = c.fetchall()
    with open('sorted_by_salary.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent =4)
    c.close() 
    

def avg_salary():
    # характеристки для зарплат
    conn = sqlite3.connect('salary.db')
    c = conn.cursor()
    c.execute("""SELECT SUM(salary) AS sum,
                        MIN(salary) AS min, 
                        MAX(salary) as max, 
                        AVG(salary) as avg 
                FROM salaries_category""")
    result = c.fetchall()
    with open('avg_salary.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent =4)
    c.close()    
    
def salries_exp():
    # те, кто не находятся на удаленном доступе, с уровнем навыков SE
    conn = sqlite3.connect('salary.db')
    c = conn.cursor()
    c.execute("""SELECT  salries_exp.experience_level, salries_exp.job_title, location.remote_ratio
                            FROM salries_exp
                            JOIN location ON salries_exp.id == location.id
                            ORDER BY remote_ratio ASC 
                            WHERE experience_level = SE LIMIT 10;""")
    result = c.fetchall()
    with open('salries_exp.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent =4)
    c.close()
    

def experience_level():
    # общее количество, когда опыт = EX
    conn = sqlite3.connect('salary.db')
    c = conn.cursor()
    c.execute("""SELECT COUNT() AS count
              FROM salries_exp WHERE experience_level = EX""")
    result = c.fetchall()
    with open('experience_level.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent =4)
    c.close()


def job_title():
    # рейтинг 10 зарплат у Machine Learning Engineer
    conn = sqlite3.connect('salary.db')
    c = conn.cursor()
    c.execute("""SELECT salries_exp.job_title, salries_exp.experience_level, salaries_category.salary
                            FROM salries_exp
                            JOIN salaries_category ON salries_exp.id == salaries_category.id
                            WHERE job_title = 'Machine Learning Engineer' LIMIT 10""")
    result = c.fetchall()
    with open('job_title.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent =4)
    c.close()


def salary_in_usd():
    # количество зарплат, валюта которых US
    conn = sqlite3.connect('salary.db')
    c = conn.cursor()
    c.execute("""UPDATE salaries_category SET count = count + 1
                       WHERE salary_in_usd = US""")
    result = c.fetchall()
    with open('salary_in_usd.json', 'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent =4)
    c.close()
    

json_data = get_data_from_json('salaries_cybersecurity.json')
db = connect_to_db('salary.db')
create_table(db)
insert_data(db, json_data)     
sorted_by_work_year()
sorted_by_salary()
avg_salary()
salries_exp()
experience_level()
job_title()
salary_in_usd()


    
