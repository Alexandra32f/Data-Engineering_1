# -*- coding: utf-8 -*-
"""
Created on Sat Dec  9 08:54:39 2023

@author: alex-
"""

from pymongo import MongoClient
import json
from bson import ObjectId
from bson import json_util

def connect_db():
    client = MongoClient("localhost", 27017)
    db = client['test-database']
    return db.person

def insert_many(collection,data):
    collection.insert_many(data)
    
def default_json_encoder(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def write_result_to_json(result, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False, default=default_json_encoder)
        
def parse_data(file_name):
    items = []
    with open(file_name, 'r', encoding = "utf-8") as f:
        lines = f.readlines()
        item = dict()
        for line in lines:
            if line == "=====\n":
                items.append(item)
                item = dict()
            else:
                line = line.strip()
                splitted = line.split("::")
                if splitted[0] in ['salary','id', 'year', 'age']: 
                    item[splitted[0]] = int(splitted[1])
                else:
                    item[splitted[0]] = splitted[1]
            
    # print(items)
    return items

def delete_by_salary(collection):
    result = collection.delete_many({
        '$or': [
            {'salary' : {'$lt' : 25000}},
            {'salary' : {'$gt' : 175000}}]})
    print(result)
    

def update_age(collection):
    result = collection.update_many({}, {
        '$inc': {'age' : 1}})
    print(result)

def increase_salary_by_job(collection):
    filter = {
        'job' : {'$in' : ['Строитель', 'Архитектор', 'Программист', 'Психолог']}}
    update = { 
        
            '$mul' : { 'salary' :  1.05}}
    result = collection.update_many(filter, update)
    print(result)


def increase_salary_by_city(collection):
    filter = {
        'city' : {'$nin' : ['Сория', 'Луарка', 'Махадаонда', 'Сантьяго-де-Компостела']}}
    update = { 
        
            '$mul' : { 'salary' :  1.07}}
    result = collection.update_many(filter, update)
    print(result)

def increase_salary_by_all(collection):
    filter = {
        'city' : {'$nin' : ['Сория', 'Кадакес', 'Махадаонда', 'Вальядолид']},
        'job' : {'$in' : ['Учитель', 'Косметолог', 'Программист', 'Психолог']},
        'age' : {'$nin' : [29, 32, 63, 21]}}
    update = { 
        
            '$mul' : { 'salary' :  1.1}}
    result = collection.update_many(filter, update)
    print(result)

def delete_by_year(collection):
    result = collection.delete_many({
        '$or': [
            {'year' : {'$lt' : 2000}},
            {'year' : {'$gt' : 2013}},
            ]})
    print(result)

def round_salary(collection):
    data = collection.find()
    
    for person in data:
        person['salary'] = round(person['salary'], 2)
        print(person)
        
    collection.insert_many(data)
        





data = parse_data('./task_3_item.text')
# print(data[0:3])

insert_many(connect_db(), data)

delete_by_salary(connect_db())
update_age(connect_db())
increase_salary_by_job(connect_db())
increase_salary_by_city(connect_db())
increase_salary_by_all(connect_db())
delete_by_year(connect_db())
# round_salary()




