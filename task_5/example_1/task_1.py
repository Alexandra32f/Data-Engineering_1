# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 08:52:40 2023

@author: alex-
"""

from pymongo import MongoClient
import msgpack
import json
from bson import ObjectId
from bson import json_util

def parse_json(data):
    return json.loads(json_util.dumps(data))

def connect_db():
    client = MongoClient("localhost", 27017)
    db = client['test-database']
    return db.person


def get_data_from_msgpack(file_name):
    with open(file_name, "rb") as f:
        data = msgpack.unpack(f)
    return data

def default_json_encoder(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def write_result_to_json(result, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False, default=default_json_encoder)
        
    
def insert_many(collection,data):
    # collection.insert_one({'data' : 'aaa'})
    collection.insert_many(data)

def sort_by_salary(collection):
    items = []
    for person in collection.find({}, limit = 10).sort({'salary': -1}):
        items.append(person)
    write_result_to_json(items, 'sort_by_salary.json')
    # print(items)
    
    
def filter_by_age(collection):
    items = []
    for person in (collection
                   .find({'age': {'$lt': 30}}, limit = 15)
                   .sort({'salary': -1})):
        items.append(person)
    write_result_to_json(items, 'filter_by_age.json')

def complex_filter_by_city_and_job(collection):
    items = []
    for person in (collection
                   .find({'city': 'Любляна',
                          'job': {'$in' : ['Менеджер', 'IT-специалист', 'Медсестра']}},
                         limit = 10)
                   .sort({'age': 1})):
        items.append(person)
    write_result_to_json(items, 'complex_filter_by_city_and_job.json')
        
def count_object(collection): 
    result = collection.count_documents({
        'age': {"$gte": 31, "$lte" : 46},
        'year': {"$gte": 2019, "$lte" : 2022},
        '$or' : [
            {'salary': {'$gt': 50000, '$lt': 75000}},
            {'salary': {'$gt': 125000, '$lt': 150000}},]
        
        })
    # print(result)
    write_result_to_json(result, 'count_object.json')
    
    
    
# db.test.insert_one({
#     'one': 1,
#     'second': 2})
# print(client)

# client = connect_db()
data = get_data_from_msgpack('task_1_item.msgpack')
insert_many(connect_db(), data)
sort_by_salary(connect_db())
filter_by_age(connect_db())
complex_filter_by_city_and_job(connect_db())
count_object(connect_db())