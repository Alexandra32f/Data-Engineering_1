# -*- coding: utf-8 -*-
"""
Created on Sat Dec  9 09:57:37 2023

@author: alex-
"""

from pymongo import MongoClient
import json
import pandas as pd
from bson import ObjectId
from bson import json_util

def connect_db():
    client = MongoClient("localhost", 27017)
    db = client['test-5-database']
    return db.salar

salary =pd.read_csv('salaries.csv', encoding = 'utf-8', sep = ',')
salary = salary.to_dict('records')

def insert_many(collection,data):
    # collection.insert_one({'data' : 'aaa'})
    collection.insert_many(data)
    
def get_data_from_json(file_name):
    with open(file_name, "r") as f:
        data = json.load(f)
    return data
    
def default_json_encoder(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def write_result_to_json(result, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False, default=default_json_encoder)


#1        
def sort_by_salary(collection):
    items = []
    for person in collection.find({'salary' : {'$gt' : 10000}}, limit = 10).sort({'salary': -1}):
        items.append(person)
    write_result_to_json(items, 'sort_by_salary_2.json')

def filter_by_work_year(collection):
    items = []
    for person in (collection
                   .find({'work_year': {'$lt': 2021}}, limit = 15)
                   .sort({'salary': -1})):
        items.append(person)
    write_result_to_json(items, 'filter_by_work_year.json')

def complex_filter_by_company_location_and_job_title(collection):
    items = []
    for person in (collection
                   .find({'company_location': 'US',
                          'job_title': {'$in' : ['Machine Learning Engineer', 'Data Specialist', 'ML Engineer']}},
                         limit = 10)
                   .sort({'age': 1})):
        items.append(person)
    write_result_to_json(items, 'complex_filter_by_company_location_and_job_title.json')

def count_object_2(collection): 
    result = collection.count_documents({
        'remote_ratio': {"$gte": 50, "$lte" : 100},
        'work_year': {"$gte": 2022, "$lte" : 2023},
        '$or' : [
            {'salary': {'$gt': 10000, '$lt': 50000}}]
        
        })
    # print(result)
    write_result_to_json(result, 'count_object_2.json')



#2
def get_stat_by_salary_2(collection):
    q = [
        {
            "$group": {
                "_id" : "result",
                "max" : {"$max" : "$salary"},
                "min" : {"$min" : "$salary"},
                "avg" : {"$avg" : "$salary"}}}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'get_stat_by_salary_2.json')

def get_freq_by_job_title(collection):
    q = [
        {'$group' : {
            '_id' : '$job_title',
            'count' : {'$sum': 1}}}] #{'sort' : {'count' : -1}}
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'get_freq_by_job_title.json')

def get_salary_stat_by_column_2(collection, column_name):
    q = [
        {
            '$group': {
                '_id' : f'${column_name}',
                "max" : {"$max" : "$salary"},
                "min" : {"$min" : "$salary"},
                "avg" : {"$avg" : "$salary"}}}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'get_salary_stat_by_column_2.json')
    
def get_remote_ratio_stat_by_column(collection,column_name):
    q = [
        {
            "$group":{
                '_id' : f'${column_name}',
                'max' : {'$max' : '$remote_ratio'},
                'min' : {'$min' : '$remote_ratio'},
                'avg' : {'$avg' : '$remote_ratio'}}}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'get_remote_ratio_stat_by_column.json')
    
    
def max_salary_by_min_remote_ratio(collection):
    q = [
        {
            "$sort":{
                'age': 1, "salary": -1}},
        
                {"$limit": 1}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'max_salary_by_min_remote_ratio.json')

def min_salary_by_max_remote_ratio(collection):
    q = [
        {
            "$sort":{
                'age': -1, "salary": 1}},
                {"$limit": 1}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'min_salary_by_max_remote_ratio.json')

def big_query_salary_in_usd(collection):
    q = [
        {
            "$match" : {
                'salary' : {'$gt' : 10000}}},
        {
            '$group' : {
                '_id' : '$company_location',
                'min' : {'$min' : '$salary_in_usd'},
                'max' : {'$max' : '$salary_in_usd'},
                'avg' : {'$avg' : '$salary_in_usd'}}},
        {
            '$sort': {'min' : -1}}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'big_query_salary_in_usd.json')

def big_query_location(collection):
    q = [
        {
            "$match" : {
                'company_location' : {'$in' : ['GB', 'CA']},
                'job_title' : {'$nin' :['Machine Learning Engineer', 'Data Scientist', 'ML Engineer']},
                '$or' : [
                    {'salary_in_usd': {'$gt' : 2000, '$lt' : 70000}}]
                }},
        {
            '$group' : {
                "_id" : 'result',
                'min' : {'$min' : '$salary'},
                'max' : {'$max' : '$salary'},
                'avg' : {'$avg' : '$salary'}
            }}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'big_query_location.json')

def big_query_size(collection):
    q = [
        {
            "$match" : {
                'company_size' : {'$in' : ['S', 'M']},
                'job_title' : {'$nin' :['Machine Learning Engineer', 'Data Scientist', 'ML Engineer']},
                '$or' : [
                    {'salary': {'$gt' : 90000, '$lt' : 700000}}]
                }},
        {
            '$group' : {
                "_id" : 'result',
                'min' : {'$min' : '$remote_ratio'},
                'max' : {'$max' : '$remote_ratio'},
                'avg' : {'$avg' : '$remote_ratio'}
            }},
        {
            '$sort' : {'salary' : -1}}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'big_query_size.json')


#3
def connect_db_2():
    client = MongoClient("localhost", 27017)
    db = client['test-5-database']
    return db.proba

def delete_by_salary(collection):
    result = collection.delete_many({
        '$or': [
            {'salary' : {'$lt' : 150000}},
            {'salary' : {'$gt' : 600000}}]})
    print(result)
    

def update_salary_in_usd(collection):
    result = collection.update_many({}, {
        '$inc': {'salary_in_usd' : 1}})
    print(result)

def increase_salary_by_job_title(collection):
    filter = {
        'job_title' : {'$nin' :['Machine Learning Engineer', 'Data Scientist', 'ML Engineer']}}
    update = { 
        
            '$mul' : { 'salary' :  1.05}}
    result = collection.update_many(filter, update)
    print(result)


def increase_salary_by_company_location(collection):
    filter = {
        'company_location' : {'$in' : ['GB', 'CA']}}
    update = { 
        
            '$mul' : { 'salary' :  1.07}}
    result = collection.update_many(filter, update)
    print(result)

def increase_salary_by_all(collection):
    filter = {
        'company_location' : {'$nin' : ['GB', 'CA']},
        'job_title' : {'$nin' :['Machine Learning Engineer', 'Data Scientist', 'ML Engineer']}}
    update = { 
        
            '$mul' : { 'salary' :  1.1}}
    result = collection.update_many(filter, update)
    print(result)

        


insert_many(connect_db(), salary)
sort_by_salary(connect_db())
filter_by_work_year(connect_db())
complex_filter_by_company_location_and_job_title(connect_db())
count_object_2(connect_db())
get_stat_by_salary_2(connect_db())
get_freq_by_job_title(connect_db())
get_salary_stat_by_column_2(connect_db(), 'company_size')
get_remote_ratio_stat_by_column(connect_db(), 'company_location')
max_salary_by_min_remote_ratio(connect_db())
min_salary_by_max_remote_ratio(connect_db())
big_query_salary_in_usd(connect_db())
big_query_location(connect_db())
big_query_size(connect_db())

with open('salaries_cybersecurity.json', "r") as f:
    data = json.load(f)

for item in data:
    item['work_year'] = int(item['work_year'])
    item['salary'] = int(item['salary'])
    item['salary_in_usd'] = int(item['salary_in_usd'])
    

with open('salary_cybersecurity.json', 'w') as f:
    json.dump(data, f)

with open('salary_cybersecurity.json', "r") as f:
    json_data = json.load(f)
    
insert_many(connect_db_2(), json_data)

delete_by_salary(connect_db_2())
update_salary_in_usd(connect_db_2())
increase_salary_by_job_title(connect_db_2())
increase_salary_by_company_location(connect_db_2())
increase_salary_by_all(connect_db_2())






















