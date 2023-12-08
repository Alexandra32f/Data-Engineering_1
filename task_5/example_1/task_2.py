# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 22:49:56 2023

@author: alex-
"""

from pymongo import MongoClient
import json
import pandas as pd
from bson import ObjectId
from bson import json_util

def connect_db():
    client = MongoClient("localhost", 27017)
    db = client['test-database']
    return db.salary

def insert_many(collection,data):
    collection.insert_many(data)
    
salary =pd.read_csv('task_2_item.csv', encoding = 'utf-8', sep = ';')
salary = salary.to_dict('records')
# print(salary[0:5])

def default_json_encoder(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def write_result_to_json(result, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(result, file, indent=4, ensure_ascii=False, default=default_json_encoder)
        

def get_stat_by_salary(collection):
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
    write_result_to_json(items, 'get_stat_by_salary.json')
        
def get_freq_by_job(collection):
    q = [
        {'$group' : {
            '_id' : '$job',
            'count' : {'$sum': 1}}}] #{'sort' : {'count' : -1}}
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'get_freq_by_job.json')
        

def get_salary_stat_by_column(collection, column_name):
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
    write_result_to_json(items, 'get_salary_stat_by_column.json')
        
def get_age_stat_by_column(collection,column_name):
    q = [
        {
            "$group":{
                '_id' : f'${column_name}',
                'max' : {'$max' : '$age'},
                'min' : {'$min' : '$age'},
                'avg' : {'$avg' : '$age'}}}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'get_age_stat_by_column.json')
        


def max_salary_by_min_age(collection):
    q = [
        {
            '$group': {
                '_id' : '$age',
                'max_salary' : {'$max' : '$salary'}}},
        {
            '$group' : {
                '_id' : 'result',
                'min_age' : {'$min' : '$_id'},
                'max_salary' : {'$max' : '$max_salary'}}}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'max_salary_by_min_age.json')



def min_salary_by_max_age(collection):
    q = [
        {
            '$group': {
                '_id' : '$age',
                'min_salary' : {'$min' : '$salary'}}},
        {
            '$group' : {
                '_id' : 'result',
                'max_age' : {'$max' : '$_id'},
                'min_salary' : {'$min' : '$min_salary'}}}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'min_salary_by_max_age.json')


def big_query(collection):
    q = [
        {
            "$match" : {
                'salary' : {'$gt' : 50000}}},
        {
            '$group' : {
                '_id' : '$city',
                'min' : {'$min' : '$age'},
                'max' : {'$max' : '$age'},
                'avg' : {'$avg' : '$age'}}},
        {
            '$sort': {'min' : -1}}]
    items = []
    for stat in collection.aggregate(q):
        items.append(stat)
    write_result_to_json(items, 'big_query.json')

def big_query_2(collection):
    q = [
        {
            "$match" : {
                'city' : {'$in' : ['Сан-Себастьян', 'Варшава', 'Севилья', 'Кишинев']},
                'job' : {'$in' :['Повар', 'Косметолог', 'Водитель']},
                '$or' : [
                    {'age': {'$gt' : 18, '$lt' : 25}},
                    {'age' :{'$gt' : 50, "$lt" : 65}}]
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
    write_result_to_json(items, 'big_query_2.json')



insert_many(connect_db(), salary)
get_stat_by_salary(connect_db())
get_freq_by_job(connect_db())
get_salary_stat_by_column(connect_db(), 'city')
get_salary_stat_by_column(connect_db(), 'job')
get_age_stat_by_column(connect_db(), 'job')
get_age_stat_by_column(connect_db(), 'city')
max_salary_by_min_age(connect_db())
min_salary_by_max_age(connect_db())
big_query(connect_db())
big_query_2(connect_db())



