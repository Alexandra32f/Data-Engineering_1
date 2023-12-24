# -*- coding: utf-8 -*-
"""
Created on Tue Dec 19 12:42:37 2023

@author: alex-
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import json
import seaborn as sns

pd.set_option("display.max_rows", 20, "display.max_columns", 60)

def read_file(file_name):
    #df = pd.read_csv(datasets[year], chunksize = chunksize, compression = 'gzip')
    return pd.read_csv(file_name)

def get_memory_stat_by_column(df, output_file):
    memory_usage_stat = df.memory_usage(deep=True)
    total_memory_usage = memory_usage_stat.sum()
    result = {
        "file_in_memory_size": f"{int(total_memory_usage // 1024):10} KB",
        "column_stat" :  []
    }
    for key in df.dtypes.keys():
        result['column_stat'].append({
            "column_name": key,
            "memory_abs": int(memory_usage_stat[key] // 1024),
            "memory_per": round(memory_usage_stat[key] / total_memory_usage * 100, 4),
            "dtype": str(df.dtypes[key])
        })
    result['column_stat'].sort(key=lambda x: x['memory_abs'], reverse=True)
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(result, file, indent = 4)
 
    
#compare
def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # предположим,что если не датафрейм, а серия
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # преобразуем быйты в мегабайты
    return "{:03.2f} MB".format(usage_mb)


#convert in obj type
def opt_obj(df):
    converted_obj = pd.DataFrame()
    dataset_obj = df.select_dtypes(include=['object']).copy() # choose in DataFrame all columns with type object
    for col in dataset_obj.columns:
        num_unique_values = len(dataset_obj[col].unique()) # number of unique 
        num_total_values = len(dataset_obj[col])# number of all values
        if num_unique_values / num_total_values < 0.5:
            converted_obj.loc[:, col] = dataset_obj[col].astype('category')
        else:
            converted_obj.loc[:, col] = dataset_obj[col]
    print(mem_usage(dataset_obj))
    print(mem_usage(converted_obj))
    return converted_obj   
    
#понижаем int колонок
def opt_int(df):
    dataset_int = df.select_dtypes(include=['int']) 
    converted_int = dataset_int.apply(pd.to_numeric, downcast='unsigned')
    print(mem_usage(dataset_int))
    print(mem_usage(converted_int))
    compare_ints = pd.concat([dataset_int.dtypes, converted_int.dtypes], axis=1)
    compare_ints.columns = ['before', 'after']
    compare_ints.apply(pd.Series.value_counts)
    print(compare_ints)
    return converted_int

#понижаем float колонок
def opt_float(df):
    dataset_float = df.select_dtypes(include=['float'])
    converted_float = dataset_float.apply(pd.to_numeric, downcast='float')
    print(mem_usage(dataset_float))
    print(mem_usage(converted_float))
    compare_float = pd.concat([dataset_float.dtypes, converted_float.dtypes], axis=1)
    compare_float.columns = ['before', 'after']
    compare_float.apply(pd.Series.value_counts)
    print(compare_float)
    return converted_float
    
    

file_name = "G:/Мой диск/ID/[1]game_logs.csv"
dataset = read_file(file_name)
# file_size = os.path.getsize(file_name)

file = "result_1.json"
get_memory_stat_by_column(dataset, file)
# print(f"file size               = {file_size // 1024:10} КБ")
# get_memory_stat_by_column(dataset)
dataset.info(memory_usage= 'deep') # сколько занимает озу
# print(dataset.shape) #размеры набора данных

# print(mem_usage(dataset['day_of_week']))
# opt_dow = dataset['day_of_week'].astype('category')
# print(mem_usage(opt_dow))

# opt_obj(dataset)
# opt_int(dataset)
# opt_float(dataset)


optimized_dataset = dataset.copy()


converted_obj = opt_obj(dataset)
converted_int = opt_int(dataset)
converted_float = opt_float(dataset)

#заменяем исходные столбцы оптимизированными
optimized_dataset[converted_obj.columns] = converted_obj
optimized_dataset[converted_int.columns] = converted_int
optimized_dataset[converted_float.columns] = converted_float

print(mem_usage(dataset))
print(mem_usage(optimized_dataset))
# get_memory_stat_by_column(optimized_dataset)
# optimized_dataset.info(memory_usage= 'deep') # сколько занимает озу
# print(optimized_dataset.shape)

need_column = dict()
column_names = ['date', 'number_of_game', 'day_of_week',
           'park_id', 'v_manager_name', 'length_minutes',
           'v_hits', 'h_hits', 'h_walks', 'h_errors']

opt_dtypes = optimized_dataset.dtypes

for key in column_names:
    need_column[key] = opt_dtypes[key]
    print(f"{key}:{opt_dtypes[key]}")

with open("dtypes_1.json", mode="w",  encoding='utf-8') as file:
    dtype_json = need_column.copy()
    for key in dtype_json.keys():
        dtype_json[key] = str(dtype_json[key])
    json.dump(dtype_json, file, indent = 4)
    
# Работа с чанкам
has_header = True
for chunk in pd.read_csv(file_name,
                         usecols=lambda x: x in column_names,
                         dtype=need_column,
                         #parse_dates=['date'],
                         #infer_datetime_format=True,
                         chunksize=100_000): #сколько строк за раз будем считывать
    print(mem_usage(chunk))
    chunk.to_csv('df_1.csv', mode='a', header=has_header)
    has_header = False


# read_and_optimized = pd.read_csv(file_name, usecols = lambda x: x in column_names,# только нужные колнки
#                                  dtype = need_column)

# print(read_and_optimized.shape)
# print(mem_usage(read_and_optimized))










#plotting
def read_types(file_name):
    dtypes = {}
    with open(file_name, mode='r') as file:
        dtypes = json.load(file)
    for key in dtypes.keys():
        if dtypes[key] == 'category':
            dtypes[key] = pd.CategoricalDtype
        else:
            dtypes[key] = np.dtype(dtypes[key])
    return dtypes

need_dtypes = read_types("dtypes_1.json")

dataset = pd.read_csv("df_1.csv",
                  usecols=lambda x: x in need_dtypes.keys(),
                  dtype=need_column,
                  parse_dates=['date'])
dataset.info(memory_usage='deep')

# гистограмма распределение по дням недели
plt.figure(figsize = (30, 5))
sort_dow = dataset['day_of_week'].sort_index()
plot = sort_dow.hist()
plot.get_figure().savefig('hist.png')

plot_2 = sns.histplot(data=dataset, x="day_of_week", hue="day_of_week", bins=60)
plot_2.get_figure().savefig('hist_day_of_week.png')

# Столбчатый тип, количества игр по парку
plot_3 = sns.countplot(x='park_id', data=dataset)
plt.xlabel('PArk')
plt.ylabel('Count of game')
plt.savefig('Count_game_for_park.png')

#Круговая диаграмма распределения игр
plot_3 = dataset.groupby(['number_of_game'])['number_of_game'].count()
circ = plt.pie(plot_3, labels = plot_3.index, autopct='%1.1f%%')
plt.savefig('number_of_game.png')

# Точечная диаграма зависимости продолжительности минут от забивания
fig, ax = plt.subplots()
plot_4 = sns.scatterplot(data=dataset, x='v_hits', y='length_minutes')
plt.savefig("hits_minutes.png")

#Тепловая карта
num = ['v_hits', 'h_hits', 'h_walks', 'h_errors']
corr_matrix = dataset[num].corr()
plot_5 = sns.heatmap(corr_matrix, annot=True, cmap='coolwarm')
plot_5.get_figure().savefig('corr_matrix.png')






    










