# -*- coding: utf-8 -*-
"""
Created on Sun Dec 24 22:32:12 2023

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
    return pd.read_csv(file_name, compression='zip')

 
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
    
    

file_name = "C:/Users/alex-/Downloads/la_crime_2010_to_2023.csv.zip"
dataset = read_file(file_name)

file = "result_6.json"
get_memory_stat_by_column(dataset, file)
dataset.info(memory_usage= 'deep') # сколько занимает озу


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

need_column = dict()
column_names = ['Cross Street', 'Vict Age', 'TIME OCC',
           'AREA', 'AREA NAME', 'Status',
           'Vict Sex', 'Premis Desc', 'Status', 'LOCATION']

opt_dtypes = optimized_dataset.dtypes

for key in column_names:
    need_column[key] = opt_dtypes[key]
    print(f"{key}:{opt_dtypes[key]}")

with open("dtypes_6.json", mode="w", encoding = 'utf-8') as file:
    dtype_json = need_column.copy()
    for key in dtype_json.keys():
        dtype_json[key] = str(dtype_json[key])
    json.dump(dtype_json, file)
    
# Работа с чанкам
has_header = True
for chunk in pd.read_csv(file_name,
                         usecols=lambda x: x in column_names,
                         dtype=need_column,
                         chunksize=100_000):
    # print(mem_usage(chunk))
    chunk.to_csv('df_6.csv', mode='a', header=has_header)
    has_header = False
  
    

    
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

need_dtypes = read_types("dtypes_6.json")

dataset = pd.read_csv("df_6.csv",
                  usecols=lambda x: x in need_dtypes.keys(),
                  dtype=need_column)
dataset.info(memory_usage='deep')
    
#1
plt.figure(figsize=(8, 6))
plot = sns.countplot(x='AREA', data=dataset)
plot.get_figure().savefig('hist_AREA.png')

# График круговой диаграммы
plt.figure(figsize=(8, 6))
plot_2 =  dataset['Status'].value_counts().plot(kind='pie', autopct='%1.1f%%')
plot_2.get_figure().savefig('round_Status.png')

# Гистограмма 
plt.figure(figsize=(8, 6))
plot_3 = sns.histplot(dataset['Vict Age'], bins=20)
plot_3.get_figure().savefig('Vict Age.png')

fig, ax = plt.subplots()
plot_4 = sns.scatterplot(data=dataset, x='TIME OCC', y='Vict Sex')
plt.savefig("Vict Sex_time.png")

#Тепловая карта
plt.figure(figsize=(8, 6))
plot_5 = sns.heatmap(dataset.corr(), annot=True, cmap='coolwarm')
plot_5.get_figure().savefig('corr_matrix_6.png')
    