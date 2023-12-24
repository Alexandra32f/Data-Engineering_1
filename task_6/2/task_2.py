# -*- coding: utf-8 -*-
"""
Created on Fri Dec 22 22:10:50 2023

@author: alex-
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import json
import seaborn as sns

pd.set_option("display.max_rows", 20,"display.max_columns", 60)

def read_file(file_name):
    # return next(pd.read_csv(file_name, chunksize = 100, compression = 'zip'))
    return pd.read_csv(file_name)

#compare
def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep= True).sum()
    else: # предположим,что если не датафрейм, а серия
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024**2 # преобразуем байты в мегабайты
    return "{:03.2f} MB".format(usage_mb)

def get_memory_stat_by_chunk(df, output_file):
    file_size = os.path.getsize(df)
    total_memory_usage = 0
    start_data = next(pd.read_csv(df, chunksize=100_000))

    columns_stats = {
        column: {
            'memory_abs': 0,
            'memory_per': 0,
            'dtype': str(start_data.dtypes[column])
        }
        for column in start_data}
    for chunk in pd.read_csv(df, chunksize=100_000):
        chunk_memory = chunk.memory_usage(deep=True)
        total_memory_usage += float(chunk_memory.sum())
        for column in chunk:
            columns_stats[column]['memory_abs'] += float(chunk_memory[column])
            
    for col in columns_stats.keys():
        columns_stats[col]['memory_per'] = round(columns_stats[col]['memory_abs'] / total_memory_usage * 100, 4)
        columns_stats[col]['memory_abs'] = columns_stats[col]['memory_abs'] // 1024 
        

    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(columns_stats, file, indent = 4)
 
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
 
    
def opt_obj(df):
    converted_obj = pd.DataFrame()
    dataset_obj = df.select_dtypes(include=['object']).copy() # choose in DataFrame all columns with type object
    
    for col in dataset_obj.columns:
        num_unique_values = len(dataset_obj[col].unique()) # number of unique 
        num_total_values = len(dataset_obj[col]) # number of all values
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
    
    
   
file_name = "G:/Мой диск/ID/[2]automotive.csv.zip"

column_type = {
    "firstSeen": object,
    "brandName": object,
    "modelName": object,
    "askPrice": pd.Int64Dtype(),
    "isNew": "bool",
    "vf_Wheels": pd.Int64Dtype(),
    "vf_Seats": pd.Int64Dtype(),
    "vf_Windows": pd.Int64Dtype(),
    "vf_WheelSizeRear": pd.Int64Dtype(),
    "vf_WheelBaseShort": "float64"
}

total_size = 0
index = 0 
has_header = True
for part in pd.read_csv(file_name, 
                           usecols=lambda x: x in column_type.keys(),
                           dtype=column_type,
                           chunksize=500_000, 
                           compression='zip',
                           na_values=['NA']):
    print(f"index: {index}, cum_size = {total_size}")
    index = index + 1
    total_size = total_size + part.memory_usage(deep=True).sum()
    print(part.shape)
    part.dropna().to_csv("df_2.csv", mode='a', header=has_header)
    has_header = False
    print(part.shape)
    
print(total_size)

file_name = "df_2.csv"
dataset = read_file(file_name)

file = "result_2.json"
get_memory_stat_by_column(dataset, file)
file_chunk = "result_chunk.json"
get_memory_stat_by_chunk(dataset, file_chunk)

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

opt_dtypes = optimized_dataset.dtypes

for key in dataset.columns:
    need_column[key] = opt_dtypes[key]
    print(f"{key}:{opt_dtypes[key]}")

with open("dtypes_2.json", mode="w",  encoding='utf-8') as file:
    dtype_json = need_column.copy()
    for key in dtype_json.keys():
        dtype_json[key] = str(dtype_json[key])
    json.dump(dtype_json, file, indent = 4)





# for part in dataset:
#     vin = part['vin']
    
# file_size = os.path.getsize(file_name)
# print(f"file size               = {file_size // 1024:10} КБ")

# dataset.info(memory_usage= 'deep') # сколько занимает озу
# print(dataset.shape) #размеры набора данных


#PLOTTING

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

need_dtypes = read_types("dtypes_2.json")

dataset = pd.read_csv("df_2.csv",
                  usecols=lambda x: x in need_dtypes.keys(),
                  dtype=column_type,
                  parse_dates=['date'])
dataset.info(memory_usage='deep')

#распределения по моделям
plt.figure(figsize=(8, 6))
plot = sns.countplot(x='modelName', data=dataset)
plot.get_figure().savefig('modelName.png')

#распредление по брендовым названиям
lot = sns.histplot(data=dataset, x="brandName", hue="brandName", bins=60)
lot.get_figure().savefig('brandName.png')

#круговая диаграмма распределения по моделям
tol = dataset.groupby(['modelName'])['modelName'].count()
circ = plt.pie(tol, labels = tol.index, y=tol.keys(), autopct='%1.1f%%', title='modelName')
plt.savefig('modelName.png')

#распределние от брендов
fig, ax = plt.subplots()
plot_4 = sns.scatterplot(data=dataset, x='brandName', y='vf_WheelBaseShort')
plt.savefig("ask_brandName.png")



#Тепловая карта
plt.figure(figsize=(16,16))
num = ['askPrice', 'isNew', 'vf_Seats','vf_WheelBaseShort']
plot = sns.heatmap(dataset[num].corr(), cmap='coolwarm')
plot.get_figure().savefig('corr_matrix_2.png')

