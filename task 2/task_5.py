# -*- coding: utf-8 -*-
"""
Created on Tue Nov  7 09:55:57 2023

@author: alex-
"""

import json
import pickle
import msgpack
import csv
import os
import pandas as pd
import collections

air = pd.read_csv('Electric_Vehicle_Population_Size_History_By_County.csv', encoding = 'windows-1251',low_memory=False)

air = air.rename(columns = {'Vehicle Primary Use': 'user', 'Battery Electric Vehicles (BEVs)': 'BEVs',
                            'Plug-In Hybrid Electric Vehicles (PHEVs)': 'PHEVs',
                            'Electric Vehicle (EV) Total' : 'EV',
                            'Non-Electric Vehicle Total' : 'nEV'})
counts = air[['County', 'State', 'user', 'BEVs', 'PHEVs', 'EV', 'nEV']]

result = list()

pd.set_option('display.float_format', '{:.2f}'.format)
result.append({
    'BEVs_min' : int(air['BEVs'].min()),
    'BEVs_max' : int(air['BEVs'].max()),
    'BEVs_mean' : int(air['BEVs'].mean()),
    'BEVs_std' : int(air['BEVs'].std()),
    'BEVs_sum' : int(air['BEVs'].sum()),
    'PHEVs_min': int(air['PHEVs'].min()),
    'PHEVs_max': int(air['PHEVs'].max()),
    'PHEVs_mean': int(air['PHEVs'].mean()),
    'PHEVs_std': int(air['PHEVs'].std()),
    'PHEVs_sum': int(air['PHEVs'].sum()),
    'EV_min' : int(air['EV'].min()),
    'EV_max' : int(air['EV'].max()),
    'EV_mean' : int(air['EV'].mean()),
    'EV_std' : int(air['EV'].std()),
    'EV_sum' : int(air['EV'].sum()),
    'nEV_min' : int(air['nEV'].min()),
    'nEV_max' : int(air['nEV'].max()),
    'nEV_mean' : int(air['nEV'].mean()),
    'nEV_std' : int(air['nEV'].std()),
    'nEV_sum' : int(air['nEV'].sum())
    })

air1 = air['County']
f1 = collections.Counter(air1)
result.append(f1)

air2 = air['State']
f2 = collections.Counter(air2)
result.append(f2)

air3 = air['user']
f3 = collections.Counter(air3)
result.append(f3)

print(result)

with open("result.json", "w") as file:
    file.write(json.dumps(result))

with open("result.msgpack", "wb") as file:
    file.write(msgpack.dumps(result))

with open("result.pkl", "wb") as file:
    file.write(pickle.dumps(result))

print(f"json = {os.path.getsize('result.json')}")
print(f"msgpack = {os.path.getsize('result.msgpack')}")
print(f"pkl = {os.path.getsize('result.pkl')}")

# with open('Air_Quality.csv') as f:
#     air = csv.load(f)
#     csv_del = ','
# quality = dict()
# quality['sum'] = 0
