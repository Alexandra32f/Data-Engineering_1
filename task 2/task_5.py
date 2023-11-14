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


    # return result_b
def process(data, name):
    pd.set_option('display.float_format', '{:.2f}'.format)
    return {
        f'{name}_min': int(data[name].min()),
        f'{name}_max': int(data[name].max()),
        f'{name}_mean': int(data[name].mean()),
        f'{name}_std': int(data[name].std()),
        f'{name}_sum': int(data[name].sum())
    }     

    
result = list()

result.append({
    **process(air, 'BEVs'),
    **process(air, 'PHEVs'),
    **process(air, 'EV'),
    **process(air, 'nEV')
})
# print(result)

air1 = air['County']
f1 = collections.Counter(air1)
result.append(f1)

air2 = air['State']
f2 = collections.Counter(air2)
result.append(f2)

air3 = air['user']
f3 = collections.Counter(air3)
result.append(f3)

# print(result)

with open("result.json", "w") as file:
    file.write(json.dumps(result))

with open("air.json", "w") as file:
    file.write(air.to_json(orient='split'))
    
air.to_pickle("air.pkl")
# air.to_msgpack("air.msgpack")

# with open("air.msgpack", "wb") as file:
#     file.write(air.to_msgpack(orient='split'))


print(f"json = {os.path.getsize('air.json')}")
# print(f"msgpack = {os.path.getsize('air.msgpack')}")
print(f"pkl = {os.path.getsize('air.pkl')}")
