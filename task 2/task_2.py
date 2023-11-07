# -*- coding: utf-8 -*-
"""
Created on Mon Nov  6 13:05:43 2023

@author: alex-
"""

import numpy as np
import os

matrix = np.load('./matrix_15_2.npy')

size = len(matrix)

x = list()
y = list()
z = list()

limit = 515

for i in range(0, size):
    for j in range(0, size):
        if matrix[i][j] > limit:
            x.append(i)
            y.append(j)
            z.append(matrix[i][j])
            
np.savez('points', x=x, y=y, z=z)
np.savez_compressed('points_zip', x=x, y=y, z=z)

print(f"points = {os.path.getsize('points.npz')}")
print(f"points_zip = {os.path.getsize('points_zip.npz')}")

plot_data = np.load('points.npz')
plot_data_compressed = np.load('points_zip.npz')





