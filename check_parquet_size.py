#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查 Parquet 分区文件体积并与原始 CSV 对比
"""

import os
import glob

OUTPUT_DIR = r"G:\Users\caoruijie\big data\clean_data_partitioned"
FILE_PATH = r"C:\Users\caoruijie\Desktop\UserBehavior.csv"

print("=" * 60)
print("Parquet 分区文件体积统计")
print("=" * 60)

parquet_files = glob.glob(f"{OUTPUT_DIR}/**/*.parquet", recursive=True)
total_size = 0

data = []
for behavior in ["pv", "buy", "cart", "fav"]:
    partition_path = f"{OUTPUT_DIR}/behavior_type={behavior}"
    files = glob.glob(f"{partition_path}/*.parquet")
    if files:
        size = sum(os.path.getsize(f) for f in files)
        size_mb = size / (1024 * 1024)
        size_gb = size / (1024 * 1024 * 1024)
        total_size += size
        data.append({"behavior_type": behavior, "size_mb": size_mb, "size_gb": size_gb})
        print(f"{behavior:<20} {size_mb:<15.2f} MB  ({size_gb:.4f} GB)")

print("-" * 45)
total_gb = total_size / (1024 * 1024 * 1024)
print(f"{'总计':<20} {total_size/(1024*1024):<15.2f} MB  ({total_gb:.4f} GB)")

# 原始文件大小
original_size = os.path.getsize(FILE_PATH)
original_gb = original_size / (1024 * 1024 * 1024)

print()
print("=" * 60)
print("体积对比")
print("=" * 60)
print(f"原始 CSV:  {original_gb:.2f} GB")
print(f"Parquet:   {total_gb:.2f} GB")
print(f"压缩率：   {(1 - total_size/original_size)*100:.1f}%")
print(f"节省空间：{original_gb - total_gb:.2f} GB")
