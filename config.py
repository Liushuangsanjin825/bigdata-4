import os

# ============================================================
# M1 DataPipeline 配置文件
# ============================================================
# 使用说明：
# 1. 复制此文件为 config_local.py（加入 .gitignore）
# 2. 修改下方的路径配置为你的实际路径
# 3. 运行 run_m1_pipeline.py 即可
# ============================================================

# --- 输入配置 ---
# CSV 文件路径（如果使用 Parquet 分区输入，请修改 is_csv=False）
INPUT_CSV = r"C:\Users\caoruijie\Desktop\UserBehavior.csv"

# --- 输出配置 ---
# 分区输出目录
OUTPUT_ROOT = r"G:\Users\caoruijie\big data\clean_data_partitioned_output"

# 最终合并输出文件
FINAL_OUTPUT = r"G:\Users\caoruijie\big data\m1_final_clean.parquet"

# --- 处理配置 ---
# PV 刷号判定阈值
PV_THRESHOLD = 500

# 是否从 CSV 文件读取（True=CSV, False=Parquet 分区）
IS_CSV = True

# --- 分块处理配置 ---
# 分块处理的行数阈值（超过此值使用分块处理）
CHUNK_THRESHOLD = 10_000_000

# 每块处理的行数
CHUNK_SIZE = 5_000_000
