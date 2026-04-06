# ============================================================
# M1 DataPipeline 配置文件
# ============================================================
# 使用说明：
# 1. 复制此文件为 config_local.py（加入 .gitignore）
# 2. 修改下方的路径配置为你的实际路径
# 3. 运行 run_m1_pipeline.py 即可
# ============================================================

# --- 输入配置 ---
INPUT_CSV = r"C:\Users\caoruijie\Desktop\UserBehavior.csv"

# --- 输出配置 ---
OUTPUT_ROOT = r"G:\Users\caoruijie\big data\clean_data_partitioned_output"
FINAL_OUTPUT = r"G:\Users\caoruijie\big data\m1_final_clean.parquet"

# --- 处理配置 ---
PV_THRESHOLD = 500                # PV 刷号判定阈值
IS_CSV = True                     # 是否从 CSV 文件读取
SESSION_TIMEOUT_SECONDS = 1800    # Session 超时时间（秒）

# --- 分块处理配置 ---
CHUNK_THRESHOLD = 10_000_000      # 超过此行数使用分块处理
CHUNK_SIZE = 5_000_000            # 每块处理的行数

# --- Parquet 压缩配置 ---
PARQUET_COMPRESSION = "zstd"      # 压缩算法：zstd / snappy / lz4 / uncompressed
PARQUET_COMPRESSION_LEVEL = 3     # zstd 压缩级别（1-22，值越大压缩率越高但越慢）

# --- 数据校验配置 ---
ENABLE_DATA_VALIDATION = True     # 是否启用数据质量校验
MAX_NULL_RATIO = 0.01             # 允许的最大空值比例（1%）
