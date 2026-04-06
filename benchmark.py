"""
Benchmark 脚本：对比重构优化前后的整体运行时间差异

对比方案：
1. 原始方案（bigdata2.ipynb）：
   - Pandas：一次性加载到内存
   - Polars：Lazy API 简单查询
   - DuckDB：SQL 直接查询磁盘文件

2. 重构优化方案（m1_pipeline.py）：
   - M1DataPipeline：完整 ETL 流程（Extract-Transform-Load）
   - 分块处理策略
   - 流式写入 Parquet
"""

import time
import os
import logging
import polars as pl
import duckdb
import pandas as pd
from m1_pipeline import M1DataPipeline

# 配置
INPUT_CSV = r"C:\Users\caoruijie\Desktop\UserBehavior.csv"
OUTPUT_ROOT = r"G:\Users\caoruijie\big data\clean_data_partitioned_output_benchmark"
FINAL_OUTPUT = r"G:\Users\caoruijie\big data\m1_final_clean_benchmark.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("Benchmark")


def benchmark_pandas():
    """
    原始方案 1：Pandas - 一次性加载到内存
    来源：bigdata2.ipynb
    """
    logger.info("=" * 60)
    logger.info("【原始方案 1】Pandas - 一次性加载到内存")
    logger.info("=" * 60)
    
    start = time.time()
    
    # 读取 CSV 文件（一次性加载 3.67GB 到内存）
    df = pd.read_csv(
        INPUT_CSV,
        header=None,
        names=['user_id', 'item_id', 'merchant_id', 'behavior_type', 'timestamp']
    )
    
    # 统计 behavior_type 的分布
    result = df['behavior_type'].value_counts()
    logger.info(f"行为类型分布：\n{result}")
    
    elapsed = time.time() - start
    logger.info(f"⏱️ Pandas 耗时：{elapsed:.2f} 秒")
    logger.info(f"💾 内存占用：约 3.67 GB（一次性加载）")
    
    return elapsed


def benchmark_polars_simple():
    """
    原始方案 2：Polars - Lazy API 简单查询
    来源：bigdata2.ipynb
    """
    logger.info("\n" + "=" * 60)
    logger.info("【原始方案 2】Polars - Lazy API 简单查询")
    logger.info("=" * 60)
    
    start = time.time()
    
    # 使用 Lazy API 流式执行
    q = pl.scan_csv(
        INPUT_CSV,
        has_header=False,
        new_columns=["user_id", "item_id", "category_id", "behavior_type", "timestamp"],
        schema_overrides={
            "user_id": pl.Int64,
            "item_id": pl.Int64,
            "category_id": pl.Int64,
            "timestamp": pl.Int64
        }
    ).with_columns(pl.col("behavior_type").cast(pl.Utf8))
    
    result = q.group_by("behavior_type").agg(pl.len())
    df_result = result.collect()
    
    logger.info(f"行为类型分布：\n{df_result}")
    
    elapsed = time.time() - start
    logger.info(f"⏱️ Polars 简单查询耗时：{elapsed:.2f} 秒")
    logger.info(f"💾 内存占用：低（流式处理）")
    
    return elapsed


def benchmark_duckdb_simple():
    """
    原始方案 3：DuckDB - SQL 直接查询磁盘文件
    来源：bigdata2.ipynb
    """
    logger.info("\n" + "=" * 60)
    logger.info("【原始方案 3】DuckDB - SQL 直接查询磁盘文件")
    logger.info("=" * 60)
    
    start = time.time()
    
    query = """
        SELECT column4 as behavior_type, COUNT(*) as count 
        FROM read_csv_auto('C:/Users/caoruijie/Desktop/UserBehavior.csv', header=false)
        GROUP BY behavior_type
    """
    
    result = duckdb.query(query).df()
    logger.info(f"行为类型分布：\n{result}")
    
    elapsed = time.time() - start
    logger.info(f"⏱️ DuckDB 简单查询耗时：{elapsed:.2f} 秒")
    logger.info(f"💾 内存占用：低（核外计算）")
    
    return elapsed


def benchmark_duckdb_partition():
    """
    原始方案 4：DuckDB - 分区写入 Parquet
    来源：bigdata2.ipynb 任务四
    """
    logger.info("\n" + "=" * 60)
    logger.info("【原始方案 4】DuckDB - 分区写入 Parquet")
    logger.info("=" * 60)
    
    output_dir = r"G:\Users\caoruijie\big data\clean_data_partitioned_duckdb"
    os.makedirs(output_dir, exist_ok=True)
    
    start = time.time()
    
    query = f"""
        COPY (
            SELECT 
                user_id,
                item_id,
                merchant_id,
                behavior_type,
                timestamp
            FROM read_csv('{INPUT_CSV.replace(chr(92), '/')}',
                          columns={{'user_id': 'INTEGER', 'item_id': 'INTEGER', 'merchant_id': 'INTEGER',
                                   'behavior_type': 'VARCHAR', 'timestamp': 'INTEGER'}})
            WHERE timestamp > 0 
              AND behavior_type IS NOT NULL 
              AND behavior_type != ''
        )
        TO '{output_dir.replace(chr(92), '/')}'
        (FORMAT 'parquet', PARTITION_BY 'behavior_type', OVERWRITE_OR_IGNORE true)
    """
    
    duckdb.execute(query)
    
    elapsed = time.time() - start
    logger.info(f"✅ DuckDB 分区写入完成")
    logger.info(f"⏱️ DuckDB 分区写入耗时：{elapsed:.2f} 秒")
    
    return elapsed


def benchmark_m1_pipeline():
    """
    重构优化方案：M1DataPipeline - 完整 ETL 流程
    特点：
    - 标准三阶段接口（Extract-Transform-Load）
    - 分块处理策略（避免内存溢出）
    - 流式写入 Parquet
    - 完整清洗逻辑（去重、异常用户过滤、session_id 生成）
    """
    logger.info("\n" + "=" * 60)
    logger.info("【重构优化方案】M1DataPipeline - 完整 ETL 流程")
    logger.info("=" * 60)
    
    start = time.time()
    
    # 初始化 Pipeline
    pipeline = M1DataPipeline(
        input_root=INPUT_CSV,
        output_root=OUTPUT_ROOT,
        pv_threshold=500,
        is_csv=True
    )
    
    # 阶段 1: Extract
    logger.info("\n[阶段 1/3] Extract - 读取数据...")
    t_extract0 = time.time()
    df = pipeline.extract()
    t_extract1 = time.time()
    logger.info(f"⏱️ Extract 耗时：{t_extract1 - t_extract0:.2f} 秒")
    
    # 阶段 2: Transform
    logger.info("\n[阶段 2/3] Transform - 构建清洗表达式...")
    t_transform0 = time.time()
    cleaned = pipeline.transform(df)
    t_transform1 = time.time()
    logger.info(f"⏱️ Transform 耗时：{t_transform1 - t_transform0:.2f} 秒")
    
    # 阶段 3: Load
    logger.info("\n[阶段 3/3] Load - 分区写入并执行清洗...")
    t_load0 = time.time()
    pipeline.load(df)
    t_load1 = time.time()
    logger.info(f"⏱️ Load 耗时：{t_load1 - t_load0:.2f} 秒")
    
    # 合并所有分区文件
    logger.info("\n[合并] 合并所有分区文件...")
    t_merge0 = time.time()
    import glob
    parquet_files = glob.glob(f"{OUTPUT_ROOT}/behavior_type=*/data.parquet")
    if parquet_files:
        df_final = pl.scan_parquet(parquet_files)
        df_final.sink_parquet(FINAL_OUTPUT)
    t_merge1 = time.time()
    logger.info(f"⏱️ 合并耗时：{t_merge1 - t_merge0:.2f} 秒")
    
    elapsed = time.time() - start
    logger.info(f"\n⏱️ M1DataPipeline 总耗时：{elapsed:.2f} 秒")
    logger.info(f"💾 内存占用：低（分块处理 + 流式写入）")
    
    return {
        "total": elapsed,
        "extract": t_extract1 - t_extract0,
        "transform": t_transform1 - t_transform0,
        "load": t_load1 - t_load0,
        "merge": t_merge1 - t_merge0
    }


def print_summary(results):
    """
    打印 Benchmark 总结
    """
    logger.info("\n" + "=" * 70)
    logger.info("📊 Benchmark 总结：重构优化前后对比")
    logger.info("=" * 70)
    
    print("\n" + "=" * 70)
    print("📊 Benchmark 总结：重构优化前后对比")
    print("=" * 70)
    
    print("\n【原始方案】")
    print("-" * 70)
    print(f"  1. Pandas（一次性加载）       : {results['pandas']:.2f} 秒  |  内存：~3.67 GB")
    print(f"  2. Polars（简单查询）         : {results['polars_simple']:.2f} 秒  |  内存：低")
    print(f"  3. DuckDB（简单查询）         : {results['duckdb_simple']:.2f} 秒  |  内存：低")
    print(f"  4. DuckDB（分区写入）         : {results['duckdb_partition']:.2f} 秒  |  内存：低")
    
    print("\n【重构优化方案】")
    print("-" * 70)
    print(f"  M1DataPipeline（完整 ETL）    : {results['m1_pipeline']['total']:.2f} 秒  |  内存：低")
    print(f"    ├─ Extract（读取数据）      : {results['m1_pipeline']['extract']:.2f} 秒")
    print(f"    ├─ Transform（构建表达式）  : {results['m1_pipeline']['transform']:.2f} 秒")
    print(f"    ├─ Load（分区写入+清洗）    : {results['m1_pipeline']['load']:.2f} 秒")
    print(f"    └─ Merge（合并文件）        : {results['m1_pipeline']['merge']:.2f} 秒")
    
    print("\n【性能对比】")
    print("-" * 70)
    
    # 对比 DuckDB 分区写入 vs M1DataPipeline
    duckdb_time = results['duckdb_partition']
    m1_time = results['m1_pipeline']['total']
    
    if duckdb_time > 0:
        speedup = duckdb_time / m1_time
        print(f"  M1DataPipeline vs DuckDB 分区写入:")
        print(f"    - DuckDB 耗时  : {duckdb_time:.2f} 秒（仅分区写入，无复杂清洗）")
        print(f"    - M1 耗时      : {m1_time:.2f} 秒（完整 ETL + 复杂清洗）")
        print(f"    - 功能差异     : M1 增加了去重、异常用户过滤、session_id 生成")
    
    print("\n【核心优势】")
    print("-" * 70)
    print("  ✅ 标准三阶段接口（Extract-Transform-Load）")
    print("  ✅ 完整清洗逻辑（去重、异常用户过滤、session_id 生成）")
    print("  ✅ 分块处理策略（避免 8972 万行 pv 分区内存溢出）")
    print("  ✅ 流式写入 Parquet（零冗余 Collect）")
    print("  ✅ 谓词下推优化（filter 条件下推到数据源）")
    print("  ✅ 详细日志记录（便于监控和调试）")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    logger.info("🚀 开始 Benchmark 测试...")
    logger.info(f"📁 输入文件：{INPUT_CSV}")
    logger.info(f"📁 输出目录：{OUTPUT_ROOT}")
    
    results = {}
    
    # 运行所有 Benchmark
    try:
        results['pandas'] = benchmark_pandas()
    except Exception as e:
        logger.error(f"Pandas Benchmark 失败：{e}")
        results['pandas'] = 0
    
    try:
        results['polars_simple'] = benchmark_polars_simple()
    except Exception as e:
        logger.error(f"Polars 简单查询 Benchmark 失败：{e}")
        results['polars_simple'] = 0
    
    try:
        results['duckdb_simple'] = benchmark_duckdb_simple()
    except Exception as e:
        logger.error(f"DuckDB 简单查询 Benchmark 失败：{e}")
        results['duckdb_simple'] = 0
    
    try:
        results['duckdb_partition'] = benchmark_duckdb_partition()
    except Exception as e:
        logger.error(f"DuckDB 分区写入 Benchmark 失败：{e}")
        results['duckdb_partition'] = 0
    
    try:
        results['m1_pipeline'] = benchmark_m1_pipeline()
    except Exception as e:
        logger.error(f"M1DataPipeline Benchmark 失败：{e}")
        import traceback
        traceback.print_exc()
        results['m1_pipeline'] = {"total": 0, "extract": 0, "transform": 0, "load": 0, "merge": 0}
    
    # 打印总结
    print_summary(results)
    
    logger.info("\n✅ Benchmark 测试完成！")
