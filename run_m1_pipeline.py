from m1_pipeline import M1DataPipeline
import logging
import polars as pl
import glob
import os
import time

if __name__ == "__main__":
    # 配置
    INPUT_CSV = r"C:\Users\caoruijie\Desktop\UserBehavior.csv"
    OUTPUT_ROOT = r"G:\Users\caoruijie\big data\clean_data_partitioned_output"
    FINAL_OUTPUT = r"G:\Users\caoruijie\big data\m1_final_clean.parquet"

    try:
        print("\n" + "=" * 60)
        print("M1 数据处理流程开始")
        print("=" * 60)
        t0 = time.time()
        
        # 初始化 Pipeline
        pipeline = M1DataPipeline(
            input_root=INPUT_CSV,
            output_root=OUTPUT_ROOT,
            pv_threshold=500,
            is_csv=True
        )
        
        # 阶段 1: Extract - 读取数据
        print("\n[阶段 1/3] Extract - 读取数据...")
        df = pipeline.extract()
        
        # 阶段 2: Transform - 构建清洗表达式
        print("\n[阶段 2/3] Transform - 构建清洗表达式...")
        cleaned = pipeline.transform(df)
        
        # 阶段 3: Load - 分区写入并执行复杂清洗
        print("\n[阶段 3/3] Load - 分区写入并执行复杂清洗...")
        pipeline.load(df)  # 传入原始 df，load() 内部会分区后执行清洗
        
        t1 = time.time()
        print(f"\n✅ ETL 流程完成！耗时：{t1-t0:.2f} 秒")
        
        # 合并所有分区文件
        print("\n[合并] 合并所有分区文件...")
        t2 = time.time()
        parquet_files = glob.glob(f"{OUTPUT_ROOT}/behavior_type=*/data.parquet")
        if not parquet_files:
            raise FileNotFoundError("未找到任何分区 parquet 文件！")

        df_final = pl.scan_parquet(parquet_files)
        df_final.sink_parquet(FINAL_OUTPUT)
        t3 = time.time()
        print(f"✅ 已合并输出为 {FINAL_OUTPUT}（耗时：{t3-t2:.2f} 秒）\n")
        
        print("=" * 60)
        print("M1 数据处理流程结束")
        print("=" * 60)
        
    except Exception as e:
        import traceback
        print("\n❌ M1DataPipeline 执行失败:", e)
        traceback.print_exc()
        logging.error(f"M1DataPipeline 执行失败：{e}")
