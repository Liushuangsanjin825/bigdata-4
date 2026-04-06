"""
M1 Pipeline - 使用 Streaming 引擎修复内存溢出问题
核心优化:
1. 所有 collect 使用 engine="streaming"
2. 大分区按 user_id hash 分桶去重
3. 最后过滤 buy_count > pv_count 违规用户
"""
from m1_pipeline import M1DataPipeline
import logging
import polars as pl
import glob
import os
import time
import sys
import shutil
from config import INPUT_CSV, OUTPUT_ROOT, FINAL_OUTPUT, PV_THRESHOLD, IS_CSV

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("m1_pipeline_run.log", encoding='utf-8')
        ]
    )
    logger = logging.getLogger("M1PipelineRunner")

    try:
        logger.info("\n" + "=" * 60)
        logger.info("M1 数据处理流程开始（Streaming 优化版）")
        logger.info("=" * 60)
        logger.info(f"[配置] 输入文件: {INPUT_CSV}")
        logger.info(f"[配置] 输出目录: {OUTPUT_ROOT}")
        logger.info(f"[配置] 最终输出: {FINAL_OUTPUT}")
        logger.info(f"[配置] PV 阈值: {PV_THRESHOLD}")
        logger.info("=" * 60)
        
        # 验证输入文件
        if IS_CSV and not os.path.exists(INPUT_CSV):
            raise FileNotFoundError(f"输入CSV文件不存在: {INPUT_CSV}")
        
        t0 = time.time()
        
        # 设置streaming配置
        pl.Config.set_streaming_chunk_size(50_000)
        
        # 如果输入是CSV，先转成parquet分区（避免重复扫描CSV）
        if IS_CSV:
            logger.info("\n[预处理] CSV转Parquet分区...")
            lazy_frame = pl.scan_csv(
                INPUT_CSV,
                has_header=False,
                new_columns=["user_id", "item_id", "category_id", "behavior_type", "timestamp"],
                schema_overrides={
                    "user_id": pl.Int64,
                    "item_id": pl.Int64,
                    "category_id": pl.Int64,
                    "timestamp": pl.Int64
                }
            )
            
            # 检查behavior_type类型
            schema = lazy_frame.collect_schema()
            if schema["behavior_type"] in [pl.Int64, pl.Int32, pl.Float64]:
                logger.info("映射 behavior_type 从数字到字符串...")
                lazy_frame = lazy_frame.with_columns(
                    pl.when(pl.col("behavior_type") == 1).then(pl.lit("pv"))
                    .when(pl.col("behavior_type") == 2).then(pl.lit("cart"))
                    .when(pl.col("behavior_type") == 3).then(pl.lit("fav"))
                    .when(pl.col("behavior_type") == 4).then(pl.lit("buy"))
                    .otherwise(pl.col("behavior_type").cast(pl.Utf8))
                    .alias("behavior_type")
                )
            
            # 按behavior_type分区sink
            temp_parquet_dir = os.path.join(OUTPUT_ROOT, "_temp_parquet")
            os.makedirs(temp_parquet_dir, exist_ok=True)
            
            for bt in ["pv", "cart", "fav", "buy"]:
                out_dir = os.path.join(temp_parquet_dir, f"behavior_type={bt}")
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, "data.parquet")
                
                logger.info(f"  处理 {bt} 分区...")
                lazy_frame.filter(pl.col("behavior_type") == bt).sink_parquet(out_path)
                
                row_count = pl.scan_parquet(out_path).select(pl.len()).collect(engine="streaming").item()
                logger.info(f"  ✅ {bt}: {row_count:,} 行")
            
            # 使用转换后的parquet作为输入
            input_for_pipeline = temp_parquet_dir
            IS_CSV = False
            logger.info("[预处理] ✅ CSV转Parquet完成")
        else:
            input_for_pipeline = INPUT_CSV
        
        # 初始化 Pipeline（使用修复后的版本）
        logger.info("\n[初始化] 创建 Pipeline 实例...")
        pipeline = M1DataPipeline(
            input_root=input_for_pipeline,
            output_root=OUTPUT_ROOT,
            pv_threshold=PV_THRESHOLD,
            is_csv=IS_CSV
        )

        # Extract
        logger.info("\n[阶段 1/3] Extract...")
        df = pipeline.extract()

        # Transform + Load（使用新的streaming方法）
        logger.info("\n[阶段 2/3] Transform + Load (Streaming)...")
        pipeline.load(df)
        logger.info("[阶段 2] ✅ 完成")

        # 合并分区
        logger.info("\n[阶段 3/3] 合并分区...")
        parquet_files = glob.glob(os.path.join(OUTPUT_ROOT, "*.parquet"))
        if parquet_files:
            # 如果load()已经生成了最终文件，直接使用
            logger.info("[阶段 3] ✅ 已有最终文件")
        else:
            # 否则合并分区
            parquet_files = glob.glob(os.path.join(OUTPUT_ROOT, "behavior_type=*", "data.parquet"))
            if parquet_files:
                df_final = pl.scan_parquet(parquet_files)
                df_final.sink_parquet(FINAL_OUTPUT)
                logger.info(f"[阶段 3] ✅ 合并完成: {FINAL_OUTPUT}")

        t1 = time.time()
        logger.info(f"\n✅ ETL 流程完成！耗时: {t1-t0:.2f} 秒 ({(t1-t0)/60:.2f} 分钟)")
        
        # 验证输出
        if os.path.exists(FINAL_OUTPUT):
            file_size = os.path.getsize(FINAL_OUTPUT) / (1024**3)
            logger.info(f"输出文件大小: {file_size:.2f} GB")
            row_count = pl.scan_parquet(FINAL_OUTPUT).select(pl.len()).collect(engine="streaming").item()
            logger.info(f"输出文件行数: {row_count:,}")

        logger.info("=" * 60)
        logger.info("M1 数据处理流程结束")
        logger.info("=" * 60)

        # 清理临时parquet
        if IS_CSV and os.path.exists(temp_parquet_dir):
            logger.info("清理临时Parquet文件...")
            shutil.rmtree(temp_parquet_dir)

    except FileNotFoundError as e:
        logger.error(f"❌ 文件错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    except Exception as e:
        import traceback
        logger.error(f"❌ M1DataPipeline 执行失败: {e}")
        logger.error("\n详细错误堆栈:")
        logger.error(traceback.format_exc())
        sys.exit(1)
