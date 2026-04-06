from m1_pipeline import M1DataPipeline
import logging
import polars as pl
import glob
import os
import time
from config import INPUT_CSV, OUTPUT_ROOT, FINAL_OUTPUT, PV_THRESHOLD, IS_CSV

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger("M1PipelineRunner")

    try:
        logger.info("\n" + "=" * 60)
        logger.info("M1 数据处理流程开始")
        logger.info("=" * 60)
        logger.info(f"[配置] 输入文件: {INPUT_CSV}")
        logger.info(f"[配置] 输出目录: {OUTPUT_ROOT}")
        logger.info(f"[配置] 最终输出: {FINAL_OUTPUT}")
        logger.info(f"[配置] PV 阈值: {PV_THRESHOLD}")
        logger.info("=" * 60)
        t0 = time.time()

        # 初始化 Pipeline
        pipeline = M1DataPipeline(
            input_root=INPUT_CSV,
            output_root=OUTPUT_ROOT,
            pv_threshold=PV_THRESHOLD,
            is_csv=IS_CSV
        )

        # 阶段 1: Extract - 读取数据
        logger.info("\n[阶段 1/3] Extract - 读取数据...")
        df = pipeline.extract()

        # 阶段 2: Transform - 构建清洗表达式
        logger.info("\n[阶段 2/3] Transform - 构建清洗表达式...")
        cleaned = pipeline.transform(df)

        # 阶段 3: Load - 分区写入并执行复杂清洗
        logger.info("\n[阶段 3/3] Load - 分区写入并执行复杂清洗...")
        pipeline.load(df)  # 传入原始 df，load() 内部会分区后执行清洗

        t1 = time.time()
        logger.info(f"\n✅ ETL 流程完成！耗时：{t1-t0:.2f} 秒")

        # 合并所有分区文件
        logger.info("\n[合并] 合并所有分区文件...")
        t2 = time.time()
        parquet_files = glob.glob(os.path.join(OUTPUT_ROOT, "behavior_type=*", "data.parquet"))
        if not parquet_files:
            raise FileNotFoundError("未找到任何分区 parquet 文件！")

        df_final = pl.scan_parquet(parquet_files)
        df_final.sink_parquet(FINAL_OUTPUT)
        t3 = time.time()
        logger.info(f"✅ 已合并输出为 {FINAL_OUTPUT}（耗时：{t3-t2:.2f} 秒）\n")

        logger.info("=" * 60)
        logger.info("M1 数据处理流程结束")
        logger.info("=" * 60)

    except Exception as e:
        import traceback
        logger.error(f"❌ M1DataPipeline 执行失败: {e}")
        logger.error(traceback.format_exc())
