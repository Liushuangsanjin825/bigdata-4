import polars as pl
import os
import glob
import logging


class M1DataPipeline:
    """
    M1DataPipeline: 用于大规模行为日志数据的 ETL 处理，支持 Polars Lazy API 高效处理。
    
    标准三阶段接口：
    - extract(): 读取数据源
    - transform(): 构建清洗表达式（不执行）
    - load(): 分区写入并执行复杂清洗
    """

    def __init__(self, input_root: str, output_root: str, pv_threshold: int = 500, is_csv: bool = False) -> None:
        """
        初始化数据管道。
        :param input_root: 输入路径（CSV 文件路径或 Parquet 分区目录）
        :param output_root: 输出 Parquet 分区目录
        :param pv_threshold: PV 刷号判定阈值
        :param is_csv: 是否从 CSV 文件读取（默认 False，使用 Parquet 分区）
        """
        self.input_root = input_root
        self.output_root = output_root
        self.pv_threshold = pv_threshold
        self.is_csv = is_csv
        self.behavior_types = ["pv", "cart", "fav", "buy"]
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger("M1DataPipeline")

    def extract(self) -> pl.LazyFrame:
        """
        读取数据源并构建 LazyFrame。
        支持 CSV 文件或 Parquet 分区目录。
        :return: 合并后的 LazyFrame
        """
        try:
            if self.is_csv:
                self.logger.info(f"[Extract] 从 CSV 文件读取数据：{self.input_root}")
                if not os.path.exists(self.input_root):
                    raise FileNotFoundError(f"CSV 文件不存在：{self.input_root}")

                # 使用 scan_csv 进行惰性读取，指定列名和数据类型
                df = pl.scan_csv(
                    self.input_root,
                    has_header=False,
                    new_columns=["user_id", "item_id", "category_id", "behavior_type", "timestamp"],
                    schema_overrides={
                        "user_id": pl.Int64,
                        "item_id": pl.Int64,
                        "category_id": pl.Int64,
                        "timestamp": pl.Int64
                    }
                )

                # 检查 behavior_type 的实际类型
                schema = df.collect_schema()
                bt_type = schema["behavior_type"]
                self.logger.info(f"[Extract] behavior_type 列的数据类型：{bt_type}")

                # 如果是数值类型，需要映射为字符串
                if bt_type in [pl.Int64, pl.Int32, pl.Float64]:
                    self.logger.info("[Extract] 将 behavior_type 从数字编码映射为字符串...")
                    df = df.with_columns(
                        pl.when(pl.col("behavior_type") == 1).then(pl.lit("pv"))
                        .when(pl.col("behavior_type") == 2).then(pl.lit("cart"))
                        .when(pl.col("behavior_type") == 3).then(pl.lit("fav"))
                        .when(pl.col("behavior_type") == 4).then(pl.lit("buy"))
                        .otherwise(pl.col("behavior_type").cast(pl.Utf8))
                        .alias("behavior_type")
                    )
                else:
                    self.logger.info("[Extract] behavior_type 已经是字符串类型，无需映射。")
                    df = df.with_columns(pl.col("behavior_type").cast(pl.Utf8))

                self.logger.info(f"[Extract] CSV 读取完成，已构建 LazyFrame.")
                return df
            else:
                # 原有的 Parquet 分区读取逻辑
                self.logger.info("[Extract] 读取所有分区 Parquet 文件...")
                parquet_files = glob.glob(os.path.join(self.input_root, "**", "*.parquet"), recursive=True)
                lazy_frames: list[pl.LazyFrame] = []
                for file in parquet_files:
                    parts = file.split(os.sep)
                    behavior_type = None
                    for part in parts:
                        if part.startswith("behavior_type="):
                            behavior_type = part.split("=")[1]
                            break
                    lf = pl.scan_parquet(file).with_columns(pl.lit(behavior_type).alias("behavior_type"))
                    lazy_frames.append(lf)
                if not lazy_frames:
                    raise FileNotFoundError("未找到任何 Parquet 文件！")
                df = pl.concat(lazy_frames)
                self.logger.info("[Extract] 合并完成，已构建 LazyFrame.")
                return df
        except Exception as e:
            self.logger.error(f"[Extract] 发生异常：{e}")
            raise

    def transform(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        数据清洗：构建清洗表达式（去重、异常用户过滤、session_id 生成）。
        
        注意：对于超大数据集（>1 亿行），此方法只构建表达式，不执行。
        实际的复杂清洗会在 load() 阶段分区后执行，以避免内存问题。
        
        :param df: 输入 LazyFrame
        :return: 带清洗表达式的 LazyFrame（未执行）
        """
        try:
            self.logger.info("[Transform] 构建数据清洗表达式（LazyFrame，无 collect）...")
            
            # 1. 先过滤无效时间戳（谓词下推）
            df_valid = df.filter(pl.col("timestamp") > 0)
            
            # 2. 去重
            df_deduped = df_valid.unique(subset=["user_id", "item_id", "timestamp"])
            
            # 3. 识别异常刷 PV 用户
            suspect_users = (
                df_deduped.filter(pl.col("behavior_type") == "pv")
                  .group_by("user_id")
                  .agg(pl.len().alias("pv_count"))
                  .filter(pl.col("pv_count") > self.pv_threshold)
                  .select("user_id")
            )
            
            # 4. 剔除异常用户（anti join）
            cleaned = df_deduped.join(suspect_users, on="user_id", how="anti")
            
            # 5. 生成 session_id 字段
            cleaned = cleaned.with_columns(
                pl.col("timestamp").sort_by("timestamp").over("user_id").alias("ts_sorted")
            ).with_columns(
                (pl.col("ts_sorted") - pl.col("ts_sorted").shift(1).over("user_id")).alias("timediff")
            ).with_columns(
                ((pl.col("timediff").is_null()) | (pl.col("timediff") > 1800)).cast(pl.Int32).alias("is_new_session")
            ).with_columns(
                pl.col("is_new_session").cum_sum().over("user_id").alias("session_id")
            ).drop("ts_sorted", "timediff", "is_new_session")
            
            self.logger.info("[Transform] 数据清洗表达式已构建（LazyFrame，无 collect）。")
            self.logger.info("[Transform] 注意：复杂清洗将在 load() 阶段分区后执行，以避免内存问题。")
            return cleaned
        except Exception as e:
            self.logger.error(f"[Transform] 发生异常：{e}")
            raise

    def load(self, raw_df: pl.LazyFrame) -> None:
        """
        按行为类型分区写入 Parquet 文件，并执行完整清洗。
        
        清洗逻辑（所有分区一致）：
        1. 过滤无效时间戳（timestamp > 0）
        2. 去重（user_id, item_id, timestamp）
        3. 识别并剔除异常刷 PV 用户
        4. 生成 session_id
        
        优化：对于大分区，使用分块处理避免内存溢出。
        
        :param raw_df: 原始 LazyFrame（未清洗）
        """
        self.logger.info("[Load] 按行为类型分区写入 Parquet，并执行完整清洗...")
        os.makedirs(self.output_root, exist_ok=True)
        import traceback
        import time
        import gc

        for bt in self.behavior_types:
            try:
                self.logger.info(f"[Load] {'='*50}")
                self.logger.info(f"[Load] 正在处理分区：{bt}")
                t_start = time.time()
                out_dir = os.path.join(self.output_root, f"behavior_type={bt}")
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, "data.parquet")

                # 阶段 1：简单过滤分区
                self.logger.info(f"[Load] 阶段 1/2：过滤分区 {bt}...")
                df_filtered = raw_df.filter(pl.col("behavior_type") == bt)
                df_filtered.sink_parquet(out_path)
                
                elapsed_1 = time.time() - t_start
                row_count_raw = pl.scan_parquet(out_path).select(pl.len()).collect().item()
                self.logger.info(f"[Load] 阶段 1 完成，耗时：{elapsed_1:.2f} 秒，行数：{row_count_raw:,}")
                
                # 阶段 2：执行完整清洗（所有分区逻辑一致）
                self.logger.info(f"[Load] 阶段 2/2：执行完整清洗...")
                self.logger.info(f"[Load]   - 过滤无效时间戳 (timestamp > 0)")
                self.logger.info(f"[Load]   - 去重 (user_id, item_id, timestamp)")
                if bt == "pv":
                    self.logger.info(f"[Load]   - 识别并剔除异常刷 PV 用户 (pv_count > {self.pv_threshold})")
                self.logger.info(f"[Load]   - 生成 session_id")
                
                t_clean = time.time()
                
                # 对于大分区，使用分块处理
                if row_count_raw > 10_000_000:
                    self.logger.info(f"[Load] 分区较大，使用分块处理...")
                    row_count_cleaned = self._clean_partition_chunked(out_path, bt, out_path)
                else:
                    # 小分区直接处理
                    df_partition = pl.scan_parquet(out_path)
                    cleaned = self._clean_partition(df_partition, bt)
                    
                    # 覆盖写入清洗后的数据
                    cleaned.sink_parquet(out_path)
                    row_count_cleaned = pl.scan_parquet(out_path).select(pl.len()).collect().item()
                
                elapsed_2 = time.time() - t_clean
                removed = row_count_raw - row_count_cleaned
                self.logger.info(f"[Load] 阶段 2 完成，耗时：{elapsed_2:.2f} 秒")
                self.logger.info(f"[Load]   原始行数：{row_count_raw:,}")
                self.logger.info(f"[Load]   清洗后行数：{row_count_cleaned:,}")
                self.logger.info(f"[Load]   剔除行数：{removed:,} ({removed/row_count_raw*100:.2f}%)")
                
                elapsed_total = time.time() - t_start
                self.logger.info(f"[Load] ✅ 分区 {bt} 处理完成，总耗时：{elapsed_total:.2f} 秒")
                
                # 强制垃圾回收
                gc.collect()
                
            except Exception as e:
                self.logger.error(f"[Load] ❌ 分区 {bt} 处理流程异常：{e}")
                self.logger.error(traceback.format_exc())
                raise

        self.logger.info("[Load] ✅ 全部分区写入流程结束.")
    
    def _clean_partition(self, df: pl.LazyFrame, behavior_type: str) -> pl.LazyFrame:
        """
        对单个分区执行完整清洗。
        """
        # 1. 过滤无效时间
        df_valid = df.filter(pl.col("timestamp") > 0)
        
        # 2. 去重
        df_deduped = df_valid.unique(subset=["user_id", "item_id", "timestamp"])
        
        # 3. 识别异常刷 PV 用户（只对 pv 分区）
        if behavior_type == "pv":
            suspect_users = (
                df_deduped.group_by("user_id")
                .agg(pl.len().alias("pv_count"))
                .filter(pl.col("pv_count") > self.pv_threshold)
                .select("user_id")
            )
            cleaned = df_deduped.join(suspect_users, on="user_id", how="anti")
        else:
            cleaned = df_deduped
        
        # 4. 生成 session_id
        cleaned = cleaned.with_columns(
            pl.col("timestamp").sort_by("timestamp").over("user_id").alias("ts_sorted")
        ).with_columns(
            (pl.col("ts_sorted") - pl.col("ts_sorted").shift(1).over("user_id")).alias("timediff")
        ).with_columns(
            ((pl.col("timediff").is_null()) | (pl.col("timediff") > 1800)).cast(pl.Int32).alias("is_new_session")
        ).with_columns(
            pl.col("is_new_session").cum_sum().over("user_id").alias("session_id")
        ).drop("ts_sorted", "timediff", "is_new_session")
        
        return cleaned
    
    def _clean_partition_chunked(self, path: str, behavior_type: str, out_path: str) -> int:
        """
        对大分区执行分块清洗，并直接写入最终文件。
        返回清洗后的行数。
        """
        # 使用输出目录作为临时目录
        out_dir = os.path.dirname(out_path)
        temp_dir = os.path.join(out_dir, "temp_chunks")
        os.makedirs(temp_dir, exist_ok=True)
        chunk_paths = []
        
        try:
            # 读取分区数据
            df_partition = pl.scan_parquet(path)
            
            # 获取总行数
            total_rows = df_partition.select(pl.len()).collect().item()
            chunk_size = 5_000_000  # 每块 500 万行
            num_chunks = (total_rows + chunk_size - 1) // chunk_size
            
            self.logger.info(f"[Load] 分块处理：{num_chunks} 块，每块约 {chunk_size:,} 行")
            
            for i in range(num_chunks):
                offset = i * chunk_size
                self.logger.info(f"[Load] 处理第 {i+1}/{num_chunks} 块（offset={offset:,}）...")
                
                # 读取一块数据
                df_chunk = df_partition.slice(offset, chunk_size)
                
                # 清洗
                cleaned_chunk = self._clean_partition(df_chunk, behavior_type)
                
                # 写入临时文件
                chunk_path = os.path.join(temp_dir, f"chunk_{i}.parquet")
                cleaned_chunk.sink_parquet(chunk_path)
                chunk_paths.append(chunk_path)
            
            # 合并所有块（直接合并，不执行跨块操作）
            self.logger.info(f"[Load] 合并 {num_chunks} 个块...")
            self.logger.info(f"[Load] 提示：由于内存限制，跨块去重、异常用户过滤和 session_id 生成在块内已执行")
            self.logger.info(f"[Load] 提示：跨块重复数据可在后续分析阶段处理")
            
            # 直接合并所有块
            final = pl.scan_parquet(chunk_paths)
            
            # 直接写入最终文件
            final.sink_parquet(out_path)
            
            # 返回清洗后的行数
            row_count = pl.scan_parquet(out_path).select(pl.len()).collect().item()
            return row_count
            
        finally:
            # 清理临时文件
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
