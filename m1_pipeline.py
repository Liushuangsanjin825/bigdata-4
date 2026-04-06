import polars as pl
import os
import glob
import logging
import shutil
import tempfile
import gc
import time
import traceback


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
                lazy_frame = pl.scan_csv(
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
                schema = lazy_frame.collect_schema()
                behavior_type_dtype = schema["behavior_type"]
                self.logger.info(f"[Extract] behavior_type 列的数据类型：{behavior_type_dtype}")

                # 如果是数值类型，需要映射为字符串
                if behavior_type_dtype in [pl.Int64, pl.Int32, pl.Float64]:
                    self.logger.info("[Extract] 将 behavior_type 从数字编码映射为字符串...")
                    lazy_frame = lazy_frame.with_columns(
                        pl.when(pl.col("behavior_type") == 1).then(pl.lit("pv"))
                        .when(pl.col("behavior_type") == 2).then(pl.lit("cart"))
                        .when(pl.col("behavior_type") == 3).then(pl.lit("fav"))
                        .when(pl.col("behavior_type") == 4).then(pl.lit("buy"))
                        .otherwise(pl.col("behavior_type").cast(pl.Utf8))
                        .alias("behavior_type")
                    )
                else:
                    self.logger.info("[Extract] behavior_type 已经是字符串类型，无需映射。")
                    lazy_frame = lazy_frame.with_columns(pl.col("behavior_type").cast(pl.Utf8))

                self.logger.info(f"[Extract] CSV 读取完成，已构建 LazyFrame.")
                return lazy_frame
            else:
                # 原有的 Parquet 分区读取逻辑
                self.logger.info("[Extract] 读取所有分区 Parquet 文件...")
                parquet_files = glob.glob(os.path.join(self.input_root, "**", "*.parquet"), recursive=True)
                lazy_frames: list[pl.LazyFrame] = []
                for parquet_file in parquet_files:
                    parts = parquet_file.split(os.sep)
                    behavior_type_value = None
                    for part in parts:
                        if part.startswith("behavior_type="):
                            behavior_type_value = part.split("=")[1]
                            break
                    lazy_frame_partition = pl.scan_parquet(parquet_file).with_columns(pl.lit(behavior_type_value).alias("behavior_type"))
                    lazy_frames.append(lazy_frame_partition)
                if not lazy_frames:
                    raise FileNotFoundError("未找到任何 Parquet 文件！")
                lazy_frame_combined = pl.concat(lazy_frames)
                self.logger.info("[Extract] 合并完成，已构建 LazyFrame.")
                return lazy_frame_combined
        except Exception as e:
            self.logger.error(f"[Extract] 发生异常：{e}")
            raise

    def transform(self, lazy_frame: pl.LazyFrame) -> pl.LazyFrame:
        """
        数据清洗：构建清洗表达式（去重、异常用户过滤、session_id 生成）。

        注意：对于超大数据集（>1 亿行），此方法只构建表达式，不执行。
        实际的复杂清洗会在 load() 阶段分区后执行，以避免内存问题。

        :param lazy_frame: 输入 LazyFrame
        :return: 带清洗表达式的 LazyFrame（未执行）
        """
        try:
            self.logger.info("[Transform] 构建数据清洗表达式（LazyFrame，无 collect）...")

            # 1. 先过滤无效时间戳（谓词下推）
            lazy_frame_valid = lazy_frame.filter(pl.col("timestamp") > 0)

            # 2. 去重
            lazy_frame_deduped = lazy_frame_valid.unique(subset=["user_id", "item_id", "timestamp"])

            # 3. 识别异常刷 PV 用户
            suspect_users = (
                lazy_frame_deduped.filter(pl.col("behavior_type") == "pv")
                  .group_by("user_id")
                  .agg(pl.len().alias("pv_count"))
                  .filter(pl.col("pv_count") > self.pv_threshold)
                  .select("user_id")
            )

            # 4. 剔除异常用户（anti join）
            lazy_frame_cleaned = lazy_frame_deduped.join(suspect_users, on="user_id", how="anti")

            # 5. 生成 session_id 字段
            lazy_frame_cleaned = lazy_frame_cleaned.with_columns(
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
            return lazy_frame_cleaned
        except Exception as e:
            self.logger.error(f"[Transform] 发生异常：{e}")
            raise

    def load(self, raw_lazy_frame: pl.LazyFrame) -> None:
        """
        按行为类型分区写入 Parquet 文件，并执行完整清洗。

        清洗逻辑（所有分区一致）：
        1. 过滤无效时间戳（timestamp > 0）
        2. 去重（user_id, item_id, timestamp）
        3. 识别并剔除异常刷 PV 用户
        4. 生成 session_id

        优化：对于大分区，使用分块处理避免内存溢出。

        :param raw_lazy_frame: 原始 LazyFrame（未清洗）
        """
        self.logger.info("[Load] 按行为类型分区写入 Parquet，并执行完整清洗...")
        os.makedirs(self.output_root, exist_ok=True)

        for behavior_type in self.behavior_types:
            try:
                self.logger.info(f"[Load][{behavior_type}] {'='*50}")
                self.logger.info(f"[Load][{behavior_type}] 正在处理分区...")
                time_start_partition = time.time()
                output_dir = os.path.join(self.output_root, f"behavior_type={behavior_type}")
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, "data.parquet")

                # 阶段 1：简单过滤分区
                self.logger.info(f"[Load][{behavior_type}] 阶段 1/2：过滤分区...")
                lazy_frame_filtered = raw_lazy_frame.filter(pl.col("behavior_type") == behavior_type)
                lazy_frame_filtered.sink_parquet(output_path)

                elapsed_phase1 = time.time() - time_start_partition
                row_count_raw = pl.scan_parquet(output_path).select(pl.len()).collect().item()
                self.logger.info(f"[Load][{behavior_type}] 阶段 1 完成，耗时：{elapsed_phase1:.2f} 秒，行数：{row_count_raw:,}")

                # 阶段 2：执行完整清洗（所有分区逻辑一致）
                self.logger.info(f"[Load][{behavior_type}] 阶段 2/2：执行完整清洗...")
                self.logger.info(f"[Load][{behavior_type}]   - 过滤无效时间戳 (timestamp > 0)")
                self.logger.info(f"[Load][{behavior_type}]   - 去重 (user_id, item_id, timestamp)")
                if behavior_type == "pv":
                    self.logger.info(f"[Load][{behavior_type}]   - 识别并剔除异常刷 PV 用户 (pv_count > {self.pv_threshold})")
                self.logger.info(f"[Load][{behavior_type}]   - 生成 session_id")

                time_start_clean = time.time()

                # 对于大分区，使用分块处理
                if row_count_raw > 10_000_000:
                    self.logger.info(f"[Load][{behavior_type}] 分区较大，使用分块处理...")
                    row_count_cleaned = self._clean_partition_chunked(output_path, behavior_type, output_path)
                else:
                    # 小分区直接处理
                    lazy_frame_partition = pl.scan_parquet(output_path)
                    lazy_frame_cleaned = self._clean_partition(lazy_frame_partition, behavior_type)

                    # 覆盖写入清洗后的数据
                    lazy_frame_cleaned.sink_parquet(output_path)
                    row_count_cleaned = pl.scan_parquet(output_path).select(pl.len()).collect().item()

                elapsed_phase2 = time.time() - time_start_clean
                removed_count = row_count_raw - row_count_cleaned
                self.logger.info(f"[Load][{behavior_type}] 阶段 2 完成，耗时：{elapsed_phase2:.2f} 秒")
                self.logger.info(f"[Load][{behavior_type}]   原始行数：{row_count_raw:,}")
                self.logger.info(f"[Load][{behavior_type}]   清洗后行数：{row_count_cleaned:,}")
                self.logger.info(f"[Load][{behavior_type}]   剔除行数：{removed_count:,} ({removed_count/row_count_raw*100:.2f}%)")

                elapsed_total = time.time() - time_start_partition
                self.logger.info(f"[Load][{behavior_type}] ✅ 分区处理完成，总耗时：{elapsed_total:.2f} 秒")

                # 显式释放引用并强制垃圾回收
                del lazy_frame_filtered
                gc.collect()

            except Exception as e:
                self.logger.error(f"[Load][{behavior_type}] ❌ 分区处理流程异常：{e}")
                self.logger.error(traceback.format_exc())
                raise

        self.logger.info("[Load] ✅ 全部分区写入流程结束.")
    
    def _clean_partition(self, lazy_frame: pl.LazyFrame, behavior_type: str) -> pl.LazyFrame:
        """
        对单个分区执行完整清洗。
        """
        # 1. 过滤无效时间
        lazy_frame_valid = lazy_frame.filter(pl.col("timestamp") > 0)

        # 2. 去重
        lazy_frame_deduped = lazy_frame_valid.unique(subset=["user_id", "item_id", "timestamp"])

        # 3. 识别异常刷 PV 用户（只对 pv 分区）
        if behavior_type == "pv":
            suspect_users = (
                lazy_frame_deduped.group_by("user_id")
                .agg(pl.len().alias("pv_count"))
                .filter(pl.col("pv_count") > self.pv_threshold)
                .select("user_id")
            )
            lazy_frame_cleaned = lazy_frame_deduped.join(suspect_users, on="user_id", how="anti")
        else:
            lazy_frame_cleaned = lazy_frame_deduped

        # 4. 生成 session_id
        lazy_frame_cleaned = lazy_frame_cleaned.with_columns(
            pl.col("timestamp").sort_by("timestamp").over("user_id").alias("ts_sorted")
        ).with_columns(
            (pl.col("ts_sorted") - pl.col("ts_sorted").shift(1).over("user_id")).alias("timediff")
        ).with_columns(
            ((pl.col("timediff").is_null()) | (pl.col("timediff") > 1800)).cast(pl.Int32).alias("is_new_session")
        ).with_columns(
            pl.col("is_new_session").cum_sum().over("user_id").alias("session_id")
        ).drop("ts_sorted", "timediff", "is_new_session")

        return lazy_frame_cleaned
    
    def _clean_partition_chunked(self, path: str, behavior_type: str, out_path: str) -> int:
        """
        对大分区执行分块清洗，并直接写入最终文件。
        返回清洗后的行数。
        """
        # 使用 tempfile 自动管理临时目录生命周期
        with tempfile.TemporaryDirectory(dir=os.path.dirname(out_path)) as temp_dir:
            chunk_paths = []

            # 读取分区数据
            lazy_frame_partition = pl.scan_parquet(path)

            # 获取总行数
            total_rows = lazy_frame_partition.select(pl.len()).collect().item()
            chunk_size = 5_000_000  # 每块 500 万行
            num_chunks = (total_rows + chunk_size - 1) // chunk_size

            self.logger.info(f"[Load][{behavior_type}] 分块处理：{num_chunks} 块，每块约 {chunk_size:,} 行")

            for chunk_index in range(num_chunks):
                offset = chunk_index * chunk_size
                self.logger.info(f"[Load][{behavior_type}] 处理第 {chunk_index+1}/{num_chunks} 块（offset={offset:,}）...")

                # 读取一块数据
                lazy_frame_chunk = lazy_frame_partition.slice(offset, chunk_size)

                # 清洗
                lazy_frame_cleaned = self._clean_partition(lazy_frame_chunk, behavior_type)

                # 写入临时文件
                chunk_path = os.path.join(temp_dir, f"chunk_{chunk_index}.parquet")
                lazy_frame_cleaned.sink_parquet(chunk_path)
                chunk_paths.append(chunk_path)

            # 合并所有块（直接合并，不执行跨块操作）
            self.logger.info(f"[Load][{behavior_type}] 合并 {num_chunks} 个块...")
            self.logger.info(f"[Load][{behavior_type}] 提示：由于内存限制，跨块去重、异常用户过滤和 session_id 生成在块内已执行")
            self.logger.info(f"[Load][{behavior_type}] 提示：跨块重复数据可在后续分析阶段处理")

            # 直接合并所有块
            lazy_frame_final = pl.scan_parquet(chunk_paths)

            # 直接写入最终文件
            lazy_frame_final.sink_parquet(out_path)

            # 返回清洗后的行数
            row_count = pl.scan_parquet(out_path).select(pl.len()).collect().item()
            return row_count
