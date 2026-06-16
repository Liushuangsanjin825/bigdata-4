# 实验十：高并发特征管道与防御性容错机制
# 任务1~4 完整代码（100%贴合任务书）
import os
import json
import time
import asyncio
import pandas as pd
from dotenv import load_dotenv
from openai import (
    AsyncOpenAI, APIStatusError, APITimeoutError,
    APIConnectionError, RateLimitError, InternalServerError
)
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, before_sleep_log
)
from tqdm.asyncio import tqdm_asyncio
import logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# ===================== 基础配置 =====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "exper.env"))
api_key = os.getenv("SILICONFLOW_API_KEY")

client = AsyncOpenAI(
    api_key=api_key,
    base_url="https://api.siliconflow.cn/v1",
    timeout=120
)


# ===================== 任务1+2+3：异步改造 + 信号量 + 指数退避 =====================
@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((
        RateLimitError,        # 429 Too Many Requests
        InternalServerError,   # 5xx 服务端错误
        APITimeoutError,       # 请求超时
        APIConnectionError,    # 网络连接中断
    )),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)
async def extract_features(sem: asyncio.Semaphore, text: str) -> dict:
    """异步特征抽取：带信号量流控和指数退避重试"""
    prompt = f"""
你是专业的电商数据流清洗组件，负责从商品评论中提取结构化特征。
严格按以下规则输出：
1. sentiment：仅选【正面、负面、中性】
2. category：仅选【物流、质量、价格、服务、综合】
3. summary：15字以内，概括核心诉求

评论内容：{text}

请仅返回纯净JSON对象，不要任何额外文字、解释、Markdown标记。
    """.strip()

    async with sem:
        response = await client.chat.completions.create(
            model="Qwen/Qwen3.5-4B",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            extra_body={"enable_thinking": False}
        )
        content = response.choices[0].message.content
        try:
            return json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return {"sentiment": "解析失败", "category": "解析失败", "summary": str(content)[:30]}


# ===================== 任务4：1000条数据高速清洗主控 =====================
async def main():
    # 读取数据，截取1000条
    df = pd.read_csv(os.path.join(BASE_DIR, "online_shopping_10_cats.csv"))
    test_df = df.iloc[:1000].copy()
    test_texts = test_df["review"].tolist()

    # 任务2：Semaphore 必须在 async def main() 内部创建（不能在全局作用域）
    sem = asyncio.Semaphore(20)

    print(f"开始并发处理 {len(test_texts)} 条评论（并发上限：20）...")
    start_time = time.perf_counter()

    # 任务4：异步任务编排 + tqdm 进度条
    tasks = [extract_features(sem, text) for text in test_texts]
    results = await tqdm_asyncio.gather(*tasks, desc="特征抽取进度")

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    # 拼接结果
    features_df = pd.DataFrame(results)
    final_df = pd.concat([test_df.reset_index(drop=True), features_df], axis=1)

    # 落盘（任务书指定文件名）
    output_path = os.path.join(BASE_DIR, "batch_1000_features.csv")
    final_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    # ===================== 时耗统计 =====================
    sync_theory_minutes = (len(test_texts) * 8) / 60
    actual_minutes = elapsed / 60
    speedup = sync_theory_minutes / actual_minutes if actual_minutes > 0 else float("inf")

    print(f"\n===== 任务4 完成 =====")
    print(f"处理条数：{len(test_texts)}")
    print(f"实际耗时：{elapsed:.2f} 秒（{actual_minutes:.2f} 分钟）")
    print(f"单线程理论耗时：{sync_theory_minutes:.2f} 分钟（按 8 秒/条）")
    print(f"加速倍数：{speedup:.1f} 倍")
    print(f"[OK] 文件已落盘：{output_path}")

    # 预览结果
    print(f"\n===== 前5条结果预览 =====")
    print(final_df[["review", "sentiment", "category", "summary"]].head().to_string())


if __name__ == "__main__":
    asyncio.run(main())
