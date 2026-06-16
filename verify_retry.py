# 任务3 验证脚本：调高并发至100触发429限流，验证Tenacity指数退避重试
import os
import json
import time
import asyncio
import logging
import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI, APITimeoutError, APIConnectionError, RateLimitError, InternalServerError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from tqdm.asyncio import tqdm_asyncio

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "exper.env"))

client = AsyncOpenAI(
    api_key=os.getenv("SILICONFLOW_API_KEY"),
    base_url="https://api.siliconflow.cn/v1",
    timeout=120
)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((
        RateLimitError,
        InternalServerError,
        APITimeoutError,
        APIConnectionError,
    )),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)
async def extract_features(sem: asyncio.Semaphore, text: str) -> dict:
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


async def main():
    df = pd.read_csv(os.path.join(BASE_DIR, "online_shopping_10_cats.csv"))
    test_df = df.iloc[:200].copy()  # 用200条验证即可，避免浪费
    test_texts = test_df["review"].tolist()

    # 任务3 验证：临时调高至100并发
    sem = asyncio.Semaphore(100)

    print(f"===== 重试机制验证 =====")
    print(f"并发上限：100（正常为20，此处为验证429限流临时调高）")
    print(f"数据量：{len(test_texts)} 条")
    print(f"预期：高并发将触发429限流，Tenacity自动指数退避重试")
    print()

    start_time = time.perf_counter()
    tasks = [extract_features(sem, text) for text in test_texts]
    results = await tqdm_asyncio.gather(*tasks, desc="验证进度")
    elapsed = time.perf_counter() - start_time

    success_count = sum(1 for r in results if r.get("sentiment") != "解析失败")
    fail_count = len(results) - success_count

    print(f"\n===== 验证结果 =====")
    print(f"总耗时：{elapsed:.1f} 秒")
    print(f"成功：{success_count} 条")
    print(f"解析失败：{fail_count} 条")
    print(f"验证完成 —— 若终端上方出现 WARNING 级别的 'Retrying...' 日志，则说明重试机制已生效")


if __name__ == "__main__":
    asyncio.run(main())
