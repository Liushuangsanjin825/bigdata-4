# 任务3~6 完整代码（100%贴合任务书 · 文件名严格合规）
import os
import json
import time
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

# ===================== 基础配置 =====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "exper.env"))
api_key = os.getenv("SILICONFLOW_API_KEY")

client = OpenAI(
    api_key=api_key,
    base_url="https://api.siliconflow.cn/v1",
    timeout=60
)

# ===================== 任务3：Prompt 设计 =====================
def extract_features(text: str) -> dict:
    prompt_template = """
你是专业的电商数据流清洗组件，负责从商品评论中提取结构化特征。
严格按以下规则输出：
1. sentiment：仅选【正面、负面、中性】
2. category：仅选【物流、质量、价格、服务、综合】
3. summary：15字以内，概括核心诉求

评论内容：{text}

请仅返回纯净JSON对象，不要任何额外文字、解释、Markdown标记。
    """.strip()

    prompt = prompt_template.format(text=text)
    try:
        response = client.chat.completions.create(
            model="deepseek-ai/DeepSeek-V4-Flash",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)
    except:
        return {"sentiment": "解析失败", "category": "解析失败", "summary": "解析失败"}

# ===================== 任务4+5：批量处理 + 官方指定文件名落盘 =====================
if __name__ == "__main__":
    # 读取数据
    df = pd.read_csv(os.path.join(BASE_DIR, "online_shopping_10_cats.csv"))
    test_df = df.iloc[100:105].copy()

    # 批量提取
    results = []
    start_time = time.perf_counter()

    for text in test_df["review"]:
        features = extract_features(text)
        results.append(features)

    end_time = time.perf_counter()
    features_df = pd.DataFrame(results)

    # 拼接数据
    final_df = pd.concat([test_df.reset_index(drop=True), features_df], axis=1)

    # ===================== 任务5：严格按任务书落盘 =====================
    # ✅ 官方指定文件名：augmented_reviews_sample.csv
    final_df.to_csv(
        os.path.join(BASE_DIR, "augmented_reviews_sample.csv"),
        index=False,
        encoding="utf-8-sig"
    )

    # 输出结果
    print("===== 结构化特征提取结果 =====")
    print(features_df)
    print(f"\n处理耗时：{end_time - start_time:.2f} 秒")
    print("\n===== 任务5 完成 =====")
    print("✅ 文件已落盘：augmented_reviews_sample.csv")