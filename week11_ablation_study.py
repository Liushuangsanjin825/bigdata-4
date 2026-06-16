# 实验十一：异构特征融合与模型消融实验闭环
# 任务1~4 完整代码（100%贴合任务书）
import os
import json
import time
import asyncio
import logging
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from openai import (
    AsyncOpenAI, APITimeoutError, APIConnectionError,
    RateLimitError, InternalServerError
)
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, before_sleep_log
)
from tqdm.asyncio import tqdm_asyncio

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
from scipy.sparse import hstack, csr_matrix, issparse
import lightgbm as lgb
import shap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# 修复 SHAP 图中文字体缺失问题
_chinese_fonts = [f for f in fm.fontManager.ttflist if 'Microsoft YaHei' in f.name]
if _chinese_fonts:
    plt.rcParams['font.family'] = _chinese_fonts[0].name
plt.rcParams['axes.unicode_minus'] = False

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "exper.env"))

client = AsyncOpenAI(
    api_key=os.getenv("SILICONFLOW_API_KEY"),
    base_url="https://api.siliconflow.cn/v1",
    timeout=90
)

# ==================== 步骤一：构建平衡数据集 ====================
# 核心思路：batch_1000_features.csv 的 1000 条是 Week 10 用 Qwen3.5-4B 提取的，
# 但全部为 label=1（正面）。Week 11 分类任务必须有负样本。
# 修复方法：复用 batch_1000_features.csv 的正面样本 + 从原始 CSV 补充负样本并跑 LLM 提取。

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((
        RateLimitError, InternalServerError, APITimeoutError, APIConnectionError,
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
            model="deepseek-ai/DeepSeek-V4-Flash",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
        )
        content = response.choices[0].message.content
        try:
            return json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return {"sentiment": "解析失败", "category": "解析失败", "summary": str(content)[:30]}


async def extract_features_batch(df_input):
    """批量提取 LLM 特征"""
    texts = df_input["review"].tolist()
    sem = asyncio.Semaphore(20)
    print(f"[LLM提取] 开始并发处理 {len(texts)} 条评论（并发上限: 20）...")
    start = time.perf_counter()
    tasks = [extract_features(sem, text) for text in texts]
    results = await tqdm_asyncio.gather(*tasks, desc="LLM特征提取")
    elapsed = time.perf_counter() - start
    print(f"[LLM提取] 完成，耗时 {elapsed:.1f} 秒（{elapsed/60:.2f} 分钟）")

    features_df = pd.DataFrame(results)
    final_df = pd.concat([df_input.reset_index(drop=True), features_df], axis=1)
    return final_df


async def build_balanced_dataset(n_neg=500):
    """
    构建平衡数据集：
    1. 从 batch_1000_features.csv 取正面样本（复用 Week 10 成果）
    2. 从原始 CSV 采样负样本并调用 LLM 提取特征
    3. 合并为平衡数据集
    """
    # 步骤1：加载 Week 10 已有正面样本
    batch_path = os.path.join(BASE_DIR, "batch_1000_features.csv")
    pos_df = pd.read_csv(batch_path)
    # 只保留需要的列（sentence、analysis 是空列，丢弃）
    pos_df = pos_df[["cat", "label", "review", "sentiment", "category", "summary"]].copy()
    pos_df = pos_df.sample(n=n_neg, random_state=42)  # 取与负样本等量

    print(f"[数据准备] 正面样本（来自 batch_1000_features.csv）: {len(pos_df)} 条")

    # 步骤2：从原始 CSV 采样负样本（排除前1000条已被 Week 10 使用的）
    df_full = pd.read_csv(os.path.join(BASE_DIR, "online_shopping_10_cats.csv"))
    neg_pool = df_full.iloc[1000:]
    neg_raw = neg_pool[neg_pool["label"] == 0].sample(n=n_neg, random_state=42)
    # 只保留 cat, label, review 三列，等 LLM 提取后再拼接
    neg_raw = neg_raw[["cat", "label", "review"]].reset_index(drop=True)

    print(f"[数据准备] 负样本（待LLM提取）: {len(neg_raw)} 条")

    # 步骤3：对负样本进行 LLM 特征提取
    neg_df = await extract_features_batch(neg_raw)

    # 步骤4：合并并打乱
    balanced = pd.concat([pos_df, neg_df], ignore_index=True)
    balanced = balanced.sample(frac=1, random_state=42).reset_index(drop=True)

    print(f"[数据准备] 平衡数据集构建完成: {len(balanced)} 条")
    print(f"  标签分布:\n{balanced['label'].value_counts().to_string()}")
    return balanced


# ==================== 任务1：TF-IDF 传统NLP基线 ====================
def task1_tfidf_baseline(df):
    print("\n===== 任务1：构建 TF-IDF 传统NLP基线 =====")
    tfidf = TfidfVectorizer(analyzer='char', max_features=500)
    X_text_sparse = tfidf.fit_transform(df['review'].fillna(''))
    print(f"[任务1] TF-IDF 矩阵维度: {X_text_sparse.shape}")
    return X_text_sparse, tfidf


# ==================== 任务2：稀疏-稠密特征融合 ====================
def task2_sparse_dense_fusion(df, X_text_sparse):
    print("\n===== 任务2：稀疏-稠密特征融合 (Sparse-Dense Fusion) =====")
    # llm_cols 根据实际 LLM 提取列名设定（替代任务书示例的 issue_type）
    llm_cols = ["cat", "sentiment", "category"]
    df[llm_cols] = df[llm_cols].fillna("Unknown")

    encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)
    X_dense = encoder.fit_transform(df[llm_cols])
    print(f"[任务2] 稠密特征维度: {X_dense.shape}")

    X_fused = hstack([X_text_sparse, csr_matrix(X_dense)])
    y = df["label"].values.astype(int)

    expected_cols = 500 + X_dense.shape[1]
    print(f"[任务2] 融合后特征维度: {X_fused.shape}")
    print(f"[任务2] 矩阵拼接验证: 列数={X_fused.shape[1]}, 期望={expected_cols} "
          f"(500 TF-IDF + {X_dense.shape[1]} 稠密特征)")
    assert X_fused.shape[1] == expected_cols, \
        f"拼接维度错误! 期望{expected_cols}, 实际{X_fused.shape[1]}"
    return X_fused, X_dense, y, encoder, llm_cols


# ==================== 任务3：LightGBM 消融实验 ====================
def task3_ablation_study(X_text_sparse, X_dense, X_fused, y):
    print("\n===== 任务3：LightGBM 消融实验 (Ablation Study) =====")

    def train_eval(X, y):
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        clf = lgb.LGBMClassifier(n_estimators=100, random_state=42, verbose=-1)
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        y_proba = clf.predict_proba(X_test)[:, 1]
        acc = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_proba)
        return clf, X_test, y_test, acc, auc

    # Baseline A: 纯 TF-IDF（传统 NLP）
    clf_a, X_test_a, y_test_a, acc_a, auc_a = train_eval(X_text_sparse, y)
    print(f"  Baseline A (纯TF-IDF)         → Accuracy: {acc_a:.4f}, AUC: {auc_a:.4f}")

    # Baseline B: 纯 LLM 特征
    clf_b, X_test_b, y_test_b, acc_b, auc_b = train_eval(X_dense, y)
    print(f"  Baseline B (纯LLM特征)        → Accuracy: {acc_b:.4f}, AUC: {auc_b:.4f}")

    # Fused C: 异构融合
    clf_c, X_test_c, y_test_c, acc_c, auc_c = train_eval(X_fused, y)
    print(f"  Fused C   (异构融合)          → Accuracy: {acc_c:.4f}, AUC: {auc_c:.4f}")

    # 消融对照表
    print("\n" + "=" * 72)
    print("消融实验对照表 (Ablation Study Results)")
    print("=" * 72)
    print(f"{'实验组':<32} {'特征维度':<10} {'Accuracy':<10} {'AUC':<10}")
    print("-" * 72)
    print(f"{'Baseline A (纯TF-IDF, 传统NLP)':<32} {X_text_sparse.shape[1]:<10} {acc_a:<10.4f} {auc_a:<10.4f}")
    print(f"{'Baseline B (纯LLM特征)':<32} {X_dense.shape[1]:<10} {acc_b:<10.4f} {auc_b:<10.4f}")
    print(f"{'Fused C (异构融合)':<32} {X_fused.shape[1]:<10} {acc_c:<10.4f} {auc_c:<10.4f}")
    print("=" * 72)

    acc_lift = acc_c - acc_a
    auc_lift = auc_c - auc_a
    print(f"\nLLM 特征增量贡献: ΔAccuracy = {acc_lift:+.4f}, ΔAUC = {auc_lift:+.4f}")
    print(f"结论: LLM 语义特征为传统 TF-IDF 基线带来了 {auc_lift:+.4f} 的 AUC 提升")

    return clf_c, X_test_c, y_test_c, (acc_a, auc_a, acc_b, auc_b, acc_c, auc_c)


# ==================== 任务4：SHAP 特征贡献审计 ====================
def task4_shap_audit(clf_c, X_test_c, y_test_c, tfidf, llm_cols):
    print("\n===== 任务4：SHAP 特征贡献审计 =====")

    # 取测试集子集加速 SHAP 计算
    n_shap = min(100, X_test_c.shape[0])
    X_sample = X_test_c[:n_shap]

    explainer = shap.TreeExplainer(clf_c)
    shap_values = explainer.shap_values(X_sample)
    if issparse(shap_values):
        shap_values = shap_values.toarray()

    # 构建特征名列表（前 500 列 TF-IDF + 后面 LLM 特征）
    tfidf_names = [f"字_{c}" for c in tfidf.get_feature_names_out()]
    feature_names = tfidf_names + llm_cols

    # ---------- 摘要图 ----------
    plt.figure(figsize=(10, 8))
    X_dense_for_shap = X_sample.toarray() if issparse(X_sample) else X_sample
    shap.summary_plot(shap_values, X_dense_for_shap, feature_names=feature_names,
                      show=False, max_display=20)
    plt.tight_layout()
    summary_path = os.path.join(BASE_DIR, "shap_summary.png")
    plt.savefig(summary_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[任务4] SHAP 摘要图已保存: shap_summary.png")

    # ---------- 瀑布图（第0条样本） ----------
    row_data = (np.asarray(X_sample[0].todense()).flatten()
                if issparse(X_sample) else X_sample[0])

    # LightGBM 二分类: shap_values 是 list[array, array], expected_value 是 list[float, float]
    if isinstance(shap_values, list):
        sv = shap_values[1]  # 取正类（label=1）的 SHAP 值
        ev = float(explainer.expected_value[1])
    else:
        sv = shap_values
        ev = float(explainer.expected_value)

    explanation = shap.Explanation(
        values=sv[0].flatten(),
        base_values=ev,
        data=row_data,
        feature_names=feature_names
    )

    plt.figure(figsize=(10, 6))
    shap.plots.waterfall(explanation, max_display=15, show=False)
    plt.tight_layout()
    waterfall_path = os.path.join(BASE_DIR, "shap_waterfall.png")
    plt.savefig(waterfall_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[任务4] SHAP 瀑布图已保存: shap_waterfall.png")

    # ---------- 特征重要性排名 ----------
    importance_df = pd.DataFrame({
        "feature": feature_names,
        "mean_abs_shap": np.abs(sv).mean(axis=0)
    }).sort_values("mean_abs_shap", ascending=False)

    print("\nTop 15 特征重要性 (按 |SHAP| 均值排序):")
    print(importance_df.head(15).to_string(index=False))

    # ---------- LLM vs TF-IDF 贡献对比 ----------
    llm_total = importance_df[importance_df["feature"].isin(llm_cols)]["mean_abs_shap"].sum()
    tfidf_total = importance_df[~importance_df["feature"].isin(llm_cols)]["mean_abs_shap"].sum()
    n_tfidf = len(feature_names) - len(llm_cols)

    print(f"\n特征贡献审计:")
    print(f"  LLM 特征 (n={len(llm_cols)})   总|SHAP|={llm_total:.4f}, "
          f"均|SHAP|={llm_total/len(llm_cols):.4f}")
    print(f"  TF-IDF特征 (n={n_tfidf}) 总|SHAP|={tfidf_total:.4f}, "
          f"均|SHAP|={tfidf_total/n_tfidf:.4f}")
    print(f"  LLM 单特征平均贡献是 TF-IDF 单特征平均的 "
          f"{(llm_total/len(llm_cols))/(tfidf_total/n_tfidf):.1f} 倍")

    return importance_df


# ==================== 主控函数 ====================
async def main():
    total_start = time.perf_counter()

    # ---- 准备平衡数据集 ----
    balanced_path = os.path.join(BASE_DIR, "balanced_features_1000.csv")
    if os.path.exists(balanced_path):
        print(f"[数据] 从缓存加载: {balanced_path}")
        df = pd.read_csv(balanced_path)
    else:
        df = await build_balanced_dataset(n_neg=500)
        df.to_csv(balanced_path, index=False, encoding="utf-8-sig")
        print(f"[数据] 平衡数据集已保存: {balanced_path}")

    print(f"\n[数据统计] 总样本: {len(df)}, 标签分布:\n{df['label'].value_counts().to_string()}")

    # ---- 任务1：TF-IDF 基线 ----
    X_text_sparse, tfidf = task1_tfidf_baseline(df)

    # ---- 任务2：稀疏-稠密融合 ----
    X_fused, X_dense, y, encoder, llm_cols = task2_sparse_dense_fusion(df, X_text_sparse)

    # ---- 任务3：消融实验 ----
    clf_c, X_test_c, y_test_c, scores = task3_ablation_study(
        X_text_sparse, X_dense, X_fused, y
    )

    # ---- 任务4：SHAP 审计 ----
    task4_shap_audit(clf_c, X_test_c, y_test_c, tfidf, llm_cols)

    total_elapsed = time.perf_counter() - total_start
    print(f"\n{'='*50}")
    print(f"实验十一 全部完成!")
    print(f"总耗时: {total_elapsed:.1f} 秒 ({total_elapsed/60:.2f} 分钟)")
    print(f"生成文件: balanced_features_1000.csv, shap_summary.png, shap_waterfall.png")


if __name__ == "__main__":
    asyncio.run(main())
