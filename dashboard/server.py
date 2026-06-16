from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd
import os
import sys
import logging
import jieba
from collections import Counter

# ── 日志配置 ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("dashboard")

# ── 系统状态追踪 ──────────────────────────────────────────
_system_status = {
    "llm_active": False,
    "reason": "API_KEY_MISSING",
    "data_source": "online_shopping_10_cats.csv",
    "data_rows": 0
}

# ── API Key 检测 ──────────────────────────────────────────
LLM_API_KEYS = [
    "SILICONFLOW_API_KEY",
    "DASHSCOPE_API_KEY",
    "OPENAI_API_KEY",
    "DEEPSEEK_API_KEY",
]

_llm_configured = False
for key_name in LLM_API_KEYS:
    if os.environ.get(key_name):
        _llm_configured = True
        _system_status["llm_active"] = True
        _system_status["reason"] = "OK"
        logger.info("检测到 LLM API Key: %s (已配置)", key_name)
        break

if not _llm_configured:
    logger.warning("[WARN] 未检测到任何大模型 API Key 环境变量 (%s)", ", ".join(LLM_API_KEYS))
    logger.warning("[WARN] 系统将以降级模式运行：LLM 功能不可用，使用内置规则库替代")
    logger.warning("[WARN] 如需启用完整 LLM 功能，请设置环境变量，例如：")
    logger.warning("[WARN]   Windows: set SILICONFLOW_API_KEY=your-key-here")
    logger.warning("[WARN]   Linux/Mac: export SILICONFLOW_API_KEY=your-key-here")

# ── 数据加载层 ──────────────────────────────────────────────
# 注意：batch_1000_features.csv（label 全为 1、品类单一）和
# balanced_features_1000.csv（正面全为"书籍"、其他品类全负面）
# 已在 Week 13 实验中确认存在结构性偏差并彻底废弃，此处不再作为备选。
# 若主数据文件缺失，使用空 DataFrame 兜底并通过 /api/system-status 透传状态。
FALLBACK_PATHS = [
    "../online_shopping_10_cats.csv",
]

df = None
loaded_path = None

for path in FALLBACK_PATHS:
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if "label" in df.columns and "sentiment" not in df.columns:
            df["sentiment"] = df["label"].map({1: "正面", 0: "负面"})
        if "sentiment" not in df.columns:
            df["sentiment"] = "中性"
        loaded_path = path
        break

if df is None:
    logger.error("[ERROR] 未找到主数据文件: %s", FALLBACK_PATHS[0])
    logger.error("[ERROR] 使用空 DataFrame 作为兜底，所有 API 将返回空数据")
    logger.error("[ERROR] 请将 online_shopping_10_cats.csv 放置于项目根目录")
    df = pd.DataFrame(columns=["cat", "review", "sentiment", "label"])
    loaded_path = "(空数据集 — 无可用数据文件)"

_system_status["data_rows"] = int(len(df))
_system_status["data_path"] = loaded_path

logger.info("已加载数据: %s (%d 条, %d 个品类)", loaded_path, len(df), df["cat"].nunique() if "cat" in df.columns else 0)
logger.info("情感分布: %s", df["sentiment"].value_counts().to_dict() if "sentiment" in df.columns else {})

# ── 子类别关键词映射（用于下钻）──────────────────────────────
SUB_CATEGORY_KEYWORDS = {
    "物流": ["快递", "发货", "物流", "送货", "配送", "收到", "速度", "慢", "快", "等了", "到货", "签收"],
    "质量": ["质量", "坏了", "差劲", "结实", "耐用", "好用", "不好用", "太差", "很差", "还不错",
             "不行", "可以", "不错", "烂", "破", "次品", "假货", "正品", "真品", "劣质"],
    "价格": ["价格", "便宜", "贵", "划算", "性价比", "不值", "值得", "价钱", "降价", "优惠",
             "活动", "打折", "太贵", "实惠"],
    "服务": ["服务", "客服", "态度", "回复", "售后", "退换", "退货", "退款", "换货", "处理",
             "解决", "投诉", "差评"],
    "包装": ["包装", "包裹", "盒子", "袋子", "完好", "破损", "损坏", "压坏", "完好无损", "拆开"],
    "描述": ["描述", "相符", "不符", "图片", "实物", "色差", "尺寸", "大小", "颜色", "样子",
             "差别", "差距", "一致", "不一致"],
    "使用体验": ["使用", "体验", "效果", "功能", "操作", "方便", "简单", "好用", "实用", "舒适",
                 "手感", "感觉", "感受"],
}

# ── 停用词表（词云过滤）──────────────────────────────────────
STOPWORDS = set(["的", "了", "是", "我", "不", "在", "也", "就", "都", "有",
                  "和", "很", "要", "会", "这", "那", "还", "没", "一", "个",
                  "用", "好", "看", "上", "下", "去", "来", "说", "到", "着",
                  "你", "他", "她", "它", "们", "吗", "啊", "吧", "呢", "哦",
                  "买", "太", "让", "给", "被", "把", "对", "从", "能", "可以",
                  "这个", "那个", "什么", "怎么", "为什么", "因为", "所以",
                  "为了", "但是", "而且", "如果", "虽然", "不过", "还是",
                  "觉得", "知道", "应该", "已经", "真的", "比较", "非常"])

# ── 正则搜索辅助函数 ───────────────────────────────────────
def _apply_regex_search(filtered: pd.DataFrame, query: str) -> pd.DataFrame:
    """对 review 列执行正则搜索，捕获非法正则时降级为普通字符串匹配"""
    try:
        return filtered[filtered["review"].str.contains(query, case=False, na=False, regex=True)]
    except Exception:
        return filtered[filtered["review"].str.contains(query, case=False, na=False, regex=False)]

# ── FastAPI 应用 ────────────────────────────────────────────
app = FastAPI(title="大数据分析看板 API · Week 14")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API A: 健康检查 ─────────────────────────────────────────
@app.get("/api/health")
def health_check():
    return {"status": "ok", "message": "服务运行正常"}

# ── API G: 系统状态（Week 14 新增：降级通知）─────────────────
@app.get("/api/system-status")
def get_system_status():
    return {
        "status": "ok",
        "llm_active": _system_status["llm_active"],
        "reason": _system_status["reason"],
        "data_source": os.path.basename(str(loaded_path)),
        "data_rows": _system_status["data_rows"],
        "degraded": not _system_status["llm_active"],
        "degradation_message": (
            "大模型功能已降级为内置规则库计算，请配置 API Key 以启用完整功能"
            if not _system_status["llm_active"] else None
        )
    }

# ── API B: 品类分布统计（支持情感过滤）──────────────────────
@app.get("/api/category-distribution")
def get_category_distribution(sentiment: str = Query(None)):
    filtered = df if sentiment is None or sentiment == "" else df[df["sentiment"] == sentiment]
    stats = filtered["cat"].value_counts()
    return {
        "categories": stats.index.tolist(),
        "counts": stats.values.tolist()
    }

# ── API C: 情感分析概览（支持品类 + 搜索过滤）────────────────
@app.get("/api/sentiment-overview")
def get_sentiment_overview(cat: str = Query(None), query: str = Query(None)):
    filtered = df
    if cat is not None and cat != "":
        filtered = filtered[filtered["cat"] == cat]
    if query is not None and query != "":
        filtered = _apply_regex_search(filtered, query)
    pivot = filtered.groupby(["cat", "sentiment"]).size().unstack(fill_value=0)
    result = []
    for cat_name in pivot.index:
        row = {"category": cat_name}
        for col in pivot.columns:
            row[col] = int(pivot.loc[cat_name, col])
        result.append(row)
    return {"data": result}

# ── API D: 评论筛选（支持多维度过滤 + 正则搜索 + 分页）───────
@app.get("/api/reviews")
def get_reviews(
    cat: str = Query(None),
    sentiment: str = Query(None),
    query: str = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100)
):
    filtered = df
    if cat is not None and cat != "":
        filtered = filtered[filtered["cat"] == cat]
    if sentiment is not None and sentiment != "":
        filtered = filtered[filtered["sentiment"] == sentiment]
    if query is not None and query != "":
        filtered = _apply_regex_search(filtered, query)
    total = int(len(filtered))
    records = filtered.iloc[offset:offset + limit][["cat", "review", "sentiment", "label"]].to_dict(orient="records")
    return {"total": total, "offset": offset, "limit": limit, "data": records}

# ── API E: 词云词频统计 ─────────────────────────────────────
@app.get("/api/word-frequency")
def get_word_frequency(
    cat: str = Query(None),
    sentiment: str = Query(None),
    query: str = Query(None),
    top_n: int = Query(100, ge=10, le=500)
):
    filtered = df
    if cat is not None and cat != "":
        filtered = filtered[filtered["cat"] == cat]
    if sentiment is not None and sentiment != "":
        filtered = filtered[filtered["sentiment"] == sentiment]
    if query is not None and query != "":
        filtered = _apply_regex_search(filtered, query)

    all_text = " ".join(filtered["review"].dropna().tolist())
    words = jieba.lcut(all_text)
    filtered_words = [w.strip() for w in words
                      if len(w.strip()) >= 2
                      and w.strip() not in STOPWORDS
                      and not w.strip().isdigit()
                      and not all(c in "，。！？、；：""''（）【】 " for c in w.strip())]
    counter = Counter(filtered_words)
    top_words = counter.most_common(top_n)
    return {"data": [{"name": word, "value": count} for word, count in top_words]}

# ── API F: 子维度下钻统计 ───────────────────────────────────
@app.get("/api/sub-category-stats")
def get_sub_category_stats(
    cat: str = Query(None),
    sentiment: str = Query(None),
    query: str = Query(None)
):
    filtered = df
    if cat is not None and cat != "":
        filtered = filtered[filtered["cat"] == cat]
    if sentiment is not None and sentiment != "":
        filtered = filtered[filtered["sentiment"] == sentiment]
    if query is not None and query != "":
        filtered = _apply_regex_search(filtered, query)

    reviews = filtered["review"].fillna("")
    result = {}
    for sub_cat, keywords in SUB_CATEGORY_KEYWORDS.items():
        pattern = "|".join(keywords)
        count = int(reviews.str.contains(pattern, case=False, na=False, regex=True).sum())
        if count > 0:
            result[sub_cat] = count

    sorted_result = dict(sorted(result.items(), key=lambda x: x[1], reverse=True))
    return {
        "cat": cat,
        "sub_categories": list(sorted_result.keys()),
        "counts": list(sorted_result.values())
    }

# ── 静态文件挂载（必须写在所有路由最后）────────────────────
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
