# 任务2 标准协议接入与连通性验证（100%贴合实验任务书）
import os
from openai import OpenAI
from dotenv import load_dotenv

# ============== 任务书要求：环境配置 ==============
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "exper.env"))
api_key = os.getenv("SILICONFLOW_API_KEY")

# 调试打印（看是否读到密钥）
print("【调试】API密钥读取成功：", api_key is not None)
print("【调试】开始请求API...")

# ============== 任务书要求：客户端初始化（仅加超时防卡死） ==============
try:
    client = OpenAI(
        api_key=api_key,
        base_url="https://api.siliconflow.cn/v1",
        timeout=60  # 仅加超时，不修改任务书核心
    )

    # ============== 任务书原文：模型+请求内容 完全不动 ==============
    response = client.chat.completions.create(
        model="deepseek-ai/DeepSeek-V4-Flash",
        messages=[{"role": "user", "content": "你好，请回复测试成功。"}]
    )

    # ============== 任务书要求：输出结果 ==============
    print("\n=== API 完整响应 ===")
    print(response)
    print("\n=== 测试结果 ===")
    print(response.choices[0].message.content)

# 捕获所有错误，告诉你为什么无响应！
except Exception as e:
    print("\n【❌ API请求失败，原因：】")
    print(f"错误类型：{type(e).__name__}")
    print(f"错误信息：{str(e)}")