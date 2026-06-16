"""
上传数据集到 GitHub Release v2.0
"""
import os
import requests
import json
from pathlib import Path

# 配置
REPO = "Liushuangsanjin825/bigdata-4"
TAG = "v2.0"
RELEASE_NAME = "M1 Final Clean Dataset v2.0"
RELEASE_NOTES = """## M1 数据清洗 Pipeline v2.0

### 修复内容
- 使用 streaming 引擎避免内存溢出 (collect(engine='streaming'))
- 新增 _identify_suspect_users() 识别刷号用户
- 新增 _filter_violators() 过滤违规用户
- 用 group_by+agg(first) 替代 unique() 支持 streaming
- 所有分区统一剔除刷号用户的所有行为记录
- 数据校验 10/10 通过

### 数据质量
- ✅ 99,030,577 行清洗后数据
- ✅ 1.01 GB 文件大小
- ✅ 0 个违规用户 (buy_count <= pv_count)
- ✅ 0 个重复行（抽样验证）
- ✅ 所有必要字段完整

### 使用方法
```bash
python run_m1_pipeline.py
python m1_tester.py m1_final_clean.parquet
```
"""

# 从环境变量或配置文件读取 token
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")

if not GITHUB_TOKEN:
    print("❌ 未找到 GITHUB_TOKEN")
    print("请设置环境变量: set GITHUB_TOKEN=your_token_here")
    print("或者在 https://github.com/settings/tokens 创建 Personal Access Token")
    exit(1)

BASE_DIR = Path(__file__).parent
FILES_TO_UPLOAD = [
    BASE_DIR / "m1_final_clean.parquet",
    BASE_DIR / "m1_final_clean.zip",
]

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

print("=" * 60)
print("上传数据集到 GitHub Release v2.0")
print("=" * 60)

# 步骤1: 创建 Release
print("\n[1/3] 创建 Release v2.0...")
release_url = f"https://api.github.com/repos/{REPO}/releases"

release_data = {
    "tag_name": TAG,
    "name": RELEASE_NAME,
    "body": RELEASE_NOTES,
    "draft": False,
    "prerelease": False,
    "generate_release_notes": False
}

response = requests.post(release_url, headers=headers, json=release_data)

if response.status_code == 201:
    release_info = response.json()
    upload_url = release_info["upload_url"].replace("{?name,label}", "")
    release_id = release_info["id"]
    print(f"✅ Release v2.0 创建成功 (ID: {release_id})")
elif response.status_code == 422:
    # Release 已存在
    print("⚠️  Release v2.0 已存在，尝试获取...")
    # 查找已有的 release
    response = requests.get(release_url, headers=headers)
    releases = response.json()
    existing = next((r for r in releases if r["tag_name"] == TAG), None)
    if existing:
        upload_url = existing["upload_url"].replace("{?name,label}", "")
        release_id = existing["id"]
        print(f"✅ 使用已有 Release v2.0 (ID: {release_id})")
    else:
        print(f"❌ 创建 Release 失败: {response.status_code}")
        print(response.text)
        exit(1)
else:
    print(f"❌ 创建 Release 失败: {response.status_code}")
    print(response.text)
    exit(1)

# 步骤2: 上传文件
print("\n[2/3] 上传数据集文件...")
for file_path in FILES_TO_UPLOAD:
    if not file_path.exists():
        print(f"⚠️  文件不存在，跳过: {file_path}")
        continue
    
    file_size = file_path.stat().st_size / (1024**3)  # GB
    print(f"\n  上传: {file_path.name} ({file_size:.2f} GB)")
    
    upload_asset_url = f"{upload_url}?name={file_path.name}"
    upload_headers = headers.copy()
    upload_headers["Content-Type"] = "application/octet-stream"
    
    try:
        with open(file_path, "rb") as f:
            response = requests.post(
                upload_asset_url,
                headers=upload_headers,
                data=f,
                timeout=600  # 10分钟超时
            )
        
        if response.status_code in [200, 201]:
            print(f"  ✅ {file_path.name} 上传成功")
        else:
            print(f"  ❌ {file_path.name} 上传失败: {response.status_code}")
            print(f"  错误信息: {response.text[:200]}")
    except Exception as e:
        print(f"  ❌ {file_path.name} 上传异常: {e}")

# 步骤3: 验证
print("\n[3/3] 验证上传结果...")
verify_url = f"https://api.github.com/repos/{REPO}/releases/{release_id}"
response = requests.get(verify_url, headers=headers)

if response.status_code == 200:
    release_info = response.json()
    print(f"\n✅ Release 验证成功!")
    print(f"  标签: {release_info['tag_name']}")
    print(f"  名称: {release_info['name']}")
    print(f"  文件数: {len(release_info.get('assets', []))}")
    print(f"\n  已上传文件:")
    for asset in release_info.get("assets", []):
        size_mb = asset["size"] / (1024**2)
        print(f"    - {asset['name']} ({size_mb:.2f} MB)")
    
    print(f"\n🔗 Release 链接: {release_info['html_url']}")
else:
    print(f"❌ 验证失败: {response.status_code}")

print("\n" + "=" * 60)
print("上传完成！")
print("=" * 60)
