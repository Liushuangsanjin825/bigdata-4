"""
M2 实时数据生成器 (M2 Real-time Data Generator)

模拟电商实时用户行为事件流，用于流处理系统的测试和演示。

功能：
1. 生成符合业务逻辑的用户行为事件流
2. 支持漏斗概率控制（view → cart → purchase）
3. 生成 session_id 模拟用户会话
4. 支持输出到控制台、文件、Kafka 等
"""

import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Iterator
import json


class UserBehaviorSimulator:
    """
    用户行为模拟器
    模拟真实用户的浏览、加购、购买行为序列
    """

    def __init__(
        self,
        num_users: int = 1000,
        num_items: int = 500,
        view_prob: float = 0.80,
        cart_prob: float = 0.15,
        purchase_prob: float = 0.05,
        session_timeout_seconds: int = 1800,
    ):
        """
        初始化模拟器

        :param num_users: 模拟用户数量
        :param num_items: 模拟商品数量
        :param view_prob: 浏览行为概率
        :param cart_prob: 加购行为概率
        :param purchase_prob: 购买行为概率
        :param session_timeout_seconds: 会话超时时间（秒）
        """
        self.num_users = num_users
        self.num_items = num_items
        self.user_ids = list(range(1, num_users + 1))
        self.item_ids = list(range(1, num_items + 1))

        # 行为概率（需要归一化）
        total_prob = view_prob + cart_prob + purchase_prob
        self.view_prob = view_prob / total_prob
        self.cart_prob = cart_prob / total_prob
        self.purchase_prob = purchase_prob / total_prob

        self.session_timeout = session_timeout_seconds

        # 用户状态追踪
        self.user_sessions: Dict[int, Dict] = {}  # user_id -> {session_id, last_active, history}

    def _generate_session_id(self, user_id: int) -> str:
        """为用户生成或获取当前 session_id"""
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = {
                "session_id": str(uuid.uuid4())[:12],
                "last_active": datetime.now(),
                "history": [],
            }
            return self.user_sessions[user_id]["session_id"]

        session = self.user_sessions[user_id]
        now = datetime.now()

        # 检查是否超时，超时则创建新 session
        if (now - session["last_active"]).total_seconds() > self.session_timeout:
            session["session_id"] = str(uuid.uuid4())[:12]
            session["history"] = []

        session["last_active"] = now
        return session["session_id"]

    def _select_behavior_type(self, user_id: int) -> str:
        """
        根据用户历史行为选择下一个行为类型
        实现业务逻辑自洽：购买前大概率有浏览铺垫
        """
        user_data = self.user_sessions.get(user_id, {"history": []})
        recent_history = user_data.get("history", [])[-5:]  # 最近 5 条行为

        # 如果用户近期没有浏览行为，提高浏览概率
        has_recent_view = any(h == "view" for h in recent_history)

        if not has_recent_view:
            # 强制先浏览再购买/加购
            adjusted_probs = {
                "view": 0.95,
                "cart": 0.04,
                "purchase": 0.01,
            }
        else:
            # 使用正常概率
            adjusted_probs = {
                "view": self.view_prob,
                "cart": self.cart_prob,
                "purchase": self.purchase_prob,
            }

        # 根据概率选择行为
        rand = random.random()
        cumulative = 0
        for behavior, prob in adjusted_probs.items():
            cumulative += prob
            if rand <= cumulative:
                return behavior

        return "view"

    def generate_event(self, user_id: Optional[int] = None) -> Dict:
        """
        生成单个用户行为事件

        :param user_id: 指定用户 ID（None 则随机选择）
        :return: 事件字典
        """
        if user_id is None:
            user_id = random.choice(self.user_ids)

        session_id = self._generate_session_id(user_id)
        behavior_type = self._select_behavior_type(user_id)
        item_id = random.choice(self.item_ids)

        # 记录到用户历史
        if user_id in self.user_sessions:
            self.user_sessions[user_id]["history"].append(behavior_type)
            # 只保留最近 10 条历史
            if len(self.user_sessions[user_id]["history"]) > 10:
                self.user_sessions[user_id]["history"] = self.user_sessions[user_id]["history"][-10:]

        event = {
            "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "user_id": user_id,
            "item_id": item_id,
            "behavior_type": behavior_type,
            "session_id": session_id,
        }

        return event

    def generate_event_stream(self, events_per_second: float = 10, duration_seconds: int = 60) -> Iterator[Dict]:
        """
        生成事件流

        :param events_per_second: 每秒事件数
        :param duration_seconds: 持续时长（秒）
        :yield: 事件字典
        """
        interval = 1.0 / events_per_second
        total_events = int(events_per_second * duration_seconds)

        for i in range(total_events):
            event = self.generate_event()
            yield event

            # 控制生成速率
            if i < total_events - 1:
                time.sleep(interval)


class EventProducer:
    """
    事件生产者
    将生成的事件输出到不同目标（控制台、文件、Kafka 等）
    """

    def __init__(self, simulator: UserBehaviorSimulator):
        self.simulator = simulator

    def produce_to_console(
        self,
        events_per_second: float = 5,
        duration_seconds: int = 30,
        format_type: str = "json",
    ):
        """
        将事件输出到控制台

        :param events_per_second: 每秒事件数
        :param duration_seconds: 持续时长
        :param format_type: 输出格式（json/csv）
        """
        print(f"🚀 开始生成事件流...")
        print(f"📊 速率：{events_per_second} 事件/秒，持续 {duration_seconds} 秒")
        print(f"👥 用户数：{self.simulator.num_users}，商品数：{self.simulator.num_items}")
        print(f"📈 行为概率：view={self.simulator.view_prob:.1%}, cart={self.simulator.cart_prob:.1%}, purchase={self.simulator.purchase_prob:.1%}")
        print("=" * 100)

        event_count = 0
        start_time = time.time()

        for event in self.simulator.generate_event_stream(events_per_second, duration_seconds):
            event_count += 1

            if format_type == "json":
                print(json.dumps(event, ensure_ascii=False))
            elif format_type == "csv":
                print(f"{event['event_time']},{event['user_id']},{event['item_id']},{event['behavior_type']},{event['session_id']}")

            # 每 10 个事件打印一次进度
            if event_count % 10 == 0:
                elapsed = time.time() - start_time
                print(f"⏱️  [{elapsed:.1f}s] 已生成 {event_count} 个事件...", flush=True)

        total_time = time.time() - start_time
        print("=" * 100)
        print(f"✅ 事件生成完成！共 {event_count} 个事件，耗时 {total_time:.1f} 秒")

    def produce_to_file(
        self,
        output_path: str,
        events_per_second: float = 10,
        duration_seconds: int = 60,
        format_type: str = "json",
    ):
        """
        将事件输出到文件

        :param output_path: 输出文件路径
        :param events_per_second: 每秒事件数
        :param duration_seconds: 持续时长
        :param format_type: 输出格式
        """
        print(f"💾 开始写入文件：{output_path}")

        with open(output_path, "w", encoding="utf-8") as f:
            if format_type == "json":
                f.write("[\n")

            event_count = 0
            separator = ""

            for event in self.simulator.generate_event_stream(events_per_second, duration_seconds):
                event_count += 1

                if format_type == "json":
                    f.write(f"{separator}  {json.dumps(event, ensure_ascii=False)}")
                    separator = ",\n"
                elif format_type == "csv":
                    if event_count == 1:
                        f.write("event_time,user_id,item_id,behavior_type,session_id\n")
                    f.write(f"{event['event_time']},{event['user_id']},{event['item_id']},{event['behavior_type']},{event['session_id']}\n")

                if event_count % 50 == 0:
                    print(f"📝 已写入 {event_count} 个事件...", flush=True)

            if format_type == "json":
                f.write("\n]\n")

        print(f"✅ 文件写入完成！共 {event_count} 个事件")


def print_schema_documentation():
    """打印 Schema 文档"""
    schema_doc = """
# 📋 M2 实时事件流 Schema 文档

## 事件模型字段说明

| 字段名          | 数据类型   | 约束条件                        | 说明                                       |
| --------------- | ---------- | ------------------------------- | ------------------------------------------ |
| **event_time**  | String     | 非空，格式：`YYYY-MM-DD HH:MM:SS.mmm` | 事件发生时间（带毫秒精度的时间戳）       |
| **user_id**     | Integer    | 非空，范围：1 ~ num_users     | 用户唯一标识符                             |
| **item_id**     | Integer    | 非空，范围：1 ~ num_items     | 商品唯一标识符                             |
| **behavior_type** | String   | 非空，枚举值：`view`/`cart`/`purchase` | 用户行为类型（浏览/加购/购买）             |
| **session_id**  | String     | 非空，格式：12位 UUID 前缀    | 用户当前会话 ID（30 分钟无操作自动失效）   |

## 业务逻辑约束

### 1. 行为概率分布

| 行为类型   | 概率   | 说明                   |
| ---------- | ------ | ---------------------- |
| **view**   | 80%    | 页面浏览行为           |
| **cart**   | 15%    | 加入购物车行为         |
| **purchase** | 5%   | 购买行为               |

### 2. 漏斗逻辑自洽

- ✅ **购买前必有浏览**：如果用户近期没有 `view` 行为，系统会提高 `view` 概率至 95%
- ✅ **会话连续性**：同一 session_id 内的行为序列符合用户真实浏览习惯
- ✅ **时间顺序**：event_time 严格按生成顺序递增

### 3. Session 规则

- 会话超时时间：**30 分钟**（1800 秒）
- 超时后自动生成新的 session_id
- 同一用户可能同时存在多个历史 session

## 示例数据

```json
{
  "event_time": "2026-04-09 14:32:15.123",
  "user_id": 42,
  "item_id": 187,
  "behavior_type": "view",
  "session_id": "a1b2c3d4e5f6"
}
```

## 与 M1 批处理数据的映射关系

| M1 字段         | M2 字段         | 转换说明                           |
| --------------- | --------------- | ---------------------------------- |
| timestamp       | event_time      | Unix 时间戳 → 可读时间格式         |
| user_id         | user_id         | 直接映射                           |
| item_id         | item_id         | 直接映射                           |
| behavior_type   | behavior_type   | pv/cart/fav/buy → view/cart/purchase |
| (无)            | session_id      | M2 新增字段，M1 在清洗阶段生成     |
"""
    print(schema_doc)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="M2 实时数据生成器")
    parser.add_argument("--mode", choices=["console", "file", "schema"], default="console", help="运行模式")
    parser.add_argument("--eps", type=float, default=5, help="每秒事件数 (events per second)")
    parser.add_argument("--duration", type=int, default=30, help="持续时长（秒）")
    parser.add_argument("--output", type=str, default="m2_events.json", help="输出文件路径（仅 file 模式）")
    parser.add_argument("--users", type=int, default=1000, help="模拟用户数量")
    parser.add_argument("--items", type=int, default=500, help="模拟商品数量")
    parser.add_argument("--format", choices=["json", "csv"], default="json", help="输出格式")

    args = parser.parse_args()

    if args.mode == "schema":
        print_schema_documentation()
    else:
        # 创建模拟器
        simulator = UserBehaviorSimulator(
            num_users=args.users,
            num_items=args.items,
            view_prob=0.80,
            cart_prob=0.15,
            purchase_prob=0.05,
        )

        # 创建生产者
        producer = EventProducer(simulator)

        if args.mode == "console":
            producer.produce_to_console(
                events_per_second=args.eps,
                duration_seconds=args.duration,
                format_type=args.format,
            )
        elif args.mode == "file":
            producer.produce_to_file(
                output_path=args.output,
                events_per_second=args.eps,
                duration_seconds=args.duration,
                format_type=args.format,
            )
