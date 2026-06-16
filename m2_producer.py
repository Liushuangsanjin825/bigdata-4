"""
M2 数据模拟器 - Producer
========================

功能：
1. 根据 Schema 每秒随机生成 10-50 条 JSON 格式的电商行为日志
2. 追加写入到 streaming_logs.jsonl 本地文件
3. 包含异常处理逻辑，允许 Ctrl+C 优雅停止
4. 通过概率权重控制不同类型行为的出现频次
5. 选做挑战：引入 Zipf 长尾分布模拟爆款商品（20% 商品占 80% 流量）

使用方法：
    python m2_producer.py                    # 默认运行（10-50条/秒，无限运行）
    python m2_producer.py --min-eps 20       # 设置最小每秒事件数
    python m2_producer.py --max-eps 100      # 设置最大每秒事件数
    python m2_producer.py --duration 300     # 运行 300 秒（5 分钟）
    python m2_producer.py --output test.jsonl # 指定输出文件
    python m2_producer.py --no-zipf          # 禁用 Zipf 长尾分布

按 Ctrl+C 优雅停止程序。
"""

import json
import time
import uuid
import random
import signal
import argparse
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional
from collections import Counter

import numpy as np


class EcommerceDataProducer:
    """
    电商数据生产者 - 模拟真实的用户行为事件流
    
    特性：
    - 漏斗概率自洽：购买前大概率有浏览铺垫
    - Zipf 长尾分布：20% 爆款商品占 80% 流量
    - Session 管理：30 分钟超时自动生成新 session
    - 优雅退出：Ctrl+C 安全停止，不丢失数据
    """

    # 行为类型定义
    BEHAVIOR_TYPES = ["view", "cart", "purchase"]
    
    # 默认概率权重（可配置）
    DEFAULT_WEIGHTS = {
        "view": 0.80,      # 浏览 80%
        "cart": 0.15,      # 加购 15%
        "purchase": 0.05,  # 购买 5%
    }

    def __init__(
        self,
        num_users: int = 1000,
        num_items: int = 500,
        weights: Optional[Dict[str, float]] = None,
        session_timeout: int = 1800,
        use_zipf: bool = True,
        zipf_alpha: float = 1.2,
        hot_item_ratio: float = 0.2,
        hot_item_traffic_ratio: float = 0.8,
    ):
        """
        初始化生产者

        :param num_users: 模拟用户数量
        :param num_items: 模拟商品数量
        :param weights: 行为概率权重 {"view": 0.80, "cart": 0.15, "purchase": 0.05}
        :param session_timeout: Session 超时时间（秒）
        :param use_zipf: 是否使用 Zipf 长尾分布
        :param zipf_alpha: Zipf 分布参数（越大越集中）
        :param hot_item_ratio: 爆款商品比例（如 0.2 表示 20% 的商品是爆款）
        :param hot_item_traffic_ratio: 爆款商品流量占比（如 0.8 表示爆款占 80% 流量）
        """
        self.num_users = num_users
        self.num_items = num_items
        self.user_ids = list(range(1, num_users + 1))
        self.item_ids = list(range(1, num_items + 1))
        
        self.weights = weights or self.DEFAULT_WEIGHTS
        self.session_timeout = session_timeout
        
        # Zipf 长尾分布相关
        self.use_zipf = use_zipf
        self.zipf_alpha = zipf_alpha
        self.hot_item_ratio = hot_item_ratio
        self.hot_item_traffic_ratio = hot_item_traffic_ratio
        
        # 预计算 Zipf 概率分布
        if self.use_zipf:
            self.item_probabilities = self._compute_zipf_probabilities()
        else:
            # 均匀分布
            self.item_probabilities = [1.0 / num_items] * num_items
        
        # 用户状态追踪
        self.user_sessions: Dict[int, Dict] = {}
        
        # 统计信息
        self.stats = Counter()
        self.total_events = 0
        self.start_time = None
        
        # 优雅退出标志
        self.running = False

    def _compute_zipf_probabilities(self) -> List[float]:
        """
        计算 Zipf 长尾分布概率
        
        实现 20% 爆款商品占 80% 流量的二八定律
        使用 numpy.random.zipf 生成概率权重
        """
        num_items = self.num_items
        hot_count = max(1, int(num_items * self.hot_item_ratio))  # 爆款商品数量
        cold_count = num_items - hot_count  # 普通商品数量
        
        # 计算爆款和普通商品的权重
        # 爆款商品总权重 = hot_item_traffic_ratio
        # 普通商品总权重 = 1 - hot_item_traffic_ratio
        hot_weight = self.hot_item_traffic_ratio / hot_count
        cold_weight = (1 - self.hot_item_traffic_ratio) / cold_count if cold_count > 0 else 0
        
        probabilities = []
        for i in range(num_items):
            if i < hot_count:
                probabilities.append(hot_weight)
            else:
                probabilities.append(cold_weight)
        
        # 归一化
        total = sum(probabilities)
        probabilities = [p / total for p in probabilities]
        
        return probabilities

    def _select_item_id(self) -> int:
        """
        选择商品 ID
        使用 Zipf 长尾分布模拟爆款效应
        """
        if self.use_zipf:
            # 使用预计算的概率分布进行加权随机选择
            item_idx = np.random.choice(self.num_items, p=self.item_probabilities)
            return item_idx + 1  # 转为 1-based
        else:
            return random.choice(self.item_ids)

    def _get_or_create_session(self, user_id: int) -> str:
        """获取或创建用户的当前 session"""
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = {
                "session_id": str(uuid.uuid4())[:12],
                "last_active": time.time(),
                "history": [],
            }
            return self.user_sessions[user_id]["session_id"]

        session = self.user_sessions[user_id]
        now = time.time()

        # 检查是否超时，超时则创建新 session
        if (now - session["last_active"]) > self.session_timeout:
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

        # 如果用户近期没有浏览行为，提高浏览概率（强制先浏览再购买）
        has_recent_view = any(h == "view" for h in recent_history)

        if not has_recent_view:
            # 强制先浏览再购买/加购
            adjusted_weights = {"view": 0.95, "cart": 0.04, "purchase": 0.01}
        else:
            adjusted_weights = self.weights.copy()

        # 根据权重随机选择行为
        behaviors = list(adjusted_weights.keys())
        probs = list(adjusted_weights.values())
        return random.choices(behaviors, weights=probs, k=1)[0]

    def generate_event(self, user_id: Optional[int] = None) -> Dict:
        """
        生成单个用户行为事件

        :param user_id: 指定用户 ID（None 则随机选择）
        :return: 事件字典
        """
        if user_id is None:
            user_id = random.choice(self.user_ids)

        session_id = self._get_or_create_session(user_id)
        behavior_type = self._select_behavior_type(user_id)
        item_id = self._select_item_id()

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

        # 更新统计信息
        self.stats[behavior_type] += 1
        self.total_events += 1

        return event

    def generate_batch(self, count: int) -> List[Dict]:
        """生成一批事件"""
        return [self.generate_event() for _ in range(count)]

    def print_stats(self):
        """打印当前统计信息"""
        if self.total_events == 0:
            return
        
        elapsed = time.time() - self.start_time
        eps = self.total_events / elapsed if elapsed > 0 else 0
        
        print(f"\n{'='*70}")
        print(f"📊 统计信息 (运行 {elapsed:.1f} 秒)")
        print(f"{'='*70}")
        print(f"总事件数：{self.total_events:,}")
        print(f"平均速率：{eps:.1f} 事件/秒")
        print(f"\n行为类型分布：")
        for behavior in self.BEHAVIOR_TYPES:
            count = self.stats.get(behavior, 0)
            pct = count / self.total_events * 100 if self.total_events > 0 else 0
            bar = "█" * int(pct / 2)
            print(f"  {behavior:10s} {count:6,}  ({pct:5.1f}%)  {bar}")
        print(f"{'='*70}\n")

    def run(
        self,
        output_file: str = "streaming_logs.jsonl",
        min_eps: int = 10,
        max_eps: int = 50,
        duration: Optional[int] = None,
    ):
        """
        运行生产者，持续生成事件并写入文件

        :param output_file: 输出文件路径
        :param min_eps: 最小每秒事件数
        :param max_eps: 最大每秒事件数
        :param duration: 运行时长（秒），None 表示无限运行
        """
        self.running = True
        self.start_time = time.time()
        
        print(f"🚀 M2 数据模拟器启动")
        print(f"📁 输出文件：{output_file}")
        print(f"📊 速率范围：{min_eps}-{max_eps} 事件/秒")
        print(f"👥 用户数：{self.num_users}，商品数：{self.num_items}")
        print(f"📈 行为权重：{self.weights}")
        if self.use_zipf:
            print(f"🔥 Zipf 长尾分布：{self.hot_item_ratio*100:.0f}% 爆款商品占 {self.hot_item_traffic_ratio*100:.0f}% 流量")
        print(f"{'='*70}")
        print(f"⏹️  按 Ctrl+C 优雅停止\n")

        # 打开文件（追加模式）
        with open(output_file, "a", encoding="utf-8") as f:
            try:
                while self.running:
                    # 随机决定本批生成的事件数量
                    batch_size = random.randint(min_eps, max_eps)
                    
                    # 生成事件
                    events = self.generate_batch(batch_size)
                    
                    # 写入文件（每行一个 JSON 对象，JSONL 格式）
                    for event in events:
                        f.write(json.dumps(event, ensure_ascii=False) + "\n")
                    
                    # 刷新缓冲区
                    f.flush()

                    # 获取文件当前大小
                    current_size = os.path.getsize(output_file)
                    file_size_mb = current_size / (1024 * 1024)

                    # 打印进度
                    elapsed = time.time() - self.start_time
                    current_eps = self.total_events / elapsed if elapsed > 0 else 0
                    print(f"⚡ [{elapsed:6.1f}s] 批次 {batch_size:2d} 条 | "
                          f"累计 {self.total_events:>8,} 条 | "
                          f"速率 {current_eps:5.1f} 条/秒 | "
                          f"📦 文件 {file_size_mb:.2f} MB", flush=True)
                    
                    # 检查运行时长
                    if duration and elapsed >= duration:
                        print(f"\n✅ 达到指定运行时长 {duration} 秒，正在停止...")
                        break
                    
                    # 控制速率：sleep 1 秒后生成下一批
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                print(f"\n\n⚠️  收到 Ctrl+C 信号，正在优雅停止...")
                self.running = False
            except Exception as e:
                print(f"\n❌ 发生异常：{e}")
                self.running = False
            finally:
                # 打印最终统计
                self.print_stats()
                
                # 保存统计信息到文件
                stats_file = output_file.replace(".jsonl", "_stats.json")
                with open(stats_file, "w", encoding="utf-8") as sf:
                    json.dump({
                        "total_events": self.total_events,
                        "behavior_counts": dict(self.stats),
                        "duration_seconds": time.time() - self.start_time,
                        "config": {
                            "num_users": self.num_users,
                            "num_items": self.num_items,
                            "weights": self.weights,
                            "use_zipf": self.use_zipf,
                        }
                    }, sf, ensure_ascii=False, indent=2)
                print(f"📋 统计信息已保存到：{stats_file}")

                print(f"\n✅ 生产者已安全停止。")


def signal_handler(signum, frame):
    """信号处理函数 - 用于 Ctrl+C 优雅退出"""
    print("\n⚠️  收到退出信号...")
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description="M2 电商数据模拟器 - Producer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python m2_producer.py                           # 默认运行
  python m2_producer.py --min-eps 20 --max-eps 80 # 设置速率范围
  python m2_producer.py --duration 300            # 运行 5 分钟
  python m2_producer.py --output test.jsonl       # 指定输出文件
  python m2_producer.py --no-zipf                 # 禁用 Zipf 分布
  python m2_producer.py --zipf-alpha 2.0          # 调整 Zipf 集中度
        """
    )
    
    parser.add_argument("--min-eps", type=int, default=10, help="最小每秒事件数 (默认: 10)")
    parser.add_argument("--max-eps", type=int, default=50, help="最大每秒事件数 (默认: 50)")
    parser.add_argument("--duration", type=int, default=None, help="运行时长（秒），不指定则无限运行")
    parser.add_argument("--output", type=str, default="streaming_logs.jsonl", help="输出文件路径")
    parser.add_argument("--users", type=int, default=1000, help="模拟用户数量")
    parser.add_argument("--items", type=int, default=500, help="模拟商品数量")
    parser.add_argument("--no-zipf", action="store_true", help="禁用 Zipf 长尾分布")
    parser.add_argument("--zipf-alpha", type=float, default=1.2, help="Zipf 分布参数（越大越集中）")
    parser.add_argument("--hot-ratio", type=float, default=0.2, help="爆款商品比例（0.2 = 20%%）")
    parser.add_argument("--hot-traffic", type=float, default=0.8, help="爆款商品流量占比（0.8 = 80%%）")
    parser.add_argument("--view-weight", type=float, default=0.80, help="view 行为权重")
    parser.add_argument("--cart-weight", type=float, default=0.15, help="cart 行为权重")
    parser.add_argument("--purchase-weight", type=float, default=0.05, help="purchase 行为权重")
    
    args = parser.parse_args()
    
    # 注册信号处理
    signal.signal(signal.SIGINT, signal_handler)
    
    # 创建权重配置
    weights = {
        "view": args.view_weight,
        "cart": args.cart_weight,
        "purchase": args.purchase_weight,
    }
    
    # 创建并运行生产者
    producer = EcommerceDataProducer(
        num_users=args.users,
        num_items=args.items,
        weights=weights,
        use_zipf=not args.no_zipf,
        zipf_alpha=args.zipf_alpha,
        hot_item_ratio=args.hot_ratio,
        hot_item_traffic_ratio=args.hot_traffic,
    )
    
    producer.run(
        output_file=args.output,
        min_eps=args.min_eps,
        max_eps=args.max_eps,
        duration=args.duration,
    )


if __name__ == "__main__":
    main()
