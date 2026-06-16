"""
M2 简易消费者观测脚本 (Consumer Observer)
=========================================

功能：
1. 持续监听 streaming_logs.jsonl 文件末尾追加的内容（类似 tail -f）
2. 统计每 10 秒钟内系统产生的新增点击数 (view) 和购买数 (purchase)
3. 在控制台打印滑动窗口的小计

使用方法：
    python m2_observer.py              # 默认监听 streaming_logs.jsonl
    python m2_observer.py --file xxx   # 指定其他文件
    python m2_observer.py --window 5   # 设置统计窗口为 5 秒

按 Ctrl+C 优雅停止。
"""

import json
import time
import argparse
import os
import signal
import sys
from collections import Counter


class StreamObserver:
    """流数据观测器 - 类似 tail -f 的 Python 实现"""

    def __init__(self, file_path: str, window_seconds: int = 10):
        self.file_path = file_path
        self.window_seconds = window_seconds
        self.running = False
        
        # 统计信息
        self.window_stats = Counter()
        self.total_stats = Counter()
        self.window_start_time = None

    def follow_file(self):
        """持续监听文件末尾追加的内容"""
        print(f"👁️  观测器启动")
        print(f"📁 监听文件：{self.file_path}")
        print(f"⏱️  统计窗口：{self.window_seconds} 秒")
        print(f"{'='*70}")
        print(f"⏹️  按 Ctrl+C 优雅停止\n")

        # 等待文件存在
        while not os.path.exists(self.file_path):
            print(f"⏳ 等待文件生成：{self.file_path}...")
            time.sleep(1)

        # 打开文件并定位到末尾
        with open(self.file_path, "r", encoding="utf-8") as f:
            # 先跳到文件末尾，只监听新增内容
            f.seek(0, 2)  # 2 = SEEK_END
            print("📡 开始监听文件新增内容...\n")

            self.running = True
            self.window_start_time = time.time()
            last_data_time = time.time()  # 记录最后一次收到数据的时间
            idle_timeout = 15  # 15 秒无新数据则自动停止

            try:
                while self.running:
                    line = f.readline()
                    
                    if line:
                        # 解析新行
                        try:
                            event = json.loads(line.strip())
                            behavior = event.get("behavior_type", "unknown")
                            
                            # 更新窗口统计
                            self.window_stats[behavior] += 1
                            self.total_stats[behavior] += 1
                            last_data_time = time.time()  # 更新最后收到数据的时间
                        except json.JSONDecodeError:
                            pass
                    else:
                        # 没有新数据，检查是否需要打印窗口统计
                        current_time = time.time()
                        elapsed = current_time - self.window_start_time
                        
                        if elapsed >= self.window_seconds:
                            self._print_window_stats(elapsed)
                            self.window_start_time = current_time
                            self.window_stats = Counter()
                        
                        # 检查是否长时间无新数据（Producer 可能已停止）
                        idle_time = current_time - last_data_time
                        if idle_time >= idle_timeout:
                            print(f"\n⚠️  检测到文件已 {idle_timeout} 秒无新数据，Producer 可能已停止。")
                            print("🛑 观测器自动停止。\n")
                            self.running = False
                            break
                        
                        # 短暂休眠，避免 CPU 占用过高
                        time.sleep(0.1)

            except KeyboardInterrupt:
                print(f"\n\n⚠️  收到 Ctrl+C 信号，正在停止观测...")
                self.running = False
            except Exception as e:
                print(f"\n❌ 发生异常：{e}")
                self.running = False
            finally:
                self._print_final_stats()
                print(f"\n✅ 观测器已安全停止。")

    def _print_window_stats(self, elapsed: float):
        """打印当前窗口的统计信息"""
        view_count = self.window_stats.get("view", 0)
        cart_count = self.window_stats.get("cart", 0)
        purchase_count = self.window_stats.get("purchase", 0)
        total = view_count + cart_count + purchase_count
        
        # 计算速率
        view_rate = view_count / elapsed if elapsed > 0 else 0
        purchase_rate = purchase_count / elapsed if elapsed > 0 else 0
        
        print(f"{'─'*70}")
        print(f"📊 最近 {elapsed:.0f} 秒窗口统计：")
        print(f"   view (点击)：  {view_count:5d} 条  (速率：{view_rate:5.1f} 条/秒)")
        print(f"   cart (加购)：  {cart_count:5d} 条")
        print(f"   purchase (购买)：{purchase_count:5d} 条  (速率：{purchase_rate:5.1f} 条/秒)")
        print(f"   ─────────────")
        print(f"   窗口总计：    {total:5d} 条")
        print(f"{'─'*70}\n")

    def _print_final_stats(self):
        """打印最终统计信息"""
        total_all = sum(self.total_stats.values())
        if total_all == 0:
            print("\n📊 观测期间未收到任何数据。")
            return
        
        print(f"\n{'='*70}")
        print(f"📊 观测期间总统计：")
        print(f"{'='*70}")
        for behavior, count in self.total_stats.most_common():
            pct = count / total_all * 100
            bar = "█" * int(pct / 2)
            print(f"   {behavior:10s} {count:6,}  ({pct:5.1f}%)  {bar}")
        print(f"{'─'*70}")
        print(f"   总计：{total_all:,} 条")
        print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser(description="M2 流数据观测脚本 (Consumer Observer)")
    parser.add_argument("--file", type=str, default="streaming_logs.jsonl", help="要监听的文件路径")
    parser.add_argument("--window", type=int, default=10, help="统计窗口时长（秒）")
    
    args = parser.parse_args()
    
    observer = StreamObserver(
        file_path=args.file,
        window_seconds=args.window,
    )
    
    observer.follow_file()


if __name__ == "__main__":
    main()
