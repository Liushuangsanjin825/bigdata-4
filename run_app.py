#!/usr/bin/env python3
"""大数据分析看板 · 一键启动脚本 (Week 14 系统联调)

功能：
  1. 环境自检 —— 检查必要文件、端口占用
  2. 异步进程管理 —— 使用 subprocess 启动 uvicorn
  3. 自动浏览器唤起 —— 服务就绪后打开前端页面
  4. 优雅终止 —— Ctrl+C 关闭子进程，不留孤儿进程
"""

import os
import sys

# ── Windows 终端编码修复 ───────────────────────────────────
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass
import time
import signal
import socket
import subprocess
import webbrowser
import urllib.request
from pathlib import Path

# ── 配置 ──────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent
DASHBOARD_DIR = ROOT_DIR / "dashboard"
SERVER_FILE = DASHBOARD_DIR / "server.py"
FRONTEND_DIR = DASHBOARD_DIR / "frontend"
INDEX_FILE = FRONTEND_DIR / "index.html"
HOST = "127.0.0.1"
PORT = 8000
BASE_URL = f"http://{HOST}:{PORT}"

# ── 终端颜色 ──────────────────────────────────────────────
class Color:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

def log_info(msg):
    print(f"{Color.CYAN}[INFO]{Color.RESET} {msg}")

def log_ok(msg):
    print(f"{Color.GREEN}[OK]{Color.RESET} {msg}")

def log_warn(msg):
    print(f"{Color.YELLOW}[WARN]{Color.RESET} {msg}")

def log_error(msg):
    print(f"{Color.RED}[ERROR]{Color.RESET} {msg}")

# ── 环境自检 ──────────────────────────────────────────────
def check_environment():
    """检查必要文件和端口状态"""
    log_info("正在执行环境自检...")
    all_ok = True

    # 检查 dashboard 目录
    if not DASHBOARD_DIR.exists():
        log_error(f"dashboard 目录不存在: {DASHBOARD_DIR}")
        all_ok = False

    # 检查 server.py
    if not SERVER_FILE.exists():
        log_error(f"后端服务文件不存在: {SERVER_FILE}")
        all_ok = False
    else:
        log_ok(f"后端服务文件: {SERVER_FILE}")

    # 检查前端页面
    if not INDEX_FILE.exists():
        log_warn(f"前端页面不存在: {INDEX_FILE}（将无法自动打开浏览器）")
    else:
        log_ok(f"前端页面: {INDEX_FILE}")

    # 检查数据文件
    data_file = ROOT_DIR / "online_shopping_10_cats.csv"
    if not data_file.exists():
        log_warn(f"主数据文件不存在: {data_file}（后端将无法加载数据）")
    else:
        size_mb = data_file.stat().st_size / (1024 * 1024)
        log_ok(f"数据文件: {data_file.name} ({size_mb:.1f} MB)")

    # 检查端口占用
    if is_port_in_use(HOST, PORT):
        log_warn(f"端口 {PORT} 已被占用，将尝试复用或终止已有进程")
    else:
        log_ok(f"端口 {PORT} 可用")

    return all_ok

def is_port_in_use(host, port):
    """检测端口是否被占用"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        try:
            s.connect((host, port))
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False

# ── 进程管理 ──────────────────────────────────────────────
server_process = None

def start_server():
    """启动 uvicorn 子进程"""
    global server_process
    log_info(f"正在启动 FastAPI 服务 (uvicorn server:app --host {HOST} --port {PORT})...")

    server_process = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "server:app",
            "--host", HOST,
            "--port", str(PORT),
            "--log-level", "info"
        ],
        cwd=str(DASHBOARD_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        encoding="utf-8",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"}
    )
    log_ok(f"uvicorn 子进程已创建 (PID: {server_process.pid})")

def wait_for_server(timeout=30):
    """轮询等待服务就绪"""
    log_info(f"等待服务就绪 (最多 {timeout} 秒)...")
    start_time = time.time()
    check_url = f"{BASE_URL}/api/health"

    while time.time() - start_time < timeout:
        # 检查子进程是否还活着
        if server_process and server_process.poll() is not None:
            log_error(f"服务进程异常退出 (exit code: {server_process.returncode})")
            return False

        try:
            req = urllib.request.Request(check_url)
            with urllib.request.urlopen(req, timeout=1) as resp:
                if resp.status == 200:
                    elapsed = time.time() - start_time
                    log_ok(f"服务已就绪 (耗时 {elapsed:.1f}s) → {BASE_URL}")
                    return True
        except Exception:
            pass

        time.sleep(0.5)

    log_error(f"服务启动超时 ({timeout}s)")
    return False

def open_browser():
    """自动打开浏览器"""
    log_info("正在打开浏览器...")
    try:
        webbrowser.open(BASE_URL)
        log_ok(f"浏览器已打开: {BASE_URL}")
    except Exception as e:
        log_warn(f"无法自动打开浏览器: {e}")
        log_info(f"请手动访问: {BASE_URL}")

def graceful_shutdown(signum=None, frame=None):
    """优雅终止：关闭子进程"""
    global server_process
    print()  # 换行
    log_info("收到终止信号，正在关闭服务...")

    if server_process and server_process.poll() is None:
        log_info(f"正在终止 uvicorn 子进程 (PID: {server_process.pid})...")
        if sys.platform == "win32":
            server_process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            server_process.terminate()
        try:
            server_process.wait(timeout=5)
            log_ok("子进程已正常退出")
        except subprocess.TimeoutExpired:
            log_warn("子进程未在 5 秒内退出，强制终止...")
            server_process.kill()
            server_process.wait()
            log_ok("子进程已强制终止")
    else:
        log_info("没有运行中的子进程")

    log_ok("服务已关闭，再见！")
    sys.exit(0)

# ── 主入口 ────────────────────────────────────────────────
def main():
    print(f"\n{Color.BOLD}{Color.BLUE}{'=' * 50}{Color.RESET}")
    print(f"{Color.BOLD}{Color.BLUE}  大数据分析看板 - 一键启动脚本 (Week 14){Color.RESET}")
    print(f"{Color.BOLD}{Color.BLUE}{'=' * 50}{Color.RESET}\n")

    # 注册信号处理
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    # 1. 环境自检
    check_environment()

    # 2. 切换工作目录
    os.chdir(str(DASHBOARD_DIR))
    log_info(f"工作目录: {DASHBOARD_DIR}")

    # 3. 启动服务
    start_server()

    # 4. 等待就绪
    if wait_for_server():
        # 5. 打开浏览器
        open_browser()

        # 6. 转发子进程输出
        log_info("服务运行中... 按 Ctrl+C 退出\n")
        print(f"{Color.GREEN}{'-' * 50}{Color.RESET}")
        try:
            while True:
                if server_process and server_process.poll() is not None:
                    log_error(f"服务进程意外退出 (exit code: {server_process.returncode})")
                    break
                # 读取并打印子进程输出
                if server_process and server_process.stdout:
                    line = server_process.stdout.readline()
                    if line:
                        print(line.rstrip())
                    else:
                        time.sleep(0.1)
        except KeyboardInterrupt:
            pass
    else:
        log_error("服务启动失败")

    graceful_shutdown()

if __name__ == "__main__":
    main()
