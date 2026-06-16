from __future__ import annotations

import argparse
import csv
import json
import queue
import random
import threading
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import joblib
import pandas as pd

from m2_producer import EcommerceDataProducer

SCORED_FIELDS = [
    "processed_at",
    "event_time",
    "user_id",
    "item_id",
    "category_id",
    "predicted_label",
    "buy_probability",
    "error",
]

METRICS_FIELDS = [
    "timestamp",
    "elapsed_sec",
    "queue_depth",
    "queue_limit",
    "load_pct",
    "produced",
    "enqueued",
    "consumed",
    "failed",
    "dead_lettered",
    "fallback_predictions",
    "queue_full_drops",
    "chaos_injected",
    "backpressure_active",
]


@dataclass
class M2Config:
    qps: int = 200
    queue_limit: int = 500
    consumer_workers: int = 2
    consumer_process_sec: float = 0.003
    duration_sec: int = 30
    chaos_rate: float = 0.01
    output_csv: str = "m2_scored_events.csv"
    metrics_csv: str = "m2_backpressure_metrics.csv"
    dead_letter_log: str = "dead_letter.log"
    model_path: str = "model.pkl"
    backpressure_high: float = 0.85
    backpressure_low: float = 0.30
    backpressure_slowdown: float = 3.0
    random_seed: int = 42
    producer_users: int = 1000
    producer_items: int = 500


def _safe_int(v: Any) -> int:
    if v is None:
        raise ValueError("value is None")
    return int(float(v))


def _event_timestamp(event: Dict[str, Any]) -> int:
    if "timestamp" in event and event["timestamp"] not in (None, ""):
        return _safe_int(event["timestamp"])
    if "event_time" not in event:
        raise KeyError("event_time/timestamp missing")
    dt = datetime.strptime(str(event["event_time"]), "%Y-%m-%d %H:%M:%S.%f")
    return int(dt.timestamp())


def event_to_feature_frame(event: Dict[str, Any]) -> pd.DataFrame:
    if not isinstance(event, dict):
        raise TypeError(f"event must be dict, got {type(event)}")

    user_id = _safe_int(event["user_id"])
    item_id = _safe_int(event["item_id"])
    category_id = _safe_int(event.get("category_id", (item_id % 500) + 1))
    ts = _event_timestamp(event)

    dt = datetime.fromtimestamp(ts)
    hour = dt.hour
    dayofweek = dt.weekday()

    row = {
        "user_id": user_id,
        "item_id": item_id,
        "category_id": category_id,
        "hour": hour,
        "dayofweek": dayofweek,
        "is_weekend": 1.0 if dayofweek in (5, 6) else 0.0,
        "hour_bucket": str(hour),
        "day_bucket": str(dayofweek),
    }
    return pd.DataFrame([row])


def inject_chaos(event: Dict[str, Any], chaos_rate: float, rng: random.Random) -> Tuple[Any, Optional[str]]:
    if chaos_rate <= 0 or rng.random() >= chaos_rate:
        return event, None

    kind = rng.choice(["drop_field", "wrong_type", "bad_timestamp", "non_dict"])
    bad = dict(event)

    if kind == "drop_field":
        bad.pop(rng.choice(["user_id", "item_id", "event_time"]), None)
        return bad, "drop_field"
    if kind == "wrong_type":
        bad["item_id"] = "not_an_int"
        return bad, "wrong_type"
    if kind == "bad_timestamp":
        bad["event_time"] = "2099/99/99 99:99:99"
        return bad, "bad_timestamp"
    return "BROKEN_EVENT_PAYLOAD", "non_dict"


def initialize_output_files(cfg: M2Config) -> None:
    for p in (cfg.output_csv, cfg.metrics_csv, cfg.dead_letter_log):
        Path(p).unlink(missing_ok=True)

    with open(cfg.output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SCORED_FIELDS)
        writer.writeheader()

    with open(cfg.metrics_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=METRICS_FIELDS)
        writer.writeheader()

    Path(cfg.dead_letter_log).touch()


def append_scored_row(path: str, row: Dict[str, Any], lock: threading.Lock) -> None:
    with lock:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SCORED_FIELDS)
            writer.writerow(row)


def append_dead_letter(path: str, payload: Dict[str, Any], lock: threading.Lock) -> None:
    with lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")


class ModelAdapter:
    """
    封装模型推理逻辑，并在模型版本不兼容时自动降级到规则推理，避免全量失败。
    """

    def __init__(self, model_path: str, worker_id: int):
        self.model_path = model_path
        self.worker_id = worker_id
        self.model = None
        self.mode = "model"  # model | rule_fallback
        self.model_probe_error = ""
        self.fallback_reason = ""

        self._init_model()

    def _init_model(self) -> None:
        try:
            self.model = joblib.load(self.model_path)
            self._probe_model()
        except Exception as exc:
            self._activate_rule_fallback(f"model_init_failed: {exc!r}")

    def _probe_model(self) -> None:
        """
        用一条合法样本做探针，提前发现 sklearn 版本导致的不可用模型。
        """
        probe_event = {
            "event_time": "2026-05-04 12:00:00.000",
            "user_id": 1,
            "item_id": 1,
            "category_id": 1,
            "behavior_type": "view",
        }
        features = event_to_feature_frame(probe_event)
        _ = self.model.predict(features)
        if hasattr(self.model, "predict_proba"):
            _ = self.model.predict_proba(features)

    def _activate_rule_fallback(self, reason: str) -> None:
        self.mode = "rule_fallback"
        self.fallback_reason = reason
        self.model_probe_error = reason
        print(
            f"[WARN] worker={self.worker_id} model incompatible, switched to rule fallback. reason={reason}",
            flush=True,
        )

    @staticmethod
    def _rule_predict(event: Dict[str, Any]) -> Tuple[int, float]:
        behavior = str(event.get("behavior_type", "")).strip().lower()
        prob_map = {
            "buy": 0.95,
            "purchase": 0.95,
            "cart": 0.35,
            "fav": 0.25,
            "view": 0.08,
            "pv": 0.08,
        }
        prob = float(prob_map.get(behavior, 0.12))
        pred = int(prob >= 0.5)
        return pred, prob

    def predict(self, event: Dict[str, Any], features: pd.DataFrame) -> Tuple[int, float, str]:
        """
        返回: (pred, prob, error_tag)
        """
        if self.mode == "rule_fallback":
            pred, prob = self._rule_predict(event)
            return pred, prob, "RULE_FALLBACK"

        try:
            pred = int(self.model.predict(features)[0])
            if hasattr(self.model, "predict_proba"):
                prob = float(self.model.predict_proba(features)[0][1])
            else:
                prob = float("nan")
            return pred, prob, ""
        except Exception as exc:
            # 运行时才暴露不兼容，也切换到兜底推理，避免全量 dead letter
            self._activate_rule_fallback(f"model_predict_failed: {exc!r}")
            pred, prob = self._rule_predict(event)
            return pred, prob, "RULE_FALLBACK"


class ProducerThread(threading.Thread):
    def __init__(
        self,
        q: queue.Queue,
        stop_event: threading.Event,
        cfg: M2Config,
        state: Dict[str, Any],
    ):
        super().__init__(daemon=True)
        self.q = q
        self.stop_event = stop_event
        self.cfg = cfg
        self.state = state
        self.rng = random.Random(cfg.random_seed)
        self.generator = EcommerceDataProducer(
            num_users=cfg.producer_users,
            num_items=cfg.producer_items,
        )

    def run(self) -> None:
        base_interval = 1.0 / max(1, self.cfg.qps)

        while not self.stop_event.is_set():
            event = self.generator.generate_event()
            payload, chaos_kind = inject_chaos(event, self.cfg.chaos_rate, self.rng)

            with self.state["lock"]:
                self.state["produced"] += 1
                if chaos_kind:
                    self.state["chaos_injected"] += 1

            try:
                self.q.put(payload, timeout=0.2)
                with self.state["lock"]:
                    self.state["enqueued"] += 1
            except queue.Full:
                with self.state["lock"]:
                    self.state["queue_full_drops"] += 1

            with self.state["lock"]:
                backpressure_active = bool(self.state["backpressure_active"])

            delay = base_interval * (self.cfg.backpressure_slowdown if backpressure_active else 1.0)
            if delay > 0:
                time.sleep(delay)


class ConsumerThread(threading.Thread):
    def __init__(
        self,
        worker_id: int,
        q: queue.Queue,
        stop_event: threading.Event,
        cfg: M2Config,
        state: Dict[str, Any],
        scored_lock: threading.Lock,
        dead_lock: threading.Lock,
    ):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.q = q
        self.stop_event = stop_event
        self.cfg = cfg
        self.state = state
        self.scored_lock = scored_lock
        self.dead_lock = dead_lock
        self.model_adapter = ModelAdapter(cfg.model_path, worker_id=worker_id)
        with self.state["lock"]:
            # 记录最终后端模式，便于 summary 输出
            if self.model_adapter.mode == "rule_fallback":
                self.state["model_backend"] = "rule_fallback"
                self.state["model_probe_error"] = self.model_adapter.model_probe_error

    def run(self) -> None:
        while True:
            if self.stop_event.is_set() and self.q.empty():
                break

            try:
                item = self.q.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                features = event_to_feature_frame(item)
                pred, prob, err_tag = self.model_adapter.predict(item, features)

                record = {
                    "processed_at": datetime.now().isoformat(timespec="seconds"),
                    "event_time": item.get("event_time") if isinstance(item, dict) else None,
                    "user_id": item.get("user_id") if isinstance(item, dict) else None,
                    "item_id": item.get("item_id") if isinstance(item, dict) else None,
                    "category_id": item.get("category_id") if isinstance(item, dict) else None,
                    "predicted_label": pred,
                    "buy_probability": prob,
                    "error": err_tag,
                }
                append_scored_row(self.cfg.output_csv, record, self.scored_lock)

                with self.state["lock"]:
                    self.state["consumed"] += 1
                    if err_tag == "RULE_FALLBACK":
                        self.state["fallback_predictions"] += 1

            except Exception as exc:
                dead_record = {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "worker_id": self.worker_id,
                    "error": repr(exc),
                    "trace": traceback.format_exc(limit=1),
                    "event": item,
                }
                append_dead_letter(self.cfg.dead_letter_log, dead_record, self.dead_lock)

                with self.state["lock"]:
                    self.state["failed"] += 1
                    self.state["dead_lettered"] += 1

            finally:
                if self.cfg.consumer_process_sec > 0:
                    time.sleep(self.cfg.consumer_process_sec)
                self.q.task_done()


class MetricsThread(threading.Thread):
    def __init__(
        self,
        q: queue.Queue,
        stop_event: threading.Event,
        cfg: M2Config,
        state: Dict[str, Any],
        started_at: float,
    ):
        super().__init__(daemon=True)
        self.q = q
        self.stop_event = stop_event
        self.cfg = cfg
        self.state = state
        self.started_at = started_at

    def run(self) -> None:
        while not self.stop_event.is_set():
            self._write_one()
            time.sleep(0.5)
        self._write_one()

    def _write_one(self) -> None:
        now = time.time()
        elapsed = now - self.started_at
        queue_depth = self.q.qsize()
        queue_limit = self.cfg.queue_limit
        load_pct = (queue_depth / queue_limit) if queue_limit > 0 else 0.0

        with self.state["lock"]:
            active = bool(self.state["backpressure_active"])
            if queue_limit > 0:
                if (not active) and load_pct >= self.cfg.backpressure_high:
                    self.state["backpressure_active"] = True
                    active = True
                elif active and load_pct <= self.cfg.backpressure_low:
                    self.state["backpressure_active"] = False
                    active = False

            snapshot = {
                "produced": self.state["produced"],
                "enqueued": self.state["enqueued"],
                "consumed": self.state["consumed"],
                "failed": self.state["failed"],
                "dead_lettered": self.state["dead_lettered"],
                "fallback_predictions": self.state["fallback_predictions"],
                "queue_full_drops": self.state["queue_full_drops"],
                "chaos_injected": self.state["chaos_injected"],
                "backpressure_active": active,
            }

        row = {
            "timestamp": datetime.fromtimestamp(now).isoformat(timespec="seconds"),
            "elapsed_sec": round(elapsed, 2),
            "queue_depth": queue_depth,
            "queue_limit": queue_limit,
            "load_pct": round(load_pct, 4),
            **snapshot,
        }
        with open(self.cfg.metrics_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=METRICS_FIELDS)
            writer.writerow(row)


def run_pipeline(cfg: M2Config) -> Dict[str, Any]:
    if cfg.qps <= 0:
        raise ValueError("qps must be > 0")
    if cfg.consumer_workers <= 0:
        raise ValueError("consumer_workers must be > 0")
    if cfg.duration_sec <= 0:
        raise ValueError("duration_sec must be > 0")
    if not Path(cfg.model_path).exists():
        raise FileNotFoundError(f"model not found: {cfg.model_path}")

    initialize_output_files(cfg)

    q = queue.Queue(maxsize=cfg.queue_limit) if cfg.queue_limit > 0 else queue.Queue()
    stop_event = threading.Event()
    scored_lock = threading.Lock()
    dead_lock = threading.Lock()

    state: Dict[str, Any] = {
        "produced": 0,
        "enqueued": 0,
        "consumed": 0,
        "failed": 0,
        "dead_lettered": 0,
        "fallback_predictions": 0,
        "queue_full_drops": 0,
        "chaos_injected": 0,
        "backpressure_active": False,
        "model_backend": "sklearn_model",
        "model_probe_error": "",
        "lock": threading.Lock(),
    }

    producer = ProducerThread(q=q, stop_event=stop_event, cfg=cfg, state=state)
    consumers = [
        ConsumerThread(
            worker_id=i,
            q=q,
            stop_event=stop_event,
            cfg=cfg,
            state=state,
            scored_lock=scored_lock,
            dead_lock=dead_lock,
        )
        for i in range(cfg.consumer_workers)
    ]

    started_at = time.time()
    metrics = MetricsThread(
        q=q,
        stop_event=stop_event,
        cfg=cfg,
        state=state,
        started_at=started_at,
    )

    producer.start()
    for worker in consumers:
        worker.start()
    metrics.start()

    try:
        time.sleep(cfg.duration_sec)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()

    producer.join(timeout=5)
    for worker in consumers:
        worker.join(timeout=10)
    metrics.join(timeout=3)

    elapsed = time.time() - started_at
    with state["lock"]:
        summary = {
            "config": asdict(cfg),
            "elapsed_sec": round(elapsed, 3),
            "produced": state["produced"],
            "enqueued": state["enqueued"],
            "consumed": state["consumed"],
            "failed": state["failed"],
            "dead_lettered": state["dead_lettered"],
            "fallback_predictions": state["fallback_predictions"],
            "queue_full_drops": state["queue_full_drops"],
            "chaos_injected": state["chaos_injected"],
            "backpressure_active_final": state["backpressure_active"],
            "model_backend": state["model_backend"],
            "model_probe_error": state["model_probe_error"],
            "final_queue_depth": q.qsize(),
            "output_csv": cfg.output_csv,
            "metrics_csv": cfg.metrics_csv,
            "dead_letter_log": cfg.dead_letter_log,
        }

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Week08 M2 integration pipeline runner")
    parser.add_argument("--qps", type=int, default=200, help="Producer events per second")
    parser.add_argument("--queue-limit", type=int, default=500, help="Bounded queue size; <=0 means unbounded")
    parser.add_argument("--consumer-workers", type=int, default=2, help="Number of consumer threads")
    parser.add_argument("--consumer-process-sec", type=float, default=0.003, help="Artificial per-event consume latency")
    parser.add_argument("--duration-sec", type=int, default=30, help="Runtime duration in seconds")
    parser.add_argument("--chaos-rate", type=float, default=0.01, help="Probability of chaos event injection")
    parser.add_argument("--output-csv", type=str, default="m2_scored_events.csv", help="Scored event output CSV")
    parser.add_argument("--metrics-csv", type=str, default="m2_backpressure_metrics.csv", help="Runtime metrics CSV")
    parser.add_argument("--dead-letter-log", type=str, default="dead_letter.log", help="Dead letter log path")
    parser.add_argument("--model-path", type=str, default="model.pkl", help="Path to model.pkl")
    parser.add_argument("--backpressure-high", type=float, default=0.85, help="High watermark to enable backpressure")
    parser.add_argument("--backpressure-low", type=float, default=0.30, help="Low watermark to disable backpressure")
    parser.add_argument("--backpressure-slowdown", type=float, default=3.0, help="Producer slowdown factor under backpressure")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for chaos injector")
    parser.add_argument("--producer-users", type=int, default=1000, help="Synthetic producer user count")
    parser.add_argument("--producer-items", type=int, default=500, help="Synthetic producer item count")
    return parser


def main(argv: Optional[list[str]] = None) -> Dict[str, Any]:
    args = build_parser().parse_args(argv)
    cfg = M2Config(
        qps=args.qps,
        queue_limit=args.queue_limit,
        consumer_workers=args.consumer_workers,
        consumer_process_sec=args.consumer_process_sec,
        duration_sec=args.duration_sec,
        chaos_rate=args.chaos_rate,
        output_csv=args.output_csv,
        metrics_csv=args.metrics_csv,
        dead_letter_log=args.dead_letter_log,
        model_path=args.model_path,
        backpressure_high=args.backpressure_high,
        backpressure_low=args.backpressure_low,
        backpressure_slowdown=args.backpressure_slowdown,
        random_seed=args.seed,
        producer_users=args.producer_users,
        producer_items=args.producer_items,
    )
    return run_pipeline(cfg)


if __name__ == "__main__":
    main()
