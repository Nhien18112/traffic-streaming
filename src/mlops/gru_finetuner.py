"""
Continuous Fine-tuning Worker for GRU Model
Fix: Train đúng cho cả 3 horizons (+5m, +10m, +15m) bằng cách
     thu thập actual speed tại t+1, t+2, t+3 làm target labels.
"""
import os
import json
import logging
import sys
import numpy as np
import torch
import torch.nn as nn
from datetime import datetime
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from pathlib import Path

from model_1 import TGCN
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.time_encoding import encode_time_features, extract_time_components
from graph import edge_index, edge_weight, node_list, node_to_idx

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [GRU FT] - %(message)s')

KAFKA_BROKER = 'kafka:19092'
CONSUME_TOPIC = 'traffic.feature'
PRODUCE_TOPIC = 'model.finetuned'

# ============================================
# CẤU HÌNH ĐỒNG BỘ VÀ TỐI ƯU FINE-TUNE
# ============================================
WINDOW_SIZE = 3
NUM_HORIZONS = 3       # Số bước dự báo: +5m, +10m, +15m
BATCH_SIZE = 16
FINETUNE_INTERVAL = 32
LEARNING_RATE = 0.0001
NUM_EPOCHS_PER_BATCH = 3

NUM_NODES = len(node_list)
NUM_FEATURES = 13

# Mảng gồm 9 giá trị: [Speed, TravelTime, FreeSpeed, FreeTravelTime, Incident, Temp, Hum, Wind, Vis]
FEATURE_MEANS = np.array([19.564741, 799.571594, 30.122337, 500.475311, 0.0, 31.281614, 56.04697, 3.399138, 8.995898], dtype=np.float32)
FEATURE_STDS = np.array([6.569058, 825.169006, 7.928298, 514.192749, 1e-05, 3.098041, 3.575646, 0.344204, 0.579564], dtype=np.float32)

MODEL_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
CHECKPOINT_DIR = MODEL_DIR / 'checkpoints'
CHECKPOINT_DIR.mkdir(exist_ok=True)

PROD_MODEL_PATH = MODEL_DIR / 'gru_model_v1.pt'
LATEST_CHECKPOINT_PATH = CHECKPOINT_DIR / 'gru_model_latest.pt'
BEST_CHECKPOINT_PATH = CHECKPOINT_DIR / 'gru_model_best.pt'


class FineTuningBuffer:
    """
    Buffer thu thập dữ liệu cho multi-horizon fine-tuning.

    Logic:
        - Khi đủ WINDOW_SIZE snapshot, tạo một pending sample (x_window, []).
        - Các NUM_HORIZONS timestamp tiếp theo sẽ cung cấp actual speed
          tại t+1, t+2, t+3 → lần lượt append vào collected_speeds.
        - Khi collected_speeds đủ 3 giá trị → sample hoàn chỉnh được trả về
          với y_targets shape [NUM_NODES, NUM_HORIZONS].
    """

    def __init__(self, window_size: int = WINDOW_SIZE, num_horizons: int = NUM_HORIZONS):
        self.window_size = window_size
        self.num_horizons = num_horizons
        self.current_timestamp = None
        self.current_graph_snapshot = np.zeros((NUM_NODES, NUM_FEATURES), dtype=np.float32)
        self.window_buffer = deque(maxlen=window_size)
        # Pending samples: list of (x_window, source_ts, [y_h1, y_h2, ...])
        self.pending_samples: list = []
        self.free_flow_cache: dict = {}

    def add_feature(self, feature_data: dict) -> list:
        """
        Nhận một message feature và cập nhật buffer.
        Returns:
            complete_samples: list of (x_window, y_targets, source_ts)
                x_window  shape: [WINDOW_SIZE, NUM_NODES, NUM_FEATURES]
                y_targets shape: [NUM_NODES, NUM_HORIZONS]  (actual speed tại t+1..t+3)
        """
        location = feature_data.get('location_name', '').replace('Vong_Xoay_Lang_Cha_Ca', 'Lang_Cha_Ca')
        if location not in node_list:
            return []

        msg_time = feature_data.get('time')
        node_idx = node_to_idx[location]

        raw_features = [
            float(feature_data.get('current_speed', feature_data.get('currentSpeed', 0.0))),
            float(feature_data.get('currentTravelTime', 0.0)),
            float(feature_data.get('free_flow_speed', feature_data.get('freeFlowSpeed', 0.0))),
            float(feature_data.get('freeFlowTravelTime', 0.0)),
            float(feature_data.get('incident_count', 0)),
            float(feature_data.get('temperature', 0.0)),
            float(feature_data.get('humidity', 0.0)),
            float(feature_data.get('wind_speed', 0.0)),
            float(feature_data.get('visibility', 0.0))
        ]
        self.free_flow_cache[node_idx] = raw_features[2]

        time_features = [0.0, 0.0, 0.0, 0.0]
        if msg_time:
            try:
                hour, minute, second, weekday = extract_time_components(msg_time)
                time_features = encode_time_features(hour, minute, second, weekday)
            except Exception:
                pass

        feature_vector = raw_features + time_features
        complete_samples = []

        # Phát hiện chuyển timestamp → kết thúc snapshot cũ, bắt đầu snapshot mới
        if self.current_timestamp is not None and msg_time != self.current_timestamp:
            # 1. Đóng snapshot hiện tại và thêm vào window_buffer
            self.window_buffer.append(self.current_graph_snapshot.copy())

            # 2. Tốc độ thực tại timestamp HIỆN TẠI (= t+k cho pending samples)
            #    Column 0 của snapshot là current_speed
            actual_speed_now = self.current_graph_snapshot[:, 0].copy()  # [NUM_NODES]

            # 3. Cập nhật tất cả pending samples đang chờ target
            still_pending = []
            for (x_window, source_ts, collected_speeds) in self.pending_samples:
                collected_speeds.append(actual_speed_now.copy())
                if len(collected_speeds) == self.num_horizons:
                    # Đủ NUM_HORIZONS bước → tạo y_targets [NUM_NODES, NUM_HORIZONS]
                    y_targets = np.stack(collected_speeds, axis=1)  # [NUM_NODES, NUM_HORIZONS]
                    complete_samples.append((x_window, y_targets, source_ts))
                    logging.debug(
                        f"[Buffer] Hoàn thành sample tại {source_ts} | "
                        f"Speed mean: 5m={y_targets[:,0].mean():.1f}, "
                        f"10m={y_targets[:,1].mean():.1f}, "
                        f"15m={y_targets[:,2].mean():.1f} km/h"
                    )
                else:
                    still_pending.append((x_window, source_ts, collected_speeds))
            self.pending_samples = still_pending

            # 4. Nếu đủ window → tạo pending sample mới chờ 3 future targets
            if len(self.window_buffer) == self.window_size:
                x_window = np.array(list(self.window_buffer), dtype=np.float32)
                self.pending_samples.append((x_window, self.current_timestamp, []))

        # Cập nhật timestamp và ghi feature vào snapshot hiện tại
        self.current_timestamp = msg_time
        self.current_graph_snapshot[node_idx] = np.array(feature_vector, dtype=np.float32)

        return complete_samples


class GRUFineTuner:
    def __init__(self):
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model = TGCN(num_node_features=NUM_FEATURES, hidden_dim=32, num_horizons=NUM_HORIZONS)
        self.load_production_model()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=LEARNING_RATE)
        self.loss_fn = nn.MSELoss()
        self.best_loss = float('inf')  # Làm mới mói lần khởi động - không mang best_loss cũ sang
        self.finetuned_steps = 0

    def load_production_model(self):
        try:
            if PROD_MODEL_PATH.exists():
                self.model.load_state_dict(torch.load(PROD_MODEL_PATH, map_location=self.device))
                logging.info(f"Loaded production model from {PROD_MODEL_PATH}")
        except Exception as e:
            logging.error(f"Error loading model: {e}")
        self.model.to(self.device)

    def finetune_batch(self, batch_samples: list) -> float:
        """
        Tính loss cho CẢ 3 horizons:
            loss = MSE(pred_5m, y_5m) + MSE(pred_10m, y_10m) + MSE(pred_15m, y_15m)

        Args:
            batch_samples: list of (x_window, y_targets, ts)
                x_window  shape: [WINDOW_SIZE, NUM_NODES, NUM_FEATURES]
                y_targets shape: [NUM_NODES, NUM_HORIZONS]
        Returns:
            avg_loss (float)
        """
        self.model.train()
        batch_loss = 0.0
        valid_count = 0

        edge_idx = edge_index.to(self.device)
        edge_w = edge_weight.to(self.device)

        for x_sample, y_targets, ts in batch_samples:
            try:
                # ── Chuẩn hóa X ──────────────────────────────────────────
                x_normalized = x_sample.copy()
                x_normalized[:, :, :9] = (x_normalized[:, :, :9] - FEATURE_MEANS) / FEATURE_STDS
                # Shape: [1, WINDOW_SIZE, NUM_NODES, NUM_FEATURES]
                x_tensor = torch.tensor(x_normalized, dtype=torch.float32).unsqueeze(0).to(self.device)

                # ── Chuẩn hóa Y cho cả 3 horizons ───────────────────────
                # y_targets: [NUM_NODES, NUM_HORIZONS] - actual speed (km/h)
                y_normalized = (y_targets - FEATURE_MEANS[0]) / FEATURE_STDS[0]
                # Shape: [NUM_NODES, NUM_HORIZONS]
                y_tensor = torch.tensor(y_normalized, dtype=torch.float32).to(self.device)

                # ── Forward pass ──────────────────────────────────────────
                # pred shape: [1, NUM_NODES, NUM_HORIZONS]
                predictions = self.model(x_tensor, edge_idx, edge_w)
                # Squeeze batch dim → [NUM_NODES, NUM_HORIZONS]
                preds = predictions.squeeze(0)

                # ── Loss trên cả 3 horizons đồng thời ────────────────────
                loss = self.loss_fn(preds, y_tensor)

                self.optimizer.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=1.0)
                self.optimizer.step()

                batch_loss += loss.item()
                valid_count += 1

            except Exception as e:
                logging.warning(f"[FineTune] Bỏ qua sample lỗi: {e}")
                continue

        avg_loss = batch_loss / max(valid_count, 1)
        self.finetuned_steps += 1
        return avg_loss

    def save_checkpoint(self, is_best: bool = False):
        torch.save(self.model.state_dict(), LATEST_CHECKPOINT_PATH)
        if is_best:
            torch.save(self.model.state_dict(), BEST_CHECKPOINT_PATH)

    def promote_to_production(self) -> bool:
        try:
            import shutil
            shutil.copy(LATEST_CHECKPOINT_PATH, PROD_MODEL_PATH)
            return True
        except Exception:
            return False


def main():
    buffer = FineTuningBuffer(window_size=WINDOW_SIZE, num_horizons=NUM_HORIZONS)
    finetuner = GRUFineTuner()

    consumer = KafkaConsumer(
        CONSUME_TOPIC, bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else {},
        auto_offset_reset='latest',   # Chỉ train trên data realtime mới
        max_poll_records=32,
        group_id='gru_finetuner_group_v2'  # Group mới để bỏ offset backlog cũ
    )
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    # Buffer tích lũy, mỗi sample giờ có y_targets [NUM_NODES, NUM_HORIZONS]
    accumulated_samples: deque = deque(maxlen=FINETUNE_INTERVAL)
    promoted = False

    for message in consumer:
        complete_samples = buffer.add_feature(message.value)
        for x, y_targets, ts in complete_samples:
            accumulated_samples.append((x, y_targets, ts))

            if len(accumulated_samples) == FINETUNE_INTERVAL:
                for epoch in range(NUM_EPOCHS_PER_BATCH):
                    avg_loss = finetuner.finetune_batch(list(accumulated_samples))
                    promoted = False

                    if avg_loss < finetuner.best_loss:
                        finetuner.best_loss = avg_loss
                        finetuner.save_checkpoint(is_best=True)
                        promoted = finetuner.promote_to_production()
                        logging.info(
                            f"Epoch {epoch+1}/{NUM_EPOCHS_PER_BATCH} - "
                            f"Loss (3 horizons): {avg_loss:.6f} ← Tốt hơn! Đã promote model."
                        )
                    else:
                        finetuner.save_checkpoint(is_best=False)
                        logging.info(
                            f"Epoch {epoch+1}/{NUM_EPOCHS_PER_BATCH} - "
                            f"Loss (3 horizons): {avg_loss:.6f} | Best: {finetuner.best_loss:.6f}"
                        )

                producer.send(PRODUCE_TOPIC, value={
                    "timestamp": datetime.now().isoformat(),
                    "event": "model_finetuned",
                    "steps": finetuner.finetuned_steps,
                    "best_loss": float(finetuner.best_loss),
                    "promoted_to_production": promoted,
                    "horizons_trained": NUM_HORIZONS,
                })
                accumulated_samples.clear()


if __name__ == "__main__":
    main()