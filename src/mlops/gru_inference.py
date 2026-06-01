import os
import json
import logging
import sys
import numpy as np
import torch
from collections import deque
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer

from model_1 import TGCN 

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.time_encoding import encode_time_features, extract_time_components
from graph import edge_index, edge_weight, node_list, node_to_idx

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [MLOps GRU] - %(message)s')

KAFKA_BROKER = 'kafka:19092'
CONSUME_TOPIC = 'traffic.feature'
PRODUCE_TOPIC = 'traffic.prediction'

# ============================================
# CẤU HÌNH ĐỒNG BỘ VỚI QUÁ TRÌNH TRAIN
# ============================================
WINDOW_SIZE = 3
NUM_NODES = len(node_list)
NUM_FEATURES = 13

# Mảng gồm 9 giá trị: [Speed, TravelTime, FreeSpeed, FreeTravelTime, Incident, Temp, Hum, Wind, Vis]
FEATURE_MEANS = np.array([19.564741, 799.571594, 30.122337, 500.475311, 0.0, 31.281614, 56.04697, 3.399138, 8.995898], dtype=np.float32)
FEATURE_STDS = np.array([6.569058, 825.169006, 7.928298, 514.192749, 1e-05, 3.098041, 3.575646, 0.344204, 0.579564], dtype=np.float32)
# Persistent buffer path
BUFFER_STATE_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / 'buffer_state'
BUFFER_STATE_DIR.mkdir(exist_ok=True)
BUFFER_STATE_FILE = BUFFER_STATE_DIR / 'window_buffer.npz'

def load_model_with_hot_swap(device):
    model = TGCN(num_node_features=NUM_FEATURES, hidden_dim=32, num_horizons=3)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    latest_ckpt_path = os.path.join(current_dir, 'checkpoints', 'gru_model_latest.pt')
    prod_model_path = os.path.join(current_dir, 'gru_model_v1.pt')
    
    try:
        if os.path.exists(latest_ckpt_path):
            model.load_state_dict(torch.load(latest_ckpt_path, map_location=device))
            model.to(device)
            model.eval()
            logging.info(f"Loaded fine-tuned checkpoint: {latest_ckpt_path}")
            return model, latest_ckpt_path
    except Exception as e:
        logging.warning(f"Failed to load fine-tuned checkpoint: {e}")
    
    try:
        model.load_state_dict(torch.load(prod_model_path, map_location=device))
        model.to(device)
        model.eval()
        logging.info(f"Loaded production model: {prod_model_path}")
        return model, prod_model_path
    except Exception as e:
        logging.error(f"Failed to load model: {e}")
        raise

def build_congestion_label(pred_speed: float, free_flow_speed: float) -> str:
    if free_flow_speed is None or free_flow_speed <= 0:
        return "Không rõ"
    ratio = pred_speed / free_flow_speed
    if ratio >= 0.8: return "Thông thoáng"
    if ratio >= 0.5: return "Bình thường"
    if ratio >= 0.25: return "Chậm"
    return "Tắc nghẽn"

def prepare_model_features(feature_data: dict) -> dict:
    location = feature_data.get('location_name')
    if location == 'Vong_Xoay_Lang_Cha_Ca':
        location = 'Lang_Cha_Ca'
    if location not in node_list: return None
    
    # Lấy Raw values (KHÔNG CHUẨN HÓA Ở ĐÂY, SẼ CHUẨN HÓA BẰNG Z-SCORE DƯỚI MAIN)
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
    
    prediction_time = feature_data.get('time')
    time_features = [0.0, 0.0, 0.0, 0.0]
    
    if prediction_time:
        try:
            hour, minute, second, weekday = extract_time_components(prediction_time)
            time_features = encode_time_features(hour, minute, second, weekday)
        except Exception:
            pass
    
    feature_vector = raw_features + time_features
    node_idx = node_to_idx[location]
    
    return {
        'feature_vector': np.array(feature_vector, dtype=np.float32),
        'location': location,
        'node_idx': node_idx,
        'prediction_time': prediction_time,
        'current_speed': raw_features[0],
        'free_flow_speed': raw_features[2]
    }

def save_buffer_state(window_buffer, current_graph_snapshot, current_timestamp):
    try:
        np.savez_compressed(
            BUFFER_STATE_FILE,
            window_buffer=np.array(list(window_buffer)),
            current_graph_snapshot=current_graph_snapshot,
            current_timestamp=np.array([current_timestamp], dtype=object)
        )
    except Exception:
        pass

def load_buffer_state():
    try:
        if BUFFER_STATE_FILE.exists():
            data = np.load(BUFFER_STATE_FILE, allow_pickle=True)
            window_buffer = deque([np.array(snap) for snap in data['window_buffer']], maxlen=WINDOW_SIZE)
            return window_buffer, data['current_graph_snapshot'], data['current_timestamp'][0]
    except Exception:
        pass
    return deque(maxlen=WINDOW_SIZE), np.zeros((NUM_NODES, NUM_FEATURES), dtype=np.float32), None

def main():
    logging.info("Đang khởi tạo Hệ thống MLOps TGCN Inference...")
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    try:
        model, model_path = load_model_with_hot_swap(device)
    except Exception:
        return

    window_buffer, current_graph_snapshot, current_timestamp = load_buffer_state()
    free_flow_cache = {}
    ready_for_inference = len(window_buffer) == WINDOW_SIZE
    
    consumer = KafkaConsumer(
        CONSUME_TOPIC, bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else {},
        auto_offset_reset='latest',   # Chỉ đọc message MỚI, không replay lại cũ
        max_poll_records=32,
        group_id='gru_inference_group_v2'  # Group mới để bỏ offset cũ bị stuck
    )
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    
    for message in consumer:
        model_input = prepare_model_features(message.value)
        if model_input is None: continue
        
        msg_time = model_input['prediction_time']
        node_idx = model_input['node_idx']
        free_flow_cache[node_idx] = model_input['free_flow_speed']

        if current_timestamp is not None and msg_time != current_timestamp:
            window_buffer.append(current_graph_snapshot.copy())
            save_buffer_state(window_buffer, current_graph_snapshot, current_timestamp)
            
            if len(window_buffer) == WINDOW_SIZE:
                if not ready_for_inference: ready_for_inference = True
                
                # Tensor Shape: [WINDOW_SIZE, NUM_NODES, NUM_FEATURES]
                x_numpy = np.array(list(window_buffer), dtype=np.float32)
                
                # CHUẨN HÓA ĐÚNG THEO DATA LOADER (Chỉ chuẩn hóa 9 features đầu)
                x_numpy[:, :, :9] = (x_numpy[:, :, :9] - FEATURE_MEANS) / FEATURE_STDS
                
                # Thêm chiều Batch -> Shape: [1, WINDOW_SIZE, NUM_NODES, NUM_FEATURES]
                x_tensor = torch.tensor(x_numpy).unsqueeze(0).to(device)
                edge_idx_t = edge_index.to(device)
                edge_w_t = edge_weight.to(device)
                
                with torch.no_grad():
                    pred = model(x_tensor, edge_idx_t, edge_w_t) # [1, NUM_NODES, 3]
                    pred = pred.squeeze(0).cpu().numpy() # [NUM_NODES, 3]
                    
                # DE-NORMALIZE VỚI MEAN/STD CỦA CỘT TỐC ĐỘ (Vị trí số 0)
                pred = pred * FEATURE_STDS[0] + FEATURE_MEANS[0]
                
                for i, node_name in enumerate(node_list):
                    p_5 = max(0.0, float(pred[i, 0]))
                    p_10 = max(0.0, float(pred[i, 1]))
                    p_15 = max(0.0, float(pred[i, 2]))
                    ffs = free_flow_cache.get(i, 40.0)
                    
                    producer.send(PRODUCE_TOPIC, key=node_name.encode('utf-8'), value={
                        "prediction_timestamp": current_timestamp,
                        "location_name": node_name,
                        "predicted_speed": round(p_5, 2),
                        "predicted_congestion_label": build_congestion_label(p_5, ffs),
                        "predicted_speed_5m": round(p_5, 2), "predicted_speed_10m": round(p_10, 2), "predicted_speed_15m": round(p_15, 2),
                        "predicted_congestion_label_5m": build_congestion_label(p_5, ffs),
                        "predicted_congestion_label_10m": build_congestion_label(p_10, ffs),
                        "predicted_congestion_label_15m": build_congestion_label(p_15, ffs),
                        "model_version": "gru_v1.0.0_production"
                    })
                logging.info(f"Đã Inference và Push kết quả 29 nút cho thời điểm: {current_timestamp}")
        
        current_timestamp = msg_time
        current_graph_snapshot[node_idx] = model_input['feature_vector']

if __name__ == "__main__":
    main()