import os
import requests
import json
import logging
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = 'kafka:9092' 
TOPIC = 'raw_hcm_camera'

# Thư mục lưu ảnh tĩnh (Sẽ map vào Data Lake khi dùng Docker)
IMAGE_SAVE_DIR = "/opt/airflow/data/raw_cameras"

# Danh sách Camera (Bạn cần lấy ID thật từ tab Network như trong hình của bạn)
# Ví dụ: http://giaothong.hochiminhcity.gov.vn/render/CameraHandler.ashx?id=516b...
CAMERAS = {
    "Nga_Tu_Hang_Xanh": "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx?id=5d9ddd49766c880017188c94&bg=black&w=300&h=230",
    "Vong_Xoay_Lang_Cha_Ca": "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx?id=5d8cdbdc766c88001718896a&bg=black&w=300&h=230&t=1772643354878",
    "Cau_Kenh_Te": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=63ae7669bfd3d90017e8f0d9&t=1772643466929",
    "Nga_Tu_Thu_Duc": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=56df81d8c062921100c143de&t=1772643506191"
}

def setup_directory():
    """Tạo thư mục lưu ảnh nếu chưa có"""
    if not os.path.exists(IMAGE_SAVE_DIR):
        os.makedirs(IMAGE_SAVE_DIR)

def main():
    setup_directory()
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=3
        )
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://giaothong.hochiminhcity.gov.vn/map.aspx"
        }
        
        timestamp_now = datetime.utcnow()
        timestamp_str = timestamp_now.strftime("%Y%m%d_%H%M%S")
        iso_time = timestamp_now.isoformat()

        for cam_name, cam_id in CAMERAS.items():
            # URL gọi API chụp ảnh tĩnh của Sở GTVT
            url = f"https://giaothong.hochiminhcity.gov.vn/render/CameraHandler.ashx?id={cam_id}"
            
            try:
                # Tải ảnh về
                response = requests.get(url, headers=headers, stream=True, timeout=15)
                
                if response.status_code == 200:
                    # 1. Lưu ảnh vào Data Lake (Ổ cứng)
                    image_filename = f"{cam_name}_{timestamp_str}.jpg"
                    image_filepath = os.path.join(IMAGE_SAVE_DIR, image_filename)
                    
                    with open(image_filepath, 'wb') as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                            
                    # 2. Đẩy Metadata vào Kafka
                    payload = {
                        "ingestion_timestamp": iso_time,
                        "location_name": cam_name,
                        "data_type": "camera_snapshot",
                        "image_path": image_filepath, # Spark sẽ đọc field này để tìm ảnh
                        "file_size_bytes": os.path.getsize(image_filepath)
                    }
                    
                    producer.send(TOPIC, value=payload)
                    logging.info(f"[Camera] Đã tải ảnh và đẩy metadata cho: {cam_name}")
                else:
                    logging.error(f"[Camera] Lỗi {response.status_code} tại {cam_name}")
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"[Camera] Lỗi mạng khi tải ảnh {cam_name}: {e}")

        producer.flush()
        producer.close()
        logging.info("[Camera] Hoàn tất tiến trình Ingestion.")
        
    except Exception as e:
        logging.error(f"Lỗi Kafka Broker: {e}")

if __name__ == "__main__":
    main()