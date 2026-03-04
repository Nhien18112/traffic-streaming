import os
import requests
import json
import logging
from datetime import datetime
from kafka import KafkaProducer

# 1. Cấu hình Logging để theo dõi tiến trình trong Airflow
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 2. Cấu hình Kafka
# LƯU Ý: Vì script này sẽ chạy BÊN TRONG container Airflow, nó phải gọi Kafka qua tên service 'kafka'
# Nếu bạn muốn chạy test script này trực tiếp trên máy host (bằng lệnh python bình thường), hãy tạm đổi thành 'localhost:9092'
KAFKA_BROKER = 'kafka:9092' 
TOPIC = 'raw_tomtom_traffic'

# 3. Lấy API Key một cách bảo mật từ file .env
API_KEY = os.getenv("TOMTOM_API_KEY")

# 4. Danh sách 4 giao lộ trọng điểm tại TP.HCM
LOCATIONS = {
    "Nga_Tu_Hang_Xanh": {"lat": "10.8015", "lon": "106.7111"},
    "Vong_Xoay_Lang_Cha_Ca": {"lat": "10.8023", "lon": "106.6603"},
    "Cau_Kenh_Te": {"lat": "10.7523", "lon": "106.6972"},
    "Nga_Tu_Thu_Duc": {"lat": "10.8504", "lon": "106.7716"}
}

def main():
    # Kiểm tra xem API Key đã được load thành công chưa
    if not API_KEY:
        logging.error("LỖI BẢO MẬT: Không tìm thấy TOMTOM_API_KEY. Vui lòng kiểm tra lại file .env!")
        return # Dừng chương trình ngay lập tức

    try:
        # Khởi tạo Kafka Producer với cơ chế tự động thử lại (retries) nếu mạng chập chờn
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=3 
        )
        
        # Lặp qua từng địa điểm để lấy dữ liệu
        for loc_name, coords in LOCATIONS.items():
            lat, lon = coords["lat"], coords["lon"]
            
            # API endpoint của TomTom Traffic Flow
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={API_KEY}&point={lat},{lon}"
            
            try:
                # Cài đặt timeout=10s để code không bị treo vĩnh viễn nếu TomTom sập
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Bước Data Enrichment (Làm giàu dữ liệu): 
                    # Gắn thêm timestamp và tên địa điểm vào cục JSON trước khi đẩy đi
                    # Điều này CỰC KỲ QUAN TRỌNG để Spark Streaming có thể join dữ liệu sau này
                    enriched_payload = {
                        "ingestion_timestamp": datetime.utcnow().isoformat(),
                        "location_name": loc_name,
                        "latitude": lat,
                        "longitude": lon,
                        "tomtom_data": data # Bọc toàn bộ data gốc của TomTom vào field này
                    }
                    
                    # Đẩy dữ liệu vào Kafka Topic
                    producer.send(TOPIC, value=enriched_payload)
                    logging.info(f"[TomTom] Đã lấy và đẩy thành công dữ liệu cho: {loc_name}")
                    
                elif response.status_code == 403:
                    logging.error(f"[TomTom] Lỗi 403: API Key không hợp lệ hoặc hết hạn tại {loc_name}.")
                else:
                    logging.error(f"[TomTom] Lỗi {response.status_code} tại {loc_name}: {response.text}")
                    
            except requests.exceptions.Timeout:
                logging.error(f"[TomTom] Lỗi Timeout: Server TomTom không phản hồi tại {loc_name}.")
            except requests.exceptions.RequestException as e:
                logging.error(f"[TomTom] Lỗi mạng khi gọi {loc_name}: {e}")

        # Bắt buộc phải xả buffer (flush) để đảm bảo toàn bộ message đã rời khỏi script và bay vào Kafka
        producer.flush() 
        producer.close()
        logging.info("[TomTom] Hoàn tất tiến trình Ingestion chu kỳ này.")
        
    except Exception as e:
        logging.error(f"Lỗi hệ thống hoặc không thể kết nối tới Kafka Broker: {e}")

if __name__ == "__main__":
    main()