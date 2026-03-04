import os
import requests
import json
import logging
from datetime import datetime
from kafka import KafkaProducer

# 1. Cấu hình Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 2. Cấu hình Kafka (Dùng 'kafka:9092' khi chạy trong Docker/Airflow)
KAFKA_BROKER = 'kafka:9092' 
TOPIC = 'raw_weather_data'

# 3. Đọc API Key bảo mật từ file .env
API_KEY = os.getenv("WEATHER_API_KEY")

# 4. Danh sách 4 tọa độ chuẩn (Khớp 100% với file TomTom)
LOCATIONS = {
    "Nga_Tu_Hang_Xanh": {"lat": "10.8015", "lon": "106.7111"},
    "Vong_Xoay_Lang_Cha_Ca": {"lat": "10.8023", "lon": "106.6603"},
    "Cau_Kenh_Te": {"lat": "10.7523", "lon": "106.6972"},
    "Nga_Tu_Thu_Duc": {"lat": "10.8504", "lon": "106.7716"}
}

def main():
    if not API_KEY:
        logging.error("LỖI BẢO MẬT: Không tìm thấy WEATHER_API_KEY trong file .env!")
        return

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=3
        )
        
        for loc_name, coords in LOCATIONS.items():
            lat, lon = coords["lat"], coords["lon"]
            
            # API endpoint lấy Current Weather Data
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
            
            try:
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Data Enrichment: Gắn nhãn để dễ JOIN với dữ liệu giao thông
                    enriched_payload = {
                        "ingestion_timestamp": datetime.utcnow().isoformat(),
                        "location_name": loc_name,
                        "latitude": lat,
                        "longitude": lon,
                        "temperature": data.get("main", {}).get("temp"),     # Trích xuất nhanh nhiệt độ (C)
                        "weather_main": data.get("weather", [{}])[0].get("main"), # Trích xuất nhanh trạng thái (Rain, Clouds, Clear...)
                        "weather_data": data # Vẫn giữ lại toàn bộ JSON gốc phòng khi cần tính năng khác
                    }
                    
                    producer.send(TOPIC, value=enriched_payload)
                    logging.info(f"[Weather] Đã lấy và đẩy thành công dữ liệu cho: {loc_name}")
                    
                elif response.status_code == 401:
                    logging.error(f"[Weather] Lỗi 401: API Key không hợp lệ hoặc chưa được kích hoạt.")
                else:
                    logging.error(f"[Weather] Lỗi {response.status_code} tại {loc_name}: {response.text}")
                    
            except requests.exceptions.Timeout:
                logging.error(f"[Weather] Lỗi Timeout: Server OpenWeather không phản hồi tại {loc_name}.")
            except requests.exceptions.RequestException as e:
                logging.error(f"[Weather] Lỗi mạng khi gọi {loc_name}: {e}")

        producer.flush()
        producer.close()
        logging.info("[Weather] Hoàn tất tiến trình Ingestion chu kỳ này.")
        
    except Exception as e:
        logging.error(f"Lỗi hệ thống hoặc không thể kết nối tới Kafka Broker: {e}")

if __name__ == "__main__":
    main()