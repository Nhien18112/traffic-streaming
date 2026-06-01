import os
import io
import json
import asyncio
import aiohttp
import logging
from datetime import datetime
from minio import Minio
from kafka import KafkaProducer

from config import LOCATIONS, CAMERAS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

KAFKA_BROKER = 'kafka:19092'
MINIO_ENDPOINT = "minio:9000"
MINIO_USER = "minioadmin"
MINIO_PASS = "minioadmin"

weather_api_key = os.getenv("WEATHER_API_KEY")
tomtom_api_key = os.getenv("TOMTOM_API_KEY")

def get_tomtom_keys():
    raw_keys = os.getenv("TOMTOM_API_KEYS", "")
    keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
    if keys:
        return keys
    if tomtom_api_key:
        return [tomtom_api_key]
    return []

def mask_key(value):
    if not value:
        return "***"
    if len(value) <= 6:
        return "***"
    return f"{value[:3]}...{value[-3:]}"

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
except Exception as e:
    logging.error(f"Chưa kết nối được Kafka: {e}")
    producer = None

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_USER,
    secret_key=MINIO_PASS,
    secure=False
)


def validate_camera_config():
    missing_camera_locations = sorted(set(LOCATIONS.keys()) - set(CAMERAS.keys()))
    invalid_camera_urls = [
        name for name, url in CAMERAS.items()
        if (not url) or url.endswith("?")
    ]

    if missing_camera_locations:
        logging.warning(
            "Camera config missing for locations: %s",
            ", ".join(missing_camera_locations),
        )

    if invalid_camera_urls:
        logging.warning(
            "Camera URL invalid/placeholder for locations: %s",
            ", ".join(sorted(invalid_camera_urls)),
        )

    if not missing_camera_locations and not invalid_camera_urls:
        logging.info("Camera config check passed: all locations have valid camera URLs.")

async def dump_raw_to_minio(data_bytes, folder, loc_name, timestamp_str, content_type):
    file_name = f"{folder}/{loc_name}_{timestamp_str}"
    try:
        minio_client.put_object(
            bucket_name="raw-data-lake",
            object_name=file_name,
            data=io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type=content_type
        )
        return f"http://{MINIO_ENDPOINT}/raw-data-lake/{file_name}"
    except Exception as e:
        logging.error(f"Lỗi khối ghi MinIO tại {file_name}: {e}")
        return None

async def poll_cameras():
    while True:
        if not producer:
            await asyncio.sleep(5)
            continue
            
        logging.info("Bắt đầu chu kỳ kéo Ảnh Camera (30s/lần)...")
        session_timeout = aiohttp.ClientTimeout(total=15)
        iso_time = datetime.utcnow().isoformat()
        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            session.headers.update({
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://giaothong.hochiminhcity.gov.vn/map.aspx"
            })
            for cam_name, cam_url in CAMERAS.items():
                if not cam_url or cam_url.endswith("?"):
                    continue
                try:
                    async with session.get(cam_url) as response:
                        if response.status == 200:
                            image_bytes = await response.read()
                            minio_url = await dump_raw_to_minio(
                                image_bytes, "camera_raw", cam_name, f"{timestamp_str}.jpg", "image/jpeg"
                            )
                            
                            if minio_url:
                                payload = {
                                    "ingestion_timestamp": iso_time,
                                    "location_name": cam_name,
                                    "image_url": minio_url
                                }
                                producer.send('camera.raw', key=cam_name.encode('utf-8'), value=payload)
                except Exception as e:
                    logging.warning(f"Timeout/Error khi lấy camera {cam_name}: {e}")
                    
        producer.flush()
        await asyncio.sleep(60) 

async def poll_weather():
    while True:
        if not weather_api_key or not producer:
            await asyncio.sleep(60)
            continue
            
        logging.info("Bắt đầu chu kỳ kéo Data Thời tiết (5 phút/lần)...")
        session_timeout = aiohttp.ClientTimeout(total=10)
        iso_time = datetime.utcnow().isoformat()
        
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            for loc_name, coords in LOCATIONS.items():
                lat, lon = coords["lat"], coords["lon"]
                url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={weather_api_key}&units=metric"
                
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            payload = {
                                "ingestion_timestamp": iso_time,
                                "location_name": loc_name,
                                "latitude": lat,
                                "longitude": lon,
                                "temperature": data.get("main", {}).get("temp"),
                                "humidity": data.get("main", {}).get("humidity"),
                                "weather_main": data.get("weather", [{}])[0].get("main"),
                                "wind_speed": data.get("wind", {}).get("speed"),
                                "visibility": data.get("visibility")
                            }
                            producer.send('weather.raw', key=loc_name.encode('utf-8'), value=payload)
                except Exception as e:
                    logging.warning(f"Lỗi API Thời tiết {loc_name}: {e}")
                    
        producer.flush()
        await asyncio.sleep(60) 

async def poll_tomtom():
    key_index = 0
    while True:
        tomtom_keys = get_tomtom_keys()
        if not tomtom_keys or not producer:
            await asyncio.sleep(60)
            continue

        if key_index >= len(tomtom_keys):
            key_index = 0
            
        logging.info("Bắt đầu chu kỳ kéo Data TomTom (5 phút/lần)...")
        session_timeout = aiohttp.ClientTimeout(total=15)
        iso_time = datetime.utcnow().isoformat()
        
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            for loc_name, coords in LOCATIONS.items():
                lat, lon = coords["lat"], coords["lon"]

                attempts = 0
                sent_payload = False
                while attempts < len(tomtom_keys):
                    current_key = tomtom_keys[key_index]
                    url_flow = (
                        "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
                        f"?key={current_key}&point={lat},{lon}"
                    )

                    min_lon, min_lat = float(lon) - 0.005, float(lat) - 0.005
                    max_lon, max_lat = float(lon) + 0.005, float(lat) + 0.005
                    url_incident = (
                        "https://api.tomtom.com/traffic/services/5/incidentDetails"
                        f"?key={current_key}&bbox={min_lon},{min_lat},{max_lon},{max_lat}"
                        "&fields={incidents{properties{iconCategory,delay}}}"
                    )

                    try:
                        flow_data = None
                        flow_status = None
                        async with session.get(url_flow) as respF:
                            flow_status = respF.status
                            if respF.status == 200:
                                flow_data = await respF.json()

                        incident_count = 0
                        incident_status = None
                        async with session.get(url_incident) as respI:
                            incident_status = respI.status
                            if respI.status == 200:
                                inc_data = await respI.json()
                                incidents = inc_data.get("incidents", [])
                                incident_count = len(incidents)

                        if (flow_status and 400 <= flow_status < 500) or (
                            incident_status and 400 <= incident_status < 500
                        ):
                            logging.warning(
                                "TomTom key %s bi loi 4xx, chuyen sang key tiep theo.",
                                mask_key(current_key),
                            )
                            key_index = (key_index + 1) % len(tomtom_keys)
                            attempts += 1
                            continue

                        if flow_data:
                            payload = {
                                "ingestion_timestamp": iso_time,
                                "location_name": loc_name,
                                "latitude": lat,
                                "longitude": lon,
                                "tomtom_data": flow_data,
                                "incident_count": incident_count
                            }
                            producer.send('traffic.raw', key=loc_name.encode('utf-8'), value=payload)
                            sent_payload = True
                        break
                    except Exception as e:
                        logging.warning(f"Lỗi TomTom API tại {loc_name}: {e}")
                        break

                if not sent_payload and attempts >= len(tomtom_keys):
                    logging.error(
                        "Tat ca TomTom keys deu bi loi 4xx. Dung polling TomTom."
                    )
                    return
                    
        producer.flush()
        await asyncio.sleep(60)

async def check_minio_bucket():
    while True:
        try:
            if not minio_client.bucket_exists("raw-data-lake"):
                minio_client.make_bucket("raw-data-lake")
            break
        except Exception:
            logging.info("Đang đợi MinIO khởi động...")
            await asyncio.sleep(3)

async def main():
    await check_minio_bucket()
    validate_camera_config()
    logging.info("===== KHỞI ĐỘNG HỆ THỐNG ASYNC POLLING =====")
    await asyncio.gather(
        poll_cameras(),
        poll_weather(),
        poll_tomtom()
    )

if __name__ == "__main__":
    asyncio.run(main())
