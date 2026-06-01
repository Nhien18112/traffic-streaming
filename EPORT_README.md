# HCMC Traffic Intelligence - Bao cao ky thuat (E2E)

## 1. Muc tieu he thong
He thong thu thap du lieu giao thong HCMC theo thoi gian thuc, ket hop anh camera, du lieu giao thong TomTom, va thoi tiet OpenWeather. Du lieu duoc xu ly streaming, luu tru vao DB, suy luan AI de dem phuong tien, va cung cap API/bao cao tren Web UI. He thong ho tro ca du doan xu huong (MLOps GRU/TGCN) va giamsat chat luong pipeline.

## 2. Kien truc tong quan (E2E)
- Nguon du lieu: Camera giao thong, TomTom Traffic API, OpenWeather API.
- Ingestion (async polling): Keo du lieu vao Kafka va MinIO.
- AI Worker: Doc camera.raw, chay YOLOv8 dem xe, day camera.processed.
- Spark Streaming: Join traffic + weather, ghi DB, ghi MinIO parquet, day feature cho ML.
- MLOps GRU: Inference du doan toc do, fine-tune lien tuc tu stream.
- Prediction Sink: Ghi ket qua du doan vao DB.
- Serving: FastAPI + Redis cache, Web UI (Vite/React).
- Observability: Prometheus + Grafana.

## 3. Mermaid - Dataflow E2E
```mermaid
flowchart LR
  CAM[Camera Snapshots] -->|HTTP fetch| ING[Ingestion Service\nasync polling]
  TOM[TomTom Traffic API] -->|HTTP fetch| ING
  WEA[OpenWeather API] -->|HTTP fetch| ING

  ING -->|camera.raw| KAFKA[(Kafka)]
  ING -->|traffic.raw| KAFKA
  ING -->|weather.raw| KAFKA
  ING -->|raw images| MINIO[(MinIO Data Lake)]

  KAFKA -->|camera.raw| AI[AI Worker\nYOLOv8]
  MINIO -->|read raw images| AI
  AI -->|camera.processed| KAFKA

  KAFKA -->|traffic.raw + weather.raw| SPARK[Spark Streaming]
  KAFKA -->|camera.processed| SPARK
  SPARK -->|upsert| PG[(PostgreSQL)]
  SPARK -->|parquet| MINIO
  SPARK -->|traffic.feature| KAFKA

  KAFKA -->|traffic.feature| GRUINF[GRU/TGCN Inference]
  GRUINF -->|traffic.prediction| KAFKA
  KAFKA -->|traffic.feature| GRUFT[GRU Fine-tuner]
  GRUFT -->|model.finetuned| KAFKA
  KAFKA -->|traffic.prediction| SINK[Prediction Sink]
  SINK -->|update| PG

  PG --> API[FastAPI Serving]
  REDIS[(Redis Cache)] <--> API
  API --> UI[Web UI]\nVite/React

  API --> PROM[Prometheus]
  PROM --> GRAF[Grafana]

  NOTE[Kafka chi chua metadata/URL, khong chua byte anh]:::note
  NOTE -.-> KAFKA

  classDef note fill:#fff7d6,stroke:#e0b400,color:#5b4b00
```

## 4. Cac thanh phan chinh va cong nghe
### 4.1 Ingestion (src/ingestion)
- main_polling.py: Keo anh camera, TomTom traffic, OpenWeather. Day Kafka topics va luu raw image vao MinIO.
- config.py: Danh sach toa do va camera URL.
- Cong nghe: aiohttp, KafkaProducer, MinIO SDK.

### 4.2 AI Worker (src/ai_worker)
- main.py: Doc camera.raw, tai anh tu MinIO, chay YOLOv8 dem xe, day camera.processed.
- Cong nghe: Ultralytics YOLOv8, OpenCV, Kafka.

### 4.3 Streaming + CQRS (src/streaming)
- spark_processor.py: Doc traffic.raw + weather.raw + camera.processed. 
  - Ghi realtime_traffic_weather va realtime_camera (PostgreSQL).
  - Ghi parquet vao MinIO (feature_lake).
  - Day traffic.feature cho ML.
  - Ghi DLQ va pipeline_quality_metrics khi bat loi data contract.
- prediction_sink.py: Doc traffic.prediction, update du doan vao realtime_traffic_weather.
- Cong nghe: PySpark Structured Streaming, PostgreSQL, Kafka, MinIO.

### 4.4 MLOps (src/mlops)
- gru_inference.py: Inference TGCN/GRU tu traffic.feature, du doan 5/10/15 phut.
- gru_finetuner.py: Fine-tune lien tuc tu dong, cap nhat checkpoint va promote model.
- graph.py: Dinh nghia do thi giao thong, edge weight.
- Cong nghe: PyTorch, Torch Geometric, Kafka.

### 4.5 Serving (src/serving)
- main.py: FastAPI endpoints, Redis cache, Prometheus metrics.
- models.py: Pydantic models cho response.
- Cong nghe: FastAPI, Redis, PostgreSQL, Prometheus.

### 4.6 Web UI (web-ui)
- Vite + React. Goi API /api/*, bieu do va dashboard.
- Nginx: Proxy /api -> api-serving.

### 4.7 Observability
- Prometheus: thu metrics FastAPI.
- Grafana: dashboard he thong.

## 5. Kafka Topics
- camera.raw: anh camera (URL MinIO).
- camera.processed: so luong xe sau YOLO.
- traffic.raw: du lieu TomTom.
- weather.raw: du lieu OpenWeather.
- traffic.feature: feature cho GRU/TGCN.
- traffic.prediction: du doan toc do.
- traffic.dlq: ban ghi loi data contract.
- model.finetuned: su kien thong bao fine-tune model (bao gom best_loss, so buoc, trang thai promote).

Vi du payload cho model.finetuned:
```json
{
  "timestamp": "2026-04-28T10:25:42.123456",
  "event": "model_finetuned",
  "steps": 12,
  "best_loss": 0.004321,
  "promoted_to_production": true
}
```

## 6. Database schema (schema.sql)
- realtime_traffic_weather: du lieu traffic + weather + prediction.
- realtime_camera: du lieu dem xe.
- pipeline_quality_metrics: thong ke chat luong pipeline.
- traffic_view: view join traffic + camera theo cua so +- 2 phut.

## 7. Cong nghe va tac dung
- Kafka: Message bus cho streaming, tach ingestion va xu ly.
- Spark Streaming: Xu ly luong du lieu realtime, ghi DB va MinIO.
- MinIO: Data lake cho raw image va parquet.
- PostgreSQL: Serving storage va truy van nhanh.
- Redis: Cache cho API, giam load DB.
- FastAPI: Serving REST API.
- YOLOv8: Dem xe tu anh camera.
- PyTorch + TGCN/GRU: Du doan toc do giao thong.
- Prometheus + Grafana: Giamsat he thong.
- Docker Compose: Orchestrate dich vu.

## 8. Cau hinh va chay he thong
1. Tao .env (vi du):
```
TOMTOM_API_KEYS=key1,key2,key3
# TOMTOM_API_KEY=single_key_fallback
WEATHER_API_KEY=your_openweather_api_key
POSTGRES_URL=postgresql://traffic_user:traffic_pass@postgres:5432/trafficdb
POSTGRES_JDBC_URL=jdbc:postgresql://postgres:5432/trafficdb
POSTGRES_USER=traffic_user
POSTGRES_PASSWORD=traffic_pass
```

2. Chay dich vu:
```
docker compose up -d --build
```

3. Cac dia chi chinh:
- Web UI: http://localhost:3000
- FastAPI: http://localhost:8000/docs
- Kafka UI: http://localhost:8081
- MinIO: http://localhost:9001
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3001

LOGS:
Follow both: docker compose logs -f spark-processor polling-services
Only Spark: docker compose logs -f spark-processor
Only polling: docker compose logs -f polling-services
With last 200 lines: docker compose logs -f --tail=200 spark-processor
## 9. Luong du lieu chi tiet (tung buoc)
1. Ingestion keo anh + traffic + weather.
2. Anh -> MinIO, event -> camera.raw.
3. AI worker doc camera.raw, tai anh tu MinIO, chay YOLO, day camera.processed.
4. Spark join traffic.raw + weather.raw, ghi realtime_traffic_weather.
5. Spark doc camera.processed, ghi realtime_camera.
6. Spark ghi parquet -> MinIO (feature lake).
7. Spark day traffic.feature -> GRU inference.
8. GRU inference day traffic.prediction.
9. Prediction sink update DB.
10. FastAPI doc traffic_view, cache Redis, phuc vu Web UI.

## 10. KPI/Chat luong va truy vet
- pipeline_quality_metrics luu so luong record valid/invalid per epoch.
- traffic.dlq giu du lieu vi pham data contract.
- FastAPI metrics xuat cho Prometheus.

## 11. Mo rong va tuong lai
- Them sensor IoT (speed/loop) vao ingestion.
- Auto-scale AI worker theo load Kafka.
- Model registry va A/B testing cho GRU/TGCN.
- Data labeling pipeline cho camera.

---

## 12. Ky thuat xu ly chat luong du lieu (Data Quality)

He thong ap dung nhieu lop xu ly de dam bao do tin cay cua du lieu truoc khi dua vao mo hinh va serving.

### 12.1 Xu ly Null / Thieu gia tri

**Tang Ingestion (main_polling.py):**
- Camera URL: Kiem tra URL hop le truoc khi fetch (`if not cam_url or cam_url.endswith("?")`). URL khong hop le bi bo qua, khong day vao Kafka.
- HTTP timeout: Moi request co timeout cung (10-15s). Neu timeout/loi mang thi ghi WARNING va tiep tuc loc tiep theo - tranh block toan bo pipeline.
- TomTom API key rotation: Khi mot key bi loi 4xx (het quota), tu dong chuyen sang key tiep theo. Neu het tat ca key thi dung han, ghi ERROR.
- Weather fields: Su dung `.get()` voi fallback (`data.get("main", {}).get("temp")`) - tra ve None thay vi exception khi truong khong ton tai.

**Tang Spark Streaming (spark_processor.py):**
- Data Contract validation: Truoc khi xu ly, kiem tra cung bac bat buoc:
  ```
  time IS NOT NULL AND location_name IS NOT NULL
  AND currentSpeed IS NOT NULL AND freeFlowSpeed IS NOT NULL
  ```
- Ban ghi thieu truong bat buoc -> duoc tach thanh `invalid_df`, khong duoc ghi vao DB.
- Weather join dung LEFT JOIN voi watermark 5 phut: neu khong co ban ghi weather khop -> cac truong weather la NULL nhung ban ghi traffic van duoc ghi (khong mat du lieu chinh).
- Camera join trong `traffic_view` dung LEFT JOIN LATERAL voi cua so ±2 phut: neu camera khong co du lieu -> tra ve zero vehicle counts, khong loi.

**Tang GRU Inference (gru_inference.py):**
- Truong `current_speed` / `free_flow_speed` thieu -> fallback ve 0.0 (`feature_data.get('current_speed', 0.0)`).
- `time_features` thieu / parse loi -> fallback ve vector `[0.0, 0.0, 0.0, 0.0]`.
- `free_flow_speed` thieu trong cache -> su dung gia tri mac dinh 40.0 km/h.
- Du doan am (phi ly ve vat ly) -> ep ve 0: `p_5 = max(0.0, float(pred[i, 0]))`.

**Tang Prediction Sink (prediction_sink.py):**
- Update dung cua so thoi gian ±2 phut de match prediction voi ban ghi realtime.
- Neu khong tim thay ban ghi tuong ung -> bo qua update, ghi WARNING (tranh tao "orphan prediction row").

### 12.2 Xu ly Du lieu Nhieu (Noisy Data)

**Nhieu toc do bat thuong:**
- `speed_ratio = currentSpeed / freeFlowSpeed` duoc tinh de phat hien bat thuong (ratio > 1 co nghia la bi loi API).
- `congestion_label` duoc gan theo nguong ratio (>= 0.8: Thong thoang, >= 0.5: Binh thuong, ...) thay vi dung gia tri tuyet doi de giam anh huong cua nhieu tuyet doi.

**Nhieu anh camera:**
- YOLOv8 dung nguong confidence toi thieu `conf=0.15` - loai bo detection co do tin cay thap.
- Chi detect cac class xe co y nghia: Car (2), Motorcycle (1,3), Bus (5), Truck (7) - bo qua nguoi di bo va doi tuong khac.

**Nhieu chuoi thoi gian cho GRU:**
- Z-score normalization truoc khi dua vao model: `(x - FEATURE_MEANS) / FEATURE_STDS`
- Gradient clipping: `clip_grad_norm_(max_norm=1.0)` trong fine-tuner de tranh gradient exploding khi co nhieu du lieu bat thuong.
- Window buffer (WINDOW_SIZE=3) lay trung binh truot ngam dinh boi kien truc GRU: tranh hieu ung over-fitting vao mot diem nhieu duy nhat.

### 12.3 Xu ly Trung lap (Deduplication)

- Spark Streaming dung `dropDuplicates(["location_name", "time"])` truoc khi upsert.
- PostgreSQL upsert dung `ON CONFLICT (location_name, "time") DO UPDATE SET ...`: dam bao idempotency khi Spark replay tu checkpoint.
- Camera table cung ON CONFLICT tuong tu: `ON CONFLICT (location_name, "time") DO UPDATE SET ...`.

### 12.4 Dead Letter Queue (DLQ) - Xu ly Du lieu Vi pham Contract

- Ban ghi vi pham data contract (thieu truong bat buoc) duoc day vao Kafka topic `traffic.dlq` thay vi bi xoa im lang.
- DLQ payload ghi kem nguyen nhan loi, epoch_id, gia tri cua cac truong bi thieu de debug sau.
- `pipeline_quality_metrics` luu thong ke per-batch: total_count, valid_count, invalid_count, dropped_count, dlq_count.

### 12.5 Schema Validation va Watermark

- Spark dinh nghia schema cung cho tung Kafka topic (StructType) - tranh suy dien schema sai lam tu dirty data.
- Structured Streaming dung watermark 10 phut cho ca traffic va weather: du lieu den tre qua 10 phut bi loai khoi join window, khong lam sai state store.

### 12.6 Xu ly Outlier trong Serving

- `speed_ratio` duoc round 3 chu so thap phan khi tra API: `ROUND(AVG(speed_ratio)::numeric, 3)`.
- `traffic_view` join camera theo cua so thoi gian ±2 phut (LATERAL JOIN), tranh match sai gia tri camera cua gio truoc.
- Redis cache TTL = 300s: du lieu cu qua 5 phut se bi lam moi tu DB, tranh serving outlier cu.

---

## 13. Phan loai du an: ETL hay gi?

**Du an nay KHONG phai ETL thuan tuy ma la mot Streaming ELT/Lambda Architecture ket hop MLOps.**

| Tieu chi | ETL Truyen thong | Du an nay |
|---|---|---|
| Xu ly | Batch, dinh ky | **Real-time streaming** (30s/5min) |
| Mo hinh xu ly | Extract → Transform → Load | **Extract → Load raw → Transform (Spark)** |
| Luu tru trung gian | Staging DB | **Kafka (message bus) + MinIO (data lake)** |
| Transform | Tren ETL server | **Phan tan: Spark + AI Worker + GRU** |
| ML integration | Khong co | **Online inference + continuous fine-tuning** |
| Serving | Bao cao dinh ky | **REST API realtime + Redis cache** |

**Chinh xac hon, day la:**
- **Streaming ELT**: Du lieu duoc Load raw vao Kafka/MinIO truoc, sau do Transform bang Spark Structured Streaming.
- **Lambda Architecture**: Co ca hot path (Spark → PostgreSQL → FastAPI realtime) va cold path (MinIO parquet luu tru lau dai).
- **MLOps Pipeline tich hop**: GRU/TGCN inference va fine-tune chay song song voi pipeline du lieu, khong tach biet.
- **CQRS Pattern**: Tach biet luong ghi (Spark → DB) va luong doc (FastAPI + Redis → Web UI).

Nen goi day la: **"Real-time Streaming Data Pipeline voi tich hop MLOps"** hoac **"Streaming ELT + Lambda Architecture"**.

---

## 14. Mo hinh AI: T-GCN (GNN + GRU) cho du doan giao thong

### 14.1 Tai sao can GNN + GRU?

Du doan toc do giao thong tai mot nut giao khong chi phu thuoc vao lich su cua chinh nut do (chieu thoi gian) ma con phu thuoc vao cac nut lan can tren mang luoi duong (chieu khong gian). Mo hinh TGCN (Temporal Graph Convolutional Network) ket hop:

- **GCN (Graph Convolutional Network)** - thuc chat la GNN - de hoc quan he khong gian giua cac nut giao.
- **GRU (Gated Recurrent Unit)** - de hoc xu huong thoi gian cua chuoi toc do.

Mo hinh thuan GRU bo qua quan he giua cac nut: nut A ket viet tac dong deu len nut B lien ke, nhung thong tin do khong duoc chia se. TGCN giai quyet van de nay.

### 14.2 Do thi giao thong (graph.py)

**Tap nut (Nodes):** 29 nut giao tai TP.HCM (quan 10, Tan Binh, khu vuc duong 3/2, Cach Mang Thang 8,...).

**Tap canh (Edges):** 41 can duong dinh nghia thu cong dua tren ket noi vat ly thuc te. Canh hai chieu (is_two_way=True) tao ra 2 directed edges, canh mot chieu tao 1 directed edge. Tong so: ~80 directed edges.

**Trong so canh (Edge weights):**
```
distance = Haversine(lat1, lon1, lat2, lon2)   # khoang cach thuc dia ly (met)
edge_weight = 1 / (distance + eps)             # canh gan -> trong so lon
edge_weight = normalize(edge_weight)            # [0, 1] min-max
edge_weight = GCN_norm(edge_index, edge_weight) # chuẩn hóa Laplacian theo Kipf 2017
```

Nut lan can gan hon co anh huong lon hon (trong so cao hon) khi aggregate thong tin trong GCN.

### 14.3 Kien truc mo hinh TGCN (model_1.py)

```
Input:  x shape [batch, WINDOW_SIZE, NUM_NODES, NUM_FEATURES]
              = [1, 3, 29, 13]
```

**Buoc 1 - GCN (khong gian): Voi moi buoc thoi gian t:**
```python
xt = x[:, t, :, :]                  # [batch, 29, 13]
xt_flat = xt.reshape(-1, 13)        # [batch*29, 13]
out_gcn = GCNConv(13 -> 32)(xt_flat, edge_index, edge_weight)
out_gcn = out_gcn.view(batch, 29, 32)  # [batch, 29, 32]
```
GCNConv thuc hien aggregation: moi nut tong hop thong tin tu cac nut hang xom theo trong so canh.

**Buoc 2 - GRU (thoi gian): Hoc xu huong theo chuoi:**
```python
gcn_outputs = stack([out_t0, out_t1, out_t2])  # [batch, 3, 29, 32]
gru_input = permute + reshape                  # [batch*29, 3, 32]
gru_out, _ = GRU(layers=2, hidden=32)(gru_input)
last_hidden = gru_out[:, -1, :]               # [batch*29, 32]
```
GRU 2 lop hoc xu huong theo chieu thoi gian (WINDOW_SIZE=3 buoc ~ 15 phut lich su).

**Buoc 3 - Decoder (du doan 3 horizon):**
```python
pred = Linear(32->32) -> ReLU -> Linear(32->3)  # [batch*29, 3]
pred = pred.view(batch, 29, 3)                  # [batch, 29, 3]
```
Dau ra: toc do du doan tai t+5m, t+10m, t+15m cho ca 29 nut giao.

**De-normalize:**
```python
pred = pred * FEATURE_STDS[0] + FEATURE_MEANS[0]  # ve don vi km/h
```

### 14.4 Feature Engineering (13 features / nut)

| STT | Feature | Mo ta | Chuan hoa |
|---|---|---|---|
| 0 | current_speed | Toc do hien tai (km/h) | Z-score |
| 1 | currentTravelTime | Thoi gian di chuyen hien tai (s) | Z-score |
| 2 | free_flow_speed | Toc do luong thong tu do (km/h) | Z-score |
| 3 | freeFlowTravelTime | Thoi gian di chuyen luong thong tu do (s) | Z-score |
| 4 | incident_count | So su co giao thong trong bounding box | Z-score |
| 5 | temperature | Nhiet do (°C) | Z-score |
| 6 | humidity | Do am (%) | Z-score |
| 7 | wind_speed | Toc do gio (m/s) | Z-score |
| 8 | visibility | Tam nhin xa (m) | Z-score |
| 9 | sin(hour) | Ma hoa thoi gian trong ngay | Khong can |
| 10 | cos(hour) | Ma hoa thoi gian trong ngay | Khong can |
| 11 | sin(weekday) | Ma hoa ngay trong tuan | Khong can |
| 12 | cos(weekday) | Ma hoa ngay trong tuan | Khong can |

> Ma hoa sin/cos cho bien thoi gian de bieu dien tinh tuan hoan: 23h va 0h gan nhau, Thu 7 va Thu 2 xa nhau - khac voi encoding so nguyen thuan tuy.

### 14.5 Quy trinh Inference Realtime

```
traffic.feature (Kafka)
       |
       v
Window Buffer [t-10m, t-5m, t] -- cho du 3 buoc
       |
       v
Z-score normalize (9 features dau)
       |
       v
TGCN forward: GCN x3 -> GRU -> Decoder
       |
       v
De-normalize -> pred [29, 3] (km/h)
       |
       v
Build congestion_label: speed/free_flow >= 0.8 -> Thong thoang
                                         >= 0.5 -> Binh thuong
                                         >= 0.25 -> Cham
                                         < 0.25  -> Tac nghen
       |
       v
traffic.prediction (Kafka) -> Prediction Sink -> PostgreSQL
```

### 14.6 Continuous Fine-tuning (gru_finetuner.py)

Mo hinh duoc cap nhat lien tuc tu luong du lieu moi:

1. **Thu thap pending samples**: Moi khi co du WINDOW_SIZE=3 snapshot -> tao pending sample, doi them 3 timestamp tiep theo de lay actual speed (t+1, t+2, t+3) lam label.
2. **Train khi du 32 samples**: Tinh loss tren ca 3 horizons dong thoi:
   ```
   loss = MSE(pred_5m, actual_t+1) + MSE(pred_10m, actual_t+2) + MSE(pred_15m, actual_t+3)
   ```
3. **Promote model**: Neu avg_loss < best_loss -> luu checkpoint, copy vao `gru_model_v1.pt`.
4. **Hot-swap**: Inference service load model moi khi khoi dong lai (shared Docker volume `model_checkpoints`).
5. **Gradient clipping**: `max_norm=1.0` de on dinh huan luyen voi du lieu realtime co nhieu.
