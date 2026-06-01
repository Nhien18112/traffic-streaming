# HCMC Traffic Intelligence - Presentation Script (Pipeline + Topics)

Tai lieu nay la script thuyet trinh chi tiet ve pipeline du an. Ban co the doc truc tiep khi thuyet trinh hoac trich y cho slide.

---

## 1) Muc tieu he thong

- Thu thap du lieu giao thong HCMC theo thoi gian thuc tu nhieu nguon.
- Xu ly streaming de ket hop giao thong + thoi tiet + dem xe tu camera.
- Du doan toc do giao thong ngan han (5/10/15 phut) va tu dong fine-tune mo hinh.
- Cung cap API phuc vu Web UI va he thong giamsat.

---

## 2) Tong quan luong du lieu E2E

Pipeline co 3 dong chinh:

1) **Ingestion**: keo du lieu raw (camera, TomTom, OpenWeather) -> Kafka + MinIO.
2) **Processing**: AI Worker dem xe + Spark Streaming join/ghi DB + day feature cho ML.
3) **Serving**: FastAPI truy van DB + Redis cache -> Web UI, dong thoi Prometheus/Grafana giamsat.

---

## 3) Cac thanh phan chinh va vai tro

### 3.1 Ingestion (src/ingestion/main_polling.py)
- **Camera snapshots**: tai anh tu URL, luu MinIO, day message vao `camera.raw`.
- **TomTom traffic**: lay flow data va incident count, day vao `traffic.raw`.
- **OpenWeather**: lay nhiet do, do am, gio, tam nhin, day vao `weather.raw`.
- **Ly do**: Tach raw data ra khoi xu ly de he thong chay on dinh, co the scale rieng.

### 3.2 MinIO (Data Lake)
- Luu **raw image** va **parquet** (feature lake).
- Kafka chi luu metadata/URL, khong luu byte anh -> nhe hơn, giam tai.

### 3.3 Kafka (Message Bus)
- Dong vai tro trung gian cho tat ca stream.
- Tach biet ingestion va xu ly, giup retry va scale.

### 3.4 AI Worker (src/ai_worker/main.py)
- Doc `camera.raw`, tai anh tu MinIO.
- Chay YOLOv8, dem xe (motorcycle/car/bus_truck).
- Day ket qua vao `camera.processed`.

### 3.5 Spark Streaming (src/streaming/spark_processor.py)
- Doc `traffic.raw` + `weather.raw`, join va ghi vao Postgres.
- Doc `camera.processed`, ghi vao bang camera.
- Xuat `traffic.feature` lam input cho ML.
- Ghi parquet vao MinIO (feature lake) de phan tich batch.

### 3.6 MLOps (src/mlops)
- **gru_inference.py**: nhan `traffic.feature`, du doan 5/10/15 phut, day vao `traffic.prediction`.
- **gru_finetuner.py**: tu dong fine-tune model theo streaming data.
- **graph.py**: xay dung do thi giao thong (nodes/edges/weights) cho TGCN.

### 3.7 Prediction Sink (src/streaming/prediction_sink.py)
- Doc `traffic.prediction`, update du doan vao bang realtime.

### 3.8 Serving (src/serving/main.py)
- FastAPI truy van `traffic_view` va cac bang realtime.
- Redis cache giup giam load DB.
- Web UI (Vite/React) goi API de hien thi dashboard.

### 3.9 Observability
- Prometheus thu metrics tu FastAPI.
- Grafana hien thi dashboard theo doi pipeline.

---

## 4) Y nghia cac Kafka topics

### Raw topics
- **camera.raw**: su kien moi khi ingestion lay duoc anh. Chua URL MinIO + timestamp.
- **traffic.raw**: du lieu TomTom (toc do, travel time, incident count).
- **weather.raw**: du lieu thoi tiet (nhiet do, do am, gio, tam nhin).

### Processed topics
- **camera.processed**: ket qua YOLOv8 (so xe theo loai).
- **traffic.feature**: feature da duoc chuan hoa schema cho ML.
- **traffic.prediction**: ket qua du doan toc do + nhan tac nghen.

### Quality/Meta topics
- **traffic.dlq**: chua ban ghi loi (thieu truong bat buoc, sai contract).
- **model.finetuned**: thong bao khi model duoc fine-tune va promote.

---

## 5) Luong xu ly chi tiet (Step-by-step)

1) **Ingestion** keo camera/traffic/weather.
2) **Camera** -> MinIO + message `camera.raw`.
3) **AI Worker** doc `camera.raw`, chay YOLO -> `camera.processed`.
4) **Spark** join traffic + weather -> ghi Postgres.
5) **Spark** ghi camera -> Postgres.
6) **Spark** xuat parquet -> MinIO (feature lake).
7) **Spark** push `traffic.feature` -> MLOps.
8) **GRU Inference** du doan + push `traffic.prediction`.
9) **Prediction Sink** update DB.
10) **FastAPI** doc `traffic_view` -> Web UI.

---

## 6) Mo hinh du doan (TGCN/GRU)

### Ly do dung TGCN
- Giao thong co tinh **khong gian** (nut giao lien ket) va **thoi gian** (tinh xu huong).
- TGCN ket hop GNN (khong gian) + GRU (thoi gian).

### Input/Output
- Input: 3 snapshot gan nhat, 29 nut giao, 13 features/nut.
- Output: toc do du doan cho +5m, +10m, +15m.

### Feature chinh (13)
- current_speed, currentTravelTime, free_flow_speed, freeFlowTravelTime, incident_count
- temperature, humidity, wind_speed, visibility
- sin/cos time (hour, weekday)

---

## 7) Co che fine-tune tu dong (quan trong)

### Y tuong chinh
- Moi lan co du 3 snapshot -> tao 1 sample X.
- Doi them 3 timestamp tiep theo -> lay actual speed lam nhan Y.
- Khi du 32 sample -> fine-tune model.

### Quy trinh cu the
1) **Buffer window** (WINDOW_SIZE=3).
2) Tao pending sample (X, cho nhan).
3) Sau 3 timestamp tiep theo, thu nhan Y (t+1, t+2, t+3).
4) Train mini-batch, tinh loss 3 horizon.
5) Luu checkpoint neu loss tot hon best_loss.
6) Promote checkpoint -> model production.
7) Push su kien vao `model.finetuned`.

### Loi ich
- Tu dong thich nghi voi drift (mua, gio cao diem, su co).
- Khong can retrain offline, he thong luon cap nhat.

---

## 8) Chat luong du lieu

- **Data contract**: bat buoc co time, location, currentSpeed, freeFlowSpeed.
- **DLQ**: ban ghi loi -> `traffic.dlq` de debug.
- **Dedup**: drop duplicates theo (location_name, time).
- **Upsert**: ON CONFLICT trong Postgres de idempotent.
- **Watermark**: gioi han du lieu tre khi join.

---

## 9) Serving va trai nghiem nguoi dung

- FastAPI cung cap endpoint realtime + chart + summary.
- Redis cache giam latency.
- UI cho phep loc khu vuc, xem xu huong, va bieu do.
- Co endpoint diagnostics phat hien camera khong hoat dong.

---

## 10) Ket luan

- Du an la **Streaming ELT + Lambda Architecture** + **MLOps real-time**.
- He thong xu ly du lieu giao thong tu raw -> AI -> du doan -> hien thi.
- Mo hinh tu dong fine-tune giup he thong luon phu hop voi thuc te.

---

## 11) Goi y trinh bay ngan gon (5-7 phut)

- 1 phut: tong quan pipeline.
- 2 phut: Kafka topics va Spark Streaming.
- 1 phut: AI Worker + camera.
- 1-2 phut: Mo hinh TGCN/GRU + fine-tune.
- 30s: Serving + Observability.

---

## 12) Thuat ngu then chot can nhac

- Streaming ELT
- Kafka message bus
- Spark Structured Streaming
- Data lake (MinIO)
- MLOps continuous fine-tuning
- TGCN (GNN + GRU)
- CQRS (separate write/read path)

---

## 13) Cac file quan trong de tham khao

- src/ingestion/main_polling.py
- src/ai_worker/main.py
- src/streaming/spark_processor.py
- src/mlops/gru_inference.py
- src/mlops/gru_finetuner.py
- schema.sql
- docker-compose.yml
