import { useEffect, useMemo, useRef, useState } from 'react';
import L from 'leaflet';
import markerIcon2x from 'leaflet/dist/images/marker-icon-2x.png';
import markerIcon from 'leaflet/dist/images/marker-icon.png';
import markerShadow from 'leaflet/dist/images/marker-shadow.png';
import {
  LineChart,
  Line,
  ResponsiveContainer,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  Area
} from 'recharts';

const API_BASE = import.meta.env.VITE_API_BASE || '';
const WEATHER_ICON = {
  Rain: 'Mưa',
  Clouds: 'Mây',
  Clear: 'Trời quang',
  Thunderstorm: 'Giông',
  Drizzle: 'Mưa phùn',
  Mist: 'Sương mù',
  Haze: 'Hơi mù',
  Fog: 'Sương mù dày',
  Snow: 'Tuyết'
};

const WEATHER_LABEL = {
  Rain: 'Mưa',
  Clouds: 'Mây',
  Clear: 'Trời quang',
  Thunderstorm: 'Giông',
  Drizzle: 'Mưa phùn',
  Mist: 'Sương mù',
  Haze: 'Hơi mù',
  Fog: 'Sương mù dày',
  Snow: 'Tuyết'
};

const tabs = [
  { id: 'overview', label: 'Tổng quan' },
  { id: 'traffic', label: 'Giao thông' },
  { id: 'weather', label: 'Thời tiết' },
  { id: 'map', label: 'Bản đồ' },
  { id: 'locations', label: 'Vị trí' }
];

const CITY_SCENE_IMAGES = [
  'https://images.unsplash.com/photo-1465447142348-e9952c393450?auto=format&fit=crop&w=1600&q=80',
  'https://images.unsplash.com/photo-1477959858617-67f85cf4f1df?auto=format&fit=crop&w=1600&q=80',
  'https://images.unsplash.com/photo-1469474968028-56623f02e42e?auto=format&fit=crop&w=1600&q=80'
];

const TRAFFIC_STORY_CARDS = [
  {
    title: 'Nhịp sáng sớm',
    subtitle: 'Theo dõi dòng xe trước giờ cao điểm',
    image: 'https://images.unsplash.com/photo-1473448912268-2022ce9509d8?auto=format&fit=crop&w=1200&q=80'
  },
  {
    title: 'Áp lực mưa',
    subtitle: 'So sánh thời tiết và suy giảm vận tốc tức thì',
    image: 'https://images.unsplash.com/photo-1519692933481-e162a57d6721?auto=format&fit=crop&w=1200&q=80'
  },
  {
    title: 'Nghẽn nút tín hiệu',
    subtitle: 'Định vị giao lộ lưu lượng cao, tốc độ thấp',
    image: 'https://images.unsplash.com/photo-1465447142348-e9952c393450?auto=format&fit=crop&w=1600&q=80'
  }
];

function formatName(value) {
  return value ? value.replaceAll('_', ' ') : '-';
}

function formatTime(value) {
  if (!value) return '-';
  return new Date(value).toLocaleString('vi-VN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

function scoreClass(ratio) {
  if (ratio == null) return 'score-low';
  if (ratio >= 0.75) return 'score-high';
  if (ratio >= 0.4) return 'score-mid';
  return 'score-low';
}

function formatRelativeMinutes(value) {
  if (!value) return 'Không có dữ liệu';
  const deltaMs = Date.now() - new Date(value).getTime();
  const mins = Math.max(0, Math.round(deltaMs / 60000));
  if (mins < 1) return 'Vừa cập nhật';
  return `Cập nhật ${mins} phút trước`;
}

async function fetchJson(path, options) {
  const response = await fetch(`${API_BASE}${path}`, options);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function App() {
  const [activeTab, setActiveTab] = useState('overview');
  const [health, setHealth] = useState(null);
  const [latestRows, setLatestRows] = useState([]);
  const [summaryRows, setSummaryRows] = useState([]);
  const [weatherRows, setWeatherRows] = useState([]);
  const [cameraCoverage, setCameraCoverage] = useState([]);
  const [cameraCritical, setCameraCritical] = useState([]);
  const [locationCatalog, setLocationCatalog] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [locationFilter, setLocationFilter] = useState('');
  const [minVehicles, setMinVehicles] = useState(0);
  const [sortMode, setSortMode] = useState('speed-asc');
  const [targetSpeed, setTargetSpeed] = useState(35);
  const [selectedLocation, setSelectedLocation] = useState('');
  const [locationHistory, setLocationHistory] = useState([]);
  const [horizonData, setHorizonData] = useState(null);
  const [detailError, setDetailError] = useState('');
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [locationTabSelected, setLocationTabSelected] = useState('');
  const [locationTabDetail, setLocationTabDetail] = useState(null);
  const [locationTabError, setLocationTabError] = useState('');
  const [locationTabLoading, setLocationTabLoading] = useState(false);
  const [mapSelectedLocation, setMapSelectedLocation] = useState('');
  const [mapDetail, setMapDetail] = useState(null);
  const [mapError, setMapError] = useState('');
  const [mapLoading, setMapLoading] = useState(false);
  const mapRef = useRef(null);
  const mapInstanceRef = useRef(null);
  const markerRefs = useRef([]);

  const heroImage = CITY_SCENE_IMAGES[new Date().getMinutes() % CITY_SCENE_IMAGES.length];

  const avgSpeed = useMemo(() => {
    const valid = summaryRows.filter((x) => x.avg_speed != null);
    if (!valid.length) return '-';
    const sum = valid.reduce((acc, x) => acc + x.avg_speed, 0);
    return (sum / valid.length).toFixed(1);
  }, [summaryRows]);

  const totalVehicles = useMemo(() => {
    return summaryRows.reduce((acc, x) => acc + (x.total_vehicles || 0), 0).toLocaleString('vi-VN');
  }, [summaryRows]);

  const avgSpeedValue = useMemo(() => {
    const valid = summaryRows.filter((x) => x.avg_speed != null);
    if (!valid.length) return 0;
    const sum = valid.reduce((acc, x) => acc + x.avg_speed, 0);
    return sum / valid.length;
  }, [summaryRows]);

  const filteredSummaryRows = useMemo(() => {
    const keyword = locationFilter.trim().toLowerCase();
    let rows = summaryRows.filter((row) => {
      const name = (row.location_name || '').toLowerCase();
      const vehicles = row.total_vehicles || 0;
      return name.includes(keyword) && vehicles >= minVehicles;
    });

    const sorted = [...rows];
    if (sortMode === 'speed-asc') sorted.sort((a, b) => (a.avg_speed ?? 999) - (b.avg_speed ?? 999));
    if (sortMode === 'speed-desc') sorted.sort((a, b) => (b.avg_speed ?? -1) - (a.avg_speed ?? -1));
    if (sortMode === 'vehicles-desc') sorted.sort((a, b) => (b.total_vehicles || 0) - (a.total_vehicles || 0));
    if (sortMode === 'ratio-asc') sorted.sort((a, b) => (a.avg_speed_ratio ?? 999) - (b.avg_speed_ratio ?? 999));
    return sorted;
  }, [summaryRows, locationFilter, minVehicles, sortMode]);

  const filteredLocations = useMemo(() => {
    const keyword = locationFilter.trim().toLowerCase();
    const base = locationCatalog.length
      ? locationCatalog
      : summaryRows.map((row) => ({ location_name: row.location_name }));
    return base.filter((row) => (row.location_name || '').toLowerCase().includes(keyword));
  }, [locationCatalog, summaryRows, locationFilter]);

  const mapCenter = useMemo(() => {
    const points = locationCatalog.filter((row) => row.lat != null && row.lon != null);
    if (!points.length) return { lat: 10.77, lng: 106.67 };
    const avgLat = points.reduce((acc, row) => acc + row.lat, 0) / points.length;
    const avgLon = points.reduce((acc, row) => acc + row.lon, 0) / points.length;
    return { lat: avgLat, lng: avgLon };
  }, [locationCatalog]);

  const topHotspot = useMemo(() => filteredSummaryRows[0] || null, [filteredSummaryRows]);

  const busiestNode = useMemo(() => {
    if (!summaryRows.length) return null;
    return [...summaryRows].sort((a, b) => (b.total_vehicles || 0) - (a.total_vehicles || 0))[0] || null;
  }, [summaryRows]);

  const weatherRiskCount = useMemo(() => {
    return weatherRows.filter((row) => ['Rain', 'Thunderstorm', 'Drizzle'].includes(row.weather_condition)).length;
  }, [weatherRows]);

  const predictionDrift = useMemo(() => {
    if (!latestRows.length) return 0;
    const valid = latestRows.filter((row) => row.current_speed != null && row.predicted_speed != null);
    if (!valid.length) return 0;
    const driftSum = valid.reduce((acc, row) => acc + Math.abs(row.current_speed - row.predicted_speed), 0);
    return driftSum / valid.length;
  }, [latestRows]);

  const scenarioResult = useMemo(() => {
    if (!avgSpeedValue) {
      return {
        delta: 0,
        etaMinutes: 0,
        status: 'Đang chờ dữ liệu tổng hợp'
      };
    }
    const delta = Math.max(0, targetSpeed - avgSpeedValue);
    if (delta <= 0) {
      return {
        delta: 0,
        etaMinutes: 0,
        status: 'Đã đạt mục tiêu với hiệu năng hiện tại'
      };
    }
    return {
      delta,
      etaMinutes: Math.ceil(delta * 6),
      status: 'Cần can thiệp vào điểm nghẽn'
    };
  }, [targetSpeed, avgSpeedValue]);

  const chartSeries = useMemo(() => {
    if (!locationHistory.length) return [];
    return [...locationHistory]
      .reverse()
      .map((item) => ({
        time: formatTime(item.event_time),
        current: item.current_speed,
        predicted: item.predicted_speed
      }));
  }, [locationHistory]);

  const refreshAll = async () => {
    setLoading(true);
    setError('');
    try {
      const [healthData, latestData, summaryData, weatherData, coverageData, locationData] = await Promise.all([
        fetchJson('/api/health'),
        fetchJson('/api/traffic/latest'),
        fetchJson('/api/traffic/summary'),
        fetchJson('/api/weather/impact'),
        fetchJson('/api/diagnostics/camera-coverage?hours=1'),
        fetchJson('/api/locations')
      ]);

      setHealth(healthData);
      setLatestRows(latestData.data || []);
      setSummaryRows(summaryData.data || []);
      setWeatherRows(weatherData.data || []);
      setCameraCoverage(coverageData.data || []);
      setCameraCritical(coverageData.critical_locations || []);
      setLocationCatalog(locationData.data || []);
    } catch (e) {
      setError(e.message || 'Không thể tải dữ liệu bảng điều khiển');
    } finally {
      setLoading(false);
    }
  };

  const openLocationPanel = async (locationName) => {
    if (!locationName) return;
    setLocationTabSelected(locationName);
    setLocationTabLoading(true);
    setLocationTabError('');
    try {
      const horizonPayload = await fetchJson(`/api/traffic/horizon/${locationName}`);
      setLocationTabDetail(horizonPayload.data || null);
    } catch (e) {
      setLocationTabDetail(null);
      setLocationTabError(e.message || 'Không thể tải chi tiết dự đoán');
    } finally {
      setLocationTabLoading(false);
    }
  };

  const openMapLocation = async (locationName) => {
    if (!locationName) return;
    setMapSelectedLocation(locationName);
    setMapLoading(true);
    setMapError('');
    try {
      const horizonPayload = await fetchJson(`/api/traffic/horizon/${locationName}`);
      setMapDetail(horizonPayload.data || null);
    } catch (e) {
      setMapDetail(null);
      setMapError(e.message || 'Không thể tải chi tiết dự đoán');
    } finally {
      setMapLoading(false);
    }
  };

  const openLocationDetail = async (locationName) => {
    if (!locationName) return;
    setSelectedLocation(locationName);
    setLoadingDetail(true);
    setDetailError('');

    try {
      const [historyPayload, horizonPayload] = await Promise.all([
        fetchJson(`/api/traffic/location/${locationName}?limit=20`),
        fetchJson(`/api/traffic/horizon/${locationName}`)
      ]);

      setLocationHistory(historyPayload.data || []);
      setHorizonData(horizonPayload.data || null);
    } catch (e) {
      setLocationHistory([]);
      setHorizonData(null);
      setDetailError(e.message || 'Không thể tải chi tiết vị trí');
    } finally {
      setLoadingDetail(false);
    }
  };

  const closeLocationDetail = () => {
    setSelectedLocation('');
    setLocationHistory([]);
    setHorizonData(null);
    setDetailError('');
    setLoadingDetail(false);
  };

  useEffect(() => {
    refreshAll();
    const timer = setInterval(refreshAll, 10000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    L.Icon.Default.mergeOptions({
      iconRetinaUrl: markerIcon2x,
      iconUrl: markerIcon,
      shadowUrl: markerShadow
    });
  }, []);

  useEffect(() => {
    if (!mapRef.current || mapInstanceRef.current) return;
    const map = L.map(mapRef.current, {
      center: [mapCenter.lat, mapCenter.lng],
      zoom: 12,
      zoomControl: true
    });
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(map);
    mapInstanceRef.current = map;
    return () => {
      map.remove();
      mapInstanceRef.current = null;
    };
  }, [mapCenter]);

  useEffect(() => {
    if (!mapInstanceRef.current) return;
    mapInstanceRef.current.setView([mapCenter.lat, mapCenter.lng], mapInstanceRef.current.getZoom());
  }, [mapCenter]);

  useEffect(() => {
    if (!mapInstanceRef.current) return;
    markerRefs.current.forEach((marker) => marker.remove());
    markerRefs.current = [];

    locationCatalog
      .filter((row) => row.lat != null && row.lon != null)
      .forEach((row) => {
        const marker = L.marker([row.lat, row.lon], {
          title: formatName(row.location_name)
        }).addTo(mapInstanceRef.current);
        marker.on('click', () => openMapLocation(row.location_name));
        markerRefs.current.push(marker);
      });
  }, [locationCatalog]);

  return (
    <div className="app-shell">
      <div className="backdrop-grid" />
      <header className="topbar">
        <div>
          <p className="eyebrow">Trung tâm Điều hành Đô thị</p>
          <h1>Bảng điều khiển Giao thông</h1>
        </div>
        <div className="topbar-actions">
          <div className={`status-pill ${health ? 'online' : 'offline'}`}>
            <span className="dot" />
            {health ? 'Hệ thống hoạt động' : 'Không có API'}
          </div>
          <div className="quick-nav">
            <button type="button" onClick={() => setActiveTab('map')}>
              Mở bản đồ
            </button>
            <button type="button" onClick={() => setActiveTab('locations')}>
              Mở camera
            </button>
          </div>
          <button onClick={refreshAll} disabled={loading}>
            {loading ? 'Đang làm mới...' : 'Làm mới'}
          </button>
        </div>
      </header>

      <section className="hero-stage" style={{ backgroundImage: `url(${heroImage})` }}>
        <div className="hero-overlay" />
        <div className="hero-content">
          <p className="eyebrow">Bản đồ Sống động Đô thị</p>
          <h2>Bức tranh Di chuyển Thời gian thực</h2>
          <p>
            Theo dõi dòng giao thông theo thời gian thực, lọc điểm nóng linh hoạt,
            và mô phỏng mức tốc độ mục tiêu ngay trên bảng điều khiển.
          </p>
          <div className="hero-tags">
            <span>Độ mới: {formatRelativeMinutes(health?.latest_data)}</span>
            <span>Độ lệch dự đoán: {predictionDrift.toFixed(1)} km/h</span>
            <span>Điểm rủi ro thời tiết: {weatherRiskCount}</span>
          </div>
        </div>
      </section>

      <section className="insight-strip">
        <article className="insight-card">
          <h4>Điểm nóng chậm nhất</h4>
          <p>{topHotspot ? formatName(topHotspot.location_name) : '-'}</p>
          <small>{topHotspot ? `${topHotspot.avg_speed?.toFixed?.(1) || '-'} km/h` : 'Chưa có vị trí'}</small>
        </article>
        <article className="insight-card">
          <h4>Giao lộ đông nhất</h4>
          <p>{busiestNode ? formatName(busiestNode.location_name) : '-'}</p>
          <small>{busiestNode ? `${(busiestNode.total_vehicles || 0).toLocaleString('vi-VN')} xe` : 'Không có dữ liệu'}</small>
        </article>
        <article className="insight-card">
          <h4>Độ mới hệ thống</h4>
          <p>{formatRelativeMinutes(health?.latest_data)}</p>
          <small>Nhịp nhận dữ liệu mới nhất</small>
        </article>
      </section>

      <section className="coverage-panel">
        <div className="coverage-head">
          <h3>Độ bao phủ camera (1 giờ)</h3>
          <p className="muted">Theo dõi vị trí mất feed camera hoặc bị trễ.</p>
        </div>
        {cameraCritical.length === 0 ? (
          <div className="coverage-ok">Tất cả vị trí đều ổn định trong 1 giờ qua.</div>
        ) : (
          <div className="coverage-critical-wrap">
            {cameraCritical.map((name) => (
              <span key={name} className="coverage-critical-item">{formatName(name)}</span>
            ))}
          </div>
        )}
        <div className="coverage-grid">
          {cameraCoverage.slice(0, 8).map((row) => (
            <article key={row.location_name} className="coverage-card">
              <h4>{formatName(row.location_name)}</h4>
              <p>Độ bao phủ: {row.coverage_pct ?? 0}%</p>
              <p>Tỷ lệ 0 dòng: {row.zero_pct ?? 0}%</p>
              <small>
                {row.stale_minutes == null ? 'Không có dữ liệu camera' : `Trễ: ${row.stale_minutes} phút`}
              </small>
            </article>
          ))}
        </div>
      </section>

      <section className="control-deck">
        <div className="control-item">
          <label htmlFor="filter-location">Lọc vị trí</label>
          <input
            id="filter-location"
            value={locationFilter}
            onChange={(e) => setLocationFilter(e.target.value)}
            placeholder="Nhập từ khóa vị trí"
          />
        </div>
        <div className="control-item">
          <label htmlFor="filter-vehicles">Số xe tối thiểu: {minVehicles}</label>
          <input
            id="filter-vehicles"
            type="range"
            min={0}
            max={250}
            step={5}
            value={minVehicles}
            onChange={(e) => setMinVehicles(Number(e.target.value))}
          />
        </div>
        <div className="control-item">
          <label htmlFor="sort-mode">Sắp xếp</label>
          <select id="sort-mode" value={sortMode} onChange={(e) => setSortMode(e.target.value)}>
            <option value="speed-asc">Tốc độ tăng (điểm nóng trước)</option>
            <option value="speed-desc">Tốc độ giảm</option>
            <option value="vehicles-desc">Lượng xe giảm</option>
            <option value="ratio-asc">Tỷ lệ tăng</option>
          </select>
        </div>
      </section>

      <nav className="tab-row">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={activeTab === tab.id ? 'active' : ''}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      {error && <div className="error-banner">Lỗi tải dữ liệu: {error}</div>}

      {activeTab === 'overview' && (
        <section className="panel-grid">
          <article className="stat-card">
            <h3>Tổng bản ghi</h3>
            <p>{health?.total_records?.toLocaleString?.('vi-VN') || '-'}</p>
            <small>Mới nhất: {formatTime(health?.latest_data)}</small>
          </article>
          <article className="stat-card">
            <h3>Tốc độ trung bình</h3>
            <p>{avgSpeed} km/h</p>
            <small>Trên các vị trí đang hoạt động</small>
          </article>
          <article className="stat-card">
            <h3>Tổng số xe</h3>
            <p>{totalVehicles}</p>
            <small>Tổng hợp từ đếm camera</small>
          </article>

          <article className="wide-card">
            <h3>Tổng hợp theo vị trí</h3>
            <div className="summary-grid">
              {filteredSummaryRows.map((row) => (
                <div
                  className="summary-item summary-clickable"
                  key={row.location_name}
                  onClick={() => openLocationDetail(row.location_name)}
                  role="button"
                  tabIndex={0}
                  onKeyDown={(e) => e.key === 'Enter' && openLocationDetail(row.location_name)}
                >
                  <h4>{formatName(row.location_name)}</h4>
                  <div className="summary-speed">{row.avg_speed?.toFixed?.(1) || '-'} km/h</div>
                  <div className={`score-bar ${scoreClass(row.avg_speed_ratio)}`}>
                    <span style={{ width: `${Math.max(2, Math.round((row.avg_speed_ratio || 0) * 100))}%` }} />
                  </div>
                  <p>{(row.total_vehicles || 0).toLocaleString('vi-VN')} xe</p>
                  <div className="vehicle-breakdown">
                    <span>Xe máy: {(row.total_motorcycle || 0).toLocaleString('vi-VN')}</span>
                    <span>Ô tô: {(row.total_car || 0).toLocaleString('vi-VN')}</span>
                    <span>Bus/tải: {(row.total_bus_truck || 0).toLocaleString('vi-VN')}</span>
                  </div>
                </div>
              ))}
            </div>
            {filteredSummaryRows.length === 0 && (
              <p className="muted">Không có vị trí phù hợp bộ lọc hiện tại.</p>
            )}
          </article>

          <article className="wide-card scenario-card">
            <h3>Mô phỏng kịch bản</h3>
            <p className="muted">Đặt mức tốc độ trung bình để đánh giá mức độ ưu tiên.</p>
            <div className="scenario-grid">
              <label htmlFor="target-speed" className="scenario-input">
                Tốc độ mục tiêu: {targetSpeed} km/h
                <input
                  id="target-speed"
                  type="range"
                  min={20}
                  max={55}
                  step={1}
                  value={targetSpeed}
                  onChange={(e) => setTargetSpeed(Number(e.target.value))}
                />
              </label>
              <div className="scenario-result">
                <p>Trung bình hiện tại: {avgSpeed} km/h</p>
                <p>Cần tăng: {scenarioResult.delta.toFixed(1)} km/h</p>
                <p>Dự kiến ổn định: {scenarioResult.etaMinutes} phút</p>
                <small>{scenarioResult.status}</small>
              </div>
            </div>
          </article>
        </section>
      )}

      {activeTab === 'traffic' && (
        <section className="table-panel">
          <h3>Dòng giao thông thời gian thực</h3>
          <table>
            <thead>
              <tr>
                <th>Vị trí</th>
                <th>Hiện tại</th>
                <th>Dự đoán</th>
                <th>Ùn tắc</th>
                <th>Cập nhật</th>
              </tr>
            </thead>
            <tbody>
              {latestRows.length === 0 && (
                <tr>
                  <td colSpan={5} className="muted">Chưa có dữ liệu. Kiểm tra luồng FAST và chủ đề.</td>
                </tr>
              )}
              {latestRows.map((row) => (
                <tr key={row.location_name} className="clickable-row" onClick={() => openLocationDetail(row.location_name)}>
                  <td>{formatName(row.location_name)}</td>
                  <td>{row.current_speed?.toFixed?.(1) || '-'} km/h</td>
                  <td>{row.predicted_speed?.toFixed?.(1) || '-'} km/h</td>
                  <td>
                    <span className={`chip ${scoreClass(row.speed_ratio)}`}>
                      {row.congestion_label || 'Không rõ'}
                    </span>
                    {row.no_camera_feed && <span className="chip feed-missing">Mất feed camera</span>}
                  </td>
                  <td>{formatTime(row.event_time)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="muted">Bấm vào dòng để xem chi tiết mô hình và từng loại xe.</p>
        </section>
      )}

      {activeTab === 'weather' && (
        <section className="weather-grid">
          {weatherRows.length === 0 && <div className="muted">Chưa có dữ liệu ảnh hưởng thời tiết.</div>}
          {weatherRows.map((row) => (
            <article key={row.weather_condition} className="weather-card">
              <h4>{WEATHER_ICON[row.weather_condition] || 'Thời tiết'}</h4>
              <p className="wx-title">{WEATHER_LABEL[row.weather_condition] || row.weather_condition}</p>
              <p>{row.avg_temperature?.toFixed?.(1) || '-'} C</p>
              <p>{row.avg_speed?.toFixed?.(1) || '-'} km/h</p>
              <small>{row.sample_count} mẫu</small>
            </article>
          ))}
        </section>
      )}

      {activeTab === 'map' && (
        <section className="map-panel">
          <div className="map-frame">
            <div ref={mapRef} className="map-canvas" />
          </div>
          <div className="map-detail">
            {!mapSelectedLocation && (
              <div className="muted">Bấm vào marker để xem camera và dự đoán +15 phút.</div>
            )}

            {mapSelectedLocation && (
              <>
                <div className="map-detail-head">
                  <div>
                    <h3>{formatName(mapSelectedLocation)}</h3>
                    <p className="muted">Khung dự đoán: +15 phút</p>
                  </div>
                  <div>
                    {locationCatalog.find((row) => row.location_name === mapSelectedLocation)?.has_camera ? (
                      <span className="chip">Camera</span>
                    ) : (
                      <span className="chip feed-missing">Không có camera</span>
                    )}
                  </div>
                </div>

                {mapLoading && <p className="muted">Đang tải dự đoán...</p>}
                {!mapLoading && mapError && <div className="error-banner">{mapError}</div>}

                {!mapLoading && !mapError && (
                  <div className="map-detail-grid">
                    <div className="camera-frame">
                      {locationCatalog.find((row) => row.location_name === mapSelectedLocation)?.camera_url ? (
                        <img
                          src={locationCatalog.find((row) => row.location_name === mapSelectedLocation)?.camera_url}
                          alt={`Camera ${formatName(mapSelectedLocation)}`}
                          loading="lazy"
                        />
                      ) : (
                        <div className="muted">Không có ảnh camera cho vị trí này.</div>
                      )}
                    </div>
                    <div className="prediction-card">
                      <h4>Tốc độ dự đoán +15 phút</h4>
                      <p className="predicted-speed">
                        {mapDetail?.horizons?.['15m']?.speed?.toFixed?.(1) || '-'} km/h
                      </p>
                      <small>Cập nhật: {formatTime(mapDetail?.event_time)}</small>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </section>
      )}

      {activeTab === 'locations' && (
        <section className="locations-panel">
          <div className="locations-list">
            <div className="locations-list-head">
              <h3>Danh sách vị trí</h3>
              <p className="muted">Chọn vị trí để xem camera và dự đoán +15 phút.</p>
            </div>
            {filteredLocations.length === 0 && (
              <div className="muted">Không có vị trí phù hợp bộ lọc hiện tại.</div>
            )}
            <div className="locations-grid">
              {filteredLocations.map((row) => (
                <button
                  type="button"
                  key={row.location_name}
                  className={`location-item ${locationTabSelected === row.location_name ? 'active' : ''}`}
                  onClick={() => openLocationPanel(row.location_name)}
                >
                  <div>
                    <h4>{formatName(row.location_name)}</h4>
                    <p className="muted">
                      {row.has_camera ? 'Có camera' : 'Chưa gắn camera'}
                    </p>
                  </div>
                  <span className="location-chip">Xem</span>
                </button>
              ))}
            </div>
          </div>

          <div className="location-detail-card">
            {!locationTabSelected && (
              <div className="muted">Chọn vị trí để xem camera và dự đoán.</div>
            )}

            {locationTabSelected && (
              <>
                <div className="location-detail-head">
                  <div>
                    <h3>{formatName(locationTabSelected)}</h3>
                    <p className="muted">Khung dự đoán: +15 phút</p>
                  </div>
                  <div className="location-meta">
                    {locationCatalog.find((row) => row.location_name === locationTabSelected)?.has_camera ? (
                      <span className="chip">Camera</span>
                    ) : (
                      <span className="chip feed-missing">Không có camera</span>
                    )}
                  </div>
                </div>

                {locationTabLoading && <p className="muted">Đang tải dự đoán...</p>}
                {!locationTabLoading && locationTabError && (
                  <div className="error-banner">{locationTabError}</div>
                )}

                {!locationTabLoading && !locationTabError && (
                  <div className="location-detail-grid">
                    <div className="camera-frame">
                      {locationCatalog.find((row) => row.location_name === locationTabSelected)?.camera_url ? (
                        <img
                          src={locationCatalog.find((row) => row.location_name === locationTabSelected)?.camera_url}
                          alt={`Camera ${formatName(locationTabSelected)}`}
                          loading="lazy"
                        />
                      ) : (
                        <div className="muted">Không có ảnh camera cho vị trí này.</div>
                      )}
                    </div>
                    <div className="prediction-card">
                      <h4>Tốc độ dự đoán +15 phút</h4>
                      <p className="predicted-speed">
                        {locationTabDetail?.horizons?.['15m']?.speed?.toFixed?.(1) || '-'} km/h
                      </p>
                      <small>Cập nhật: {formatTime(locationTabDetail?.event_time)}</small>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </section>
      )}

      <section className="story-wall">
        {TRAFFIC_STORY_CARDS.map((card) => (
          <article className="story-card" key={card.title}>
            <img src={card.image} alt={card.title} loading="lazy" />
            <div className="story-copy">
              <h4>{card.title}</h4>
              <p>{card.subtitle}</p>
            </div>
          </article>
        ))}
      </section>

      {selectedLocation && (
        <div className="detail-overlay" onClick={closeLocationDetail}>
          <div className="detail-modal" onClick={(e) => e.stopPropagation()}>
            <div className="detail-header">
              <div>
                <h3>{formatName(selectedLocation)}</h3>
                <p className="muted">Chi tiết mô hình và loại xe</p>
              </div>
              <button onClick={closeLocationDetail}>Đóng</button>
            </div>

            {loadingDetail && <p className="muted">Đang tải chi tiết...</p>}
            {!loadingDetail && detailError && <div className="error-banner">{detailError}</div>}

            {!loadingDetail && !detailError && (
              <>
                <section className="detail-chart-card">
                  <h4>Diễn biến tốc độ (Hiện tại vs AI)</h4>
                  <p className="muted">Khung quan sát gần nhất cho vị trí đang chọn.</p>
                  <div className="chart-wrap">
                    <ResponsiveContainer width="100%" height={280}>
                      <LineChart data={chartSeries} margin={{ top: 12, right: 18, left: 0, bottom: 0 }}>
                        <defs>
                          <linearGradient id="currentFill" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="#00c9a7" stopOpacity={0.28} />
                            <stop offset="100%" stopColor="#00c9a7" stopOpacity={0.03} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid stroke="rgba(148,163,184,0.15)" vertical={false} />
                        <XAxis dataKey="time" tick={{ fill: '#cbd5e1', fontSize: 12 }} axisLine={false} tickLine={false} minTickGap={28} />
                        <YAxis tick={{ fill: '#cbd5e1', fontSize: 12 }} axisLine={false} tickLine={false} width={42} />
                        <Tooltip
                          contentStyle={{ background: '#0f172a', border: '1px solid rgba(148,163,184,0.35)', borderRadius: 10, color: '#e2e8f0' }}
                          labelStyle={{ color: '#e2e8f0' }}
                        />
                        <Legend wrapperStyle={{ color: '#cbd5e1', fontSize: 12 }} />
                        <Area type="monotone" dataKey="current" stroke="none" fill="url(#currentFill)" />
                        <Line
                          type="monotone"
                          dataKey="current"
                          name="Tốc độ hiện tại"
                          stroke="#00c9a7"
                          strokeWidth={2.2}
                          dot={false}
                          activeDot={{ r: 4 }}
                        />
                        <Line
                          type="monotone"
                          dataKey="predicted"
                          name="AI dự đoán"
                          stroke="#8b5cf6"
                          strokeWidth={2}
                          strokeDasharray="6 4"
                          dot={{ r: 3 }}
                          connectNulls
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </section>

                <section className="detail-grid">
                  <article className="detail-card">
                    <h4>Mô hình</h4>
                    <p>Phiên bản: {horizonData?.model_version || 'Không có'}</p>
                    <p>Hiện tại: {horizonData?.current_speed?.toFixed?.(1) || '-'} km/h</p>
                    <p>Chuẩn: {horizonData?.free_flow_speed?.toFixed?.(1) || '-'} km/h</p>
                    <p>Nhãn hiện tại: {horizonData?.predicted_congestion_label || 'Không có'}</p>
                  </article>

                  <article className="detail-card">
                    <h4>Khung dự đoán</h4>
                    <p>+5m: {horizonData?.horizons?.['5m']?.speed?.toFixed?.(1) || '-'} km/h ({horizonData?.horizons?.['5m']?.label || 'Không có'})</p>
                    <p>+10m: {horizonData?.horizons?.['10m']?.speed?.toFixed?.(1) || '-'} km/h ({horizonData?.horizons?.['10m']?.label || 'Không có'})</p>
                    <p>+15m: {horizonData?.horizons?.['15m']?.speed?.toFixed?.(1) || '-'} km/h ({horizonData?.horizons?.['15m']?.label || 'Không có'})</p>
                  </article>

                  <article className="detail-card">
                    <h4>Loại xe (mới nhất)</h4>
                    {locationHistory[0]?.no_camera_feed && (
                      <p className="warning-text">Mất feed camera gần mốc thời gian này.</p>
                    )}
                    <p>Xe máy: {(locationHistory[0]?.motorcycle_count || 0).toLocaleString('vi-VN')}</p>
                    <p>Ô tô: {(locationHistory[0]?.car_count || 0).toLocaleString('vi-VN')}</p>
                    <p>Bus/tải: {(locationHistory[0]?.bus_truck_count || 0).toLocaleString('vi-VN')}</p>
                  </article>
                </section>

                <section className="detail-table-wrap">
                  <h4>Diễn biến gần đây</h4>
                  <table>
                    <thead>
                      <tr>
                        <th>Thời gian</th>
                        <th>Hiện tại</th>
                        <th>Dự đoán</th>
                        <th>Xe máy</th>
                        <th>Ô tô</th>
                        <th>Bus/tải</th>
                      </tr>
                    </thead>
                    <tbody>
                      {locationHistory.map((item) => (
                        <tr key={`${selectedLocation}-${item.event_time}`}>
                          <td>{formatTime(item.event_time)}</td>
                          <td>{item.current_speed?.toFixed?.(1) || '-'} km/h</td>
                          <td>{item.predicted_speed?.toFixed?.(1) || '-'} km/h</td>
                          <td>{(item.motorcycle_count || 0).toLocaleString('vi-VN')}</td>
                          <td>{(item.car_count || 0).toLocaleString('vi-VN')}</td>
                          <td>{(item.bus_truck_count || 0).toLocaleString('vi-VN')}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </section>
              </>
            )}
          </div>
        </div>
      )}

    </div>
  );
}

export default App;
