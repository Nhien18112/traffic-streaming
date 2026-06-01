import math
import datetime
from typing import List, Tuple


def encode_time_features(hour: int, minute: int, second: int, weekday_int: int) -> List[float]:
    """
    Mã hóa thời gian thành sin/cos cyclic features.
    
    Args:
        hour: 0-23
        minute: 0-59
        second: 0-59
        weekday_int: 0=Monday, 6=Sunday (từ datetime.weekday())
    
    Returns:
        List của 4 float: [sin_hour, cos_hour, sin_weekday, cos_weekday]
    """
    # Mã hóa giờ (0-23 giờ)
    hour_fraction = hour + minute / 60.0 + second / 3600.0
    hour_rad = 2 * math.pi * hour_fraction / 24.0
    time_features = [math.sin(hour_rad), math.cos(hour_rad)]
    
    # Mã hóa ngày trong tuần (0-6)
    weekday_rad = 2 * math.pi * weekday_int / 7.0
    weekday_features = [math.sin(weekday_rad), math.cos(weekday_rad)]
    
    return time_features + weekday_features


def encode_timestamp(timestamp_str: str, date_str: str = None) -> Tuple[List[float], str]:
    """
    Mã hóa timestamp (từ database TIMESTAMPTZ) thành time features.
    
    Args:
        timestamp_str: ISO format string hoặc HHmmss format từ database
        date_str: (Optional) Date string 'YYYY-MM-DD' để lấy weekday
    
    Returns:
        Tuple của (time_features list, extracted timestamp string)
    """
    # Xử lý timestamp - chuẩn hóa format
    timestamp = str(timestamp_str).strip()
    if '_' in timestamp:
        timestamp = timestamp.split('_')[-1]
    
    # Extract chỉ các chữ số
    timestamp_digits = ''.join(ch for ch in timestamp if ch.isdigit())
    timestamp_digits = timestamp_digits[-6:].zfill(6)  # Lấy 6 chữ số cuối (HHmmss)
    
    hour = int(timestamp_digits[:2]) if len(timestamp_digits) >= 2 else 0
    minute = int(timestamp_digits[2:4]) if len(timestamp_digits) >= 4 else 0
    second = int(timestamp_digits[4:6]) if len(timestamp_digits) >= 6 else 0
    
    # Xác định weekday từ date_str
    weekday_int = 0
    if date_str:
        try:
            date_obj = datetime.datetime.strptime(str(date_str).strip(), '%Y-%m-%d')
            weekday_int = date_obj.weekday()
        except (ValueError, AttributeError):
            weekday_int = 0
    
    time_features = encode_time_features(hour, minute, second, weekday_int)
    return time_features, timestamp_digits


def extract_time_components(timestamp_obj) -> Tuple[int, int, int, int]:
    """
    Extract hour, minute, second, weekday từ datetime object hoặc timestamp string.
    
    Args:
        timestamp_obj: datetime object, ISO string, hoặc timestamp string
    
    Returns:
        Tuple của (hour, minute, second, weekday_int)
    """
    if isinstance(timestamp_obj, datetime.datetime):
        return (
            timestamp_obj.hour,
            timestamp_obj.minute,
            timestamp_obj.second,
            timestamp_obj.weekday()
        )
    else:
        # Parse string format
        timestamp_str = str(timestamp_obj).strip()
        
        # Cố gắng parse ISO format trước
        try:
            dt = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return (dt.hour, dt.minute, dt.second, dt.weekday())
        except (ValueError, AttributeError):
            pass
        
        # Fallback: extract HHmmss từ string
        if '_' in timestamp_str:
            timestamp_str = timestamp_str.split('_')[-1]
        
        timestamp_digits = ''.join(ch for ch in timestamp_str if ch.isdigit())
        timestamp_digits = timestamp_digits[-6:].zfill(6)
        
        hour = int(timestamp_digits[:2]) if len(timestamp_digits) >= 2 else 0
        minute = int(timestamp_digits[2:4]) if len(timestamp_digits) >= 4 else 0
        second = int(timestamp_digits[4:6]) if len(timestamp_digits) >= 6 else 0
        
        return (hour, minute, second, 0)
