import json
import os
from datetime import datetime

import asyncio
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Weather API")

STORAGE_FILE = "storage.json"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Асинхронная блокировка для безопасной работы с файлом storage.json.
storage_lock = asyncio.Lock()

# =========================
# Models
# =========================


# Модель входных данных для добавления города
class CityCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    latitude: float
    longitude: float


# Модель входных данных для регистрации пользователя
class UserCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
# =========================
# Storage helpers (JSON)
# =========================


# Загрузка данных из storage.json
def load_storage() -> dict:
    """Load storage.json or return empty structure."""
    if not os.path.exists(STORAGE_FILE):
        return {"users": {}, "next_user_id": 1}

    try:
        with open(STORAGE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"users": {}, "next_user_id": 1}

    if not isinstance(data, dict):
        return {"users": {}, "next_user_id": 1}

    data.setdefault("users", {})
    data.setdefault("next_user_id", 1)
    return data


# Сохраняет словарь с данными в storage.json
def save_storage(data: dict) -> None:
    """Save data to storage.json."""
    with open(STORAGE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# =========================
# Вспомогательные функции для работы с Open-Meteo
# =========================

# Запрашивает текущую погоду по координатам
async def fetch_current_weather(lat: float, lon: float) -> dict:

    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,wind_speed_10m,surface_pressure",
        "timezone": "auto",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(OPEN_METEO_URL, params=params)
            response.raise_for_status()
            data = response.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Open-Meteo request timeout")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo request failed: {e}")

    current = data.get("current")
    if not current:
        raise HTTPException(status_code=502, detail="Invalid response from Open-Meteo")

    return {
        "latitude": lat,
        "longitude": lon,
        "timezone": data.get("timezone"),
        "time": current.get("time"),
        "temperature": current.get("temperature_2m"),
        "wind_speed": current.get("wind_speed_10m"),
        "pressure": current.get("surface_pressure"),
    }


async def fetch_today_hourly_forecast(lat: float, lon: float) -> dict:
    """
    Запрашивает почасовой прогноз погоды на текущий день по координатам.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        "forecast_days": 1,
        "timezone": "auto",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(OPEN_METEO_URL, params=params)
            response.raise_for_status()
            data = response.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Open-Meteo request timeout")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo request failed: {e}")

    hourly = data.get("hourly")
    if not hourly:
        raise HTTPException(status_code=502, detail="Invalid hourly response from Open-Meteo")

    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    humidity = hourly.get("relative_humidity_2m", [])
    wind = hourly.get("wind_speed_10m", [])
    precipitation = hourly.get("precipitation", [])

    n = len(times)
    if not all(len(arr) == n for arr in [temps, humidity, wind, precipitation]):
        raise HTTPException(status_code=502, detail="Mismatched hourly arrays in Open-Meteo response")

    hourly_map = {}
    for i in range(n):
        hourly_map[times[i]] = {
            "temperature": temps[i],
            "humidity": humidity[i],
            "wind_speed": wind[i],
            "precipitation": precipitation[i],
        }

    date_str = times[0].split("T")[0] if times else datetime.now().strftime("%Y-%m-%d")

    return {
        "timezone": data.get("timezone"),
        "date": date_str,
        "hourly": hourly_map,
    }


async def refresh_city_forecast(user_id: int, city_name: str, latitude: float, longitude: float) -> None:
    forecast = await fetch_today_hourly_forecast(latitude, longitude)
    """
       Обновляет прогноз для одного конкретного города конкретного пользователя.

       """
    async with storage_lock:
        data = load_storage()
        user_data = data["users"].get(str(user_id))
        if not user_data:
            return

        user_data["forecasts"][city_name] = {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "timezone": forecast["timezone"],
            "date": forecast["date"],
            "hourly": forecast["hourly"],
        }
        save_storage(data)


async def refresh_all_forecasts() -> None:
    """
      Обновляет прогнозы для всех пользователей и всех их городов.

      """
    async with storage_lock:
        data = load_storage()
        users = data.get("users", {}).copy()

    for user_id_str, user_data in users.items():
        cities = user_data.get("cities", {})

        for city_name, city_info in cities.items():
            try:
                await refresh_city_forecast(
                    user_id=int(user_id_str),
                    city_name=city_name,
                    latitude=city_info["latitude"],
                    longitude=city_info["longitude"],
                )
                print(f"[refresh] Forecast updated for user {user_id_str}, city {city_name}")
            except Exception as e:
                print(f"[refresh] Failed to update user {user_id_str}, city {city_name}: {e}")


async def periodic_refresh_loop() -> None:
    """
       Бесконечный фоновый цикл обновления прогнозов.
        Стартует при запуске приложения как фоновая задача.
       """
    while True:
        try:
            await refresh_all_forecasts()
        except Exception as e:
            print(f"[refresh-loop] Unexpected error: {e}")

        await asyncio.sleep(15 * 60)


# =========================
# API endpoints
# =========================

@app.post("/users")
async def register_user(user: UserCreate):
    """
      Регистрирует нового пользователя.

          Возвращает:
          dict: user_id и имя зарегистрированного пользователя.
      """
    user_name = user.name.strip()
    if not user_name:
        raise HTTPException(status_code=400, detail="User name cannot be empty")

    async with storage_lock:
        data = load_storage()

        user_id = data["next_user_id"]
        data["users"][str(user_id)] = {
            "name": user_name,
            "cities": {},
            "forecasts": {}
        }
        data["next_user_id"] = user_id + 1

        save_storage(data)

    return {
        "user_id": user_id,
        "name": user_name
    }


@app.get("/")
async def root():
    """
        Корневой маршрут для быстрой проверки,
        что сервер запущен и отвечает.

        Возвращает:
            dict: простой статус приложения.
        """
    return {"status": "ok"}


# 1) Method: погода по координатам
@app.get("/weather/current")
async def get_current_weather(lat: float, lon: float):
    """
        Возвращает текущую погоду по координатам.
        """
    if not (-90 <= lat <= 90):
        raise HTTPException(status_code=400, detail="Latitude must be between -90 and 90")
    if not (-180 <= lon <= 180):
        raise HTTPException(status_code=400, detail="Longitude must be between -180 and 180")

    return await fetch_current_weather(lat, lon)


# 2) Method:
@app.post("/cities")
async def add_city(user_id: int, city: CityCreate):
    """
        Добавляет город пользователю и сразу кэширует прогноз на текущий день.

        """
    if not (-90 <= city.latitude <= 90):
        raise HTTPException(status_code=400, detail="Latitude must be between -90 and 90")
    if not (-180 <= city.longitude <= 180):
        raise HTTPException(status_code=400, detail="Longitude must be between -180 and 180")

    city_name = city.name.strip()
    if not city_name:
        raise HTTPException(status_code=400, detail="City name cannot be empty")

    forecast = await fetch_today_hourly_forecast(city.latitude, city.longitude)

    async with storage_lock:
        data = load_storage()

        user_data = data["users"].get(str(user_id))
        if not user_data:
            raise HTTPException(status_code=404, detail="User not found")

        user_data["cities"][city_name] = {
            "latitude": city.latitude,
            "longitude": city.longitude,
        }

        user_data["forecasts"][city_name] = {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "timezone": forecast["timezone"],
            "date": forecast["date"],
            "hourly": forecast["hourly"],
        }

        save_storage(data)

    return {
        "message": "City added and forecast cached",
        "city": city_name,
        "forecast_date": forecast["date"],
    }


@app.get("/cities")
async def list_cities(user_id: int):
    data = load_storage()

    result = []
    user_data = data.get("users", {}).get(str(user_id))
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")

    forecasts = user_data.get("forecasts", {})
    cities = user_data.get("cities", {})

    for city_name, city_info in cities.items():
        result.append({
            "name": city_name,
            "latitude": city_info.get("latitude"),
            "longitude": city_info.get("longitude"),
        })

    return result


@app.get("/cities/{city_name}/weather")
async def get_city_weather(
    city_name: str,
    user_id: int,
    time: str,
    fields: str | None = None
):
    allowed_fields = {"temperature", "humidity", "wind_speed", "precipitation"}

    city_name = city_name.strip()
    if not city_name:
        raise HTTPException(status_code=400, detail="City name cannot be empty")

    if fields:
        requested_fields = {field.strip() for field in fields.split(",") if field.strip()}
        if not requested_fields:
            raise HTTPException(status_code=400, detail="Fields parameter is empty")

        unknown_fields = requested_fields - allowed_fields
        if unknown_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown fields requested: {sorted(unknown_fields)}"
            )
    else:
        requested_fields = allowed_fields

    async with storage_lock:
        data = load_storage()
        user_data = data.get("users", {}).get(str(user_id))

    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")

    city_forecast = user_data.get("forecasts", {}).get(city_name)
    if not city_forecast:
        raise HTTPException(status_code=404, detail="Forecast for city not found")

    # Проверяем формат времени: HH:MM
    try:
        parsed_time = datetime.strptime(time, "%H:%M")
    except ValueError:
        raise HTTPException(status_code=400, detail="Time must be in HH:MM format")

    forecast_date = city_forecast.get("date")
    if not forecast_date:
        raise HTTPException(status_code=500, detail="Forecast date is missing")

    full_time_key = f"{forecast_date}T{parsed_time.strftime('%H:%M')}"

    hourly = city_forecast.get("hourly", {})
    weather_at_time = hourly.get(full_time_key)

    if not weather_at_time:
        raise HTTPException(status_code=404, detail="No forecast for the specified time")

    filtered_data = {
        key: value
        for key, value in weather_at_time.items()
        if key in requested_fields
    }

    return {
        "user_id": user_id,
        "city": city_name,
        "date": forecast_date,
        "time": time,
        "data": filtered_data
    }


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(periodic_refresh_loop())
    print("[startup] Background forecast refresh started")

if __name__ == "__main__":
    if not os.path.exists(STORAGE_FILE):
        save_storage({"users": {}, "next_user_id": 1})

    uvicorn.run(app, host="127.0.0.1", port=8000)
