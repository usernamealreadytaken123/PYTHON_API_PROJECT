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

storage_lock = asyncio.Lock()

# =========================
# Models
# =========================


class CityCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    latitude: float
    longitude: float


# =========================
# Storage helpers (JSON)
# =========================

def load_storage() -> dict:
    """Load storage.json or return empty structure."""
    if not os.path.exists(STORAGE_FILE):
        return {"cities": {}, "forecasts": {}}

    try:
        with open(STORAGE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"cities": {}, "forecasts": {}}

    if not isinstance(data, dict):
        return {"cities": {}, "forecasts": {}}

    data.setdefault("cities", {})
    data.setdefault("forecasts", {})
    return data


def save_storage(data: dict) -> None:
    """Save data to storage.json."""
    with open(STORAGE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# =========================
# Open-Meteo helpers
# =========================

async def fetch_current_weather(lat: float, lon: float) -> dict:
    """Method 1 helper: current weather by coordinates."""
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
    Method 2 helper: load today's hourly forecast and convert to a convenient cache format.
    Stores fields required later by the task:
    temperature, humidity, wind speed, precipitation.
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


async def refresh_city_forecast(city_name: str, latitude: float, longitude: float) -> None:
    forecast = await fetch_today_hourly_forecast(latitude, longitude)

    async with storage_lock:
        data = load_storage()
        data["forecasts"][city_name] = {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "timezone": forecast["timezone"],
            "date": forecast["date"],
            "hourly": forecast["hourly"],
        }
        save_storage(data)


async def refresh_all_forecasts() -> None:
    async with storage_lock:
        data = load_storage()
        cities = data.get("cities", {}).copy()

    for city_name, city_info in cities.items():
        try:
            await refresh_city_forecast(
                city_name=city_name,
                latitude=city_info["latitude"],
                longitude=city_info["longitude"],
            )
            print(f"[refresh] Forecast updated for {city_name}")
        except Exception as e:
            print(f"[refresh] Failed to update {city_name}: {e}")


async def periodic_refresh_loop() -> None:
    while True:
        try:
            await refresh_all_forecasts()
        except Exception as e:
            print(f"[refresh-loop] Unexpected error: {e}")

        await asyncio.sleep(15 * 60)


# =========================
# API endpoints
# =========================

@app.get("/")
async def root():
    return {"status": "ok"}


# 1) Method: current weather by coordinates
@app.get("/weather/current")
async def get_current_weather(lat: float, lon: float):
    if not (-90 <= lat <= 90):
        raise HTTPException(status_code=400, detail="Latitude must be between -90 and 90")
    if not (-180 <= lon <= 180):
        raise HTTPException(status_code=400, detail="Longitude must be between -180 and 180")

    return await fetch_current_weather(lat, lon)


# 2) Method: add city + cache today's forecast
@app.post("/cities")
async def add_city(city: CityCreate):
    if not (-90 <= city.latitude <= 90):
        raise HTTPException(status_code=400, detail="Latitude must be between -90 and 90")
    if not (-180 <= city.longitude <= 180):
        raise HTTPException(status_code=400, detail="Longitude must be between -180 and 180")

    city_name = city.name.strip()
    if not city_name:
        raise HTTPException(status_code=400, detail="City name cannot be empty")

    # First fetch forecast from Open-Meteo (so we don't save a city without data)
    forecast = await fetch_today_hourly_forecast(city.latitude, city.longitude)

    # Then save city + cached forecast into storage.json
    async with storage_lock:
        data = load_storage()

        data["cities"][city_name] = {
            "latitude": city.latitude,
            "longitude": city.longitude,
        }

        data["forecasts"][city_name] = {
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
async def list_cities():
    data = load_storage()

    result = []
    forecasts = data.get("forecasts", {})
    cities = data.get("cities", {})

    for city_name in forecasts:
        city_info = cities.get(city_name, {})
        result.append({
            "name": city_name,
            "latitude": city_info.get("latitude"),
            "longitude": city_info.get("longitude"),
        })

    return result


@app.get("/cities/{city_name}/weather")
async def get_city_weather(city_name: str, time: str, fields: str | None = None):
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

    data = load_storage()
    forecasts = data.get("forecasts", {})
    city_forecast = forecasts.get(city_name)

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
    # Create storage file on first start
    if not os.path.exists(STORAGE_FILE):
        save_storage({"cities": {}, "forecasts": {}})

    uvicorn.run(app, host="127.0.0.1", port=8000)
