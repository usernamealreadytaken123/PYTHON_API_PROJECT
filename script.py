import asyncio
import json
import os
from contextlib import asynccontextmanager, suppress
from datetime import datetime

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


STORAGE_FILE = os.path.join(os.path.dirname(__file__), "storage.json")
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
REFRESH_INTERVAL_SECONDS = 15 * 60

storage_lock = asyncio.Lock()


class CityCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    latitude: float
    longitude: float


class UserCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)


def empty_storage() -> dict:
    return {
        "cities": {},
        "forecasts": {},
        "users": {},
        "next_user_id": 1,
    }


def normalize_storage(data: dict) -> dict:
    data.setdefault("cities", {})
    data.setdefault("forecasts", {})
    data.setdefault("users", {})
    data.setdefault("next_user_id", 1)

    for user_data in data["users"].values():
        if isinstance(user_data, dict):
            user_data.setdefault("cities", {})
            user_data.setdefault("forecasts", {})

    return data


def load_storage() -> dict:
    if not os.path.exists(STORAGE_FILE):
        return empty_storage()

    try:
        with open(STORAGE_FILE, "r", encoding="utf-8") as storage_file:
            data = json.load(storage_file)
    except (json.JSONDecodeError, OSError):
        return empty_storage()

    if not isinstance(data, dict):
        return empty_storage()

    return normalize_storage(data)


def save_storage(data: dict) -> None:
    with open(STORAGE_FILE, "w", encoding="utf-8") as storage_file:
        json.dump(normalize_storage(data), storage_file, ensure_ascii=False, indent=2)


def get_weather_store(data: dict, user_id: int | None) -> dict:
    if user_id is None:
        return data

    user_data = data["users"].get(str(user_id))
    if user_data is None:
        raise HTTPException(status_code=404, detail="User not found")

    return user_data


def validate_coordinates(latitude: float, longitude: float) -> None:
    if not (-90 <= latitude <= 90):
        raise HTTPException(status_code=400, detail="Latitude must be between -90 and 90")
    if not (-180 <= longitude <= 180):
        raise HTTPException(status_code=400, detail="Longitude must be between -180 and 180")


def parse_requested_fields(fields: str | None) -> set[str]:
    allowed_fields = {"temperature", "humidity", "wind_speed", "precipitation"}

    if fields is None:
        return allowed_fields

    requested_fields = {field.strip() for field in fields.split(",") if field.strip()}
    if not requested_fields:
        raise HTTPException(status_code=400, detail="Fields parameter is empty")

    unknown_fields = requested_fields - allowed_fields
    if unknown_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown fields requested: {sorted(unknown_fields)}",
        )

    return requested_fields


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
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Open-Meteo request failed: {exc}")

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
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Open-Meteo request failed: {exc}")

    hourly = data.get("hourly")
    if not hourly:
        raise HTTPException(status_code=502, detail="Invalid hourly response from Open-Meteo")

    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    humidity = hourly.get("relative_humidity_2m", [])
    wind = hourly.get("wind_speed_10m", [])
    precipitation = hourly.get("precipitation", [])

    row_count = len(times)
    if not all(len(values) == row_count for values in [temps, humidity, wind, precipitation]):
        raise HTTPException(status_code=502, detail="Mismatched hourly arrays in Open-Meteo response")

    hourly_map = {}
    for index, forecast_time in enumerate(times):
        hourly_map[forecast_time] = {
            "temperature": temps[index],
            "humidity": humidity[index],
            "wind_speed": wind[index],
            "precipitation": precipitation[index],
        }

    date_str = times[0].split("T")[0] if times else datetime.now().strftime("%Y-%m-%d")
    return {
        "timezone": data.get("timezone"),
        "date": date_str,
        "hourly": hourly_map,
    }


async def refresh_city_forecast(
    user_id: int | None,
    city_name: str,
    latitude: float,
    longitude: float,
) -> None:
    forecast = await fetch_today_hourly_forecast(latitude, longitude)

    async with storage_lock:
        data = load_storage()
        weather_store = get_weather_store(data, user_id)
        weather_store["forecasts"][city_name] = {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "timezone": forecast["timezone"],
            "date": forecast["date"],
            "hourly": forecast["hourly"],
        }
        save_storage(data)


async def refresh_all_forecasts() -> None:
    async with storage_lock:
        data = load_storage()
        default_cities = data.get("cities", {}).copy()
        user_cities = {
            int(user_id): user_data.get("cities", {}).copy()
            for user_id, user_data in data.get("users", {}).items()
            if str(user_id).isdigit()
        }

    for city_name, city_info in default_cities.items():
        try:
            await refresh_city_forecast(
                user_id=None,
                city_name=city_name,
                latitude=city_info["latitude"],
                longitude=city_info["longitude"],
            )
            print(f"[refresh] Forecast updated for city {city_name}")
        except Exception as exc:
            print(f"[refresh] Failed to update city {city_name}: {exc}")

    for user_id, cities in user_cities.items():
        for city_name, city_info in cities.items():
            try:
                await refresh_city_forecast(
                    user_id=user_id,
                    city_name=city_name,
                    latitude=city_info["latitude"],
                    longitude=city_info["longitude"],
                )
                print(f"[refresh] Forecast updated for user {user_id}, city {city_name}")
            except Exception as exc:
                print(f"[refresh] Failed to update user {user_id}, city {city_name}: {exc}")


async def periodic_refresh_loop() -> None:
    while True:
        await asyncio.sleep(REFRESH_INTERVAL_SECONDS)
        try:
            await refresh_all_forecasts()
        except Exception as exc:
            print(f"[refresh-loop] Unexpected error: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    refresh_task = asyncio.create_task(periodic_refresh_loop())
    print("[startup] Background forecast refresh started")
    try:
        yield
    finally:
        refresh_task.cancel()
        with suppress(asyncio.CancelledError):
            await refresh_task


app = FastAPI(title="Weather API", lifespan=lifespan)


@app.get("/")
async def root():
    return {"status": "ok"}


@app.post("/users")
async def register_user(user: UserCreate):
    user_name = user.name.strip()
    if not user_name:
        raise HTTPException(status_code=400, detail="User name cannot be empty")

    async with storage_lock:
        data = load_storage()
        user_id = data["next_user_id"]
        data["users"][str(user_id)] = {
            "name": user_name,
            "cities": {},
            "forecasts": {},
        }
        data["next_user_id"] = user_id + 1
        save_storage(data)

    return {
        "user_id": user_id,
        "name": user_name,
    }


@app.get("/weather/current")
async def get_current_weather(lat: float, lon: float):
    validate_coordinates(lat, lon)
    return await fetch_current_weather(lat, lon)


@app.post("/cities")
async def add_city(city: CityCreate, user_id: int | None = None):
    validate_coordinates(city.latitude, city.longitude)

    city_name = city.name.strip()
    if not city_name:
        raise HTTPException(status_code=400, detail="City name cannot be empty")

    async with storage_lock:
        data = load_storage()
        get_weather_store(data, user_id)

    forecast = await fetch_today_hourly_forecast(city.latitude, city.longitude)

    async with storage_lock:
        data = load_storage()
        weather_store = get_weather_store(data, user_id)
        weather_store["cities"][city_name] = {
            "latitude": city.latitude,
            "longitude": city.longitude,
        }
        weather_store["forecasts"][city_name] = {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "timezone": forecast["timezone"],
            "date": forecast["date"],
            "hourly": forecast["hourly"],
        }
        save_storage(data)

    result = {
        "message": "City added and forecast cached",
        "city": city_name,
        "forecast_date": forecast["date"],
    }
    if user_id is not None:
        result["user_id"] = user_id

    return result


@app.get("/cities")
async def list_cities(user_id: int | None = None):
    async with storage_lock:
        data = load_storage()
        weather_store = get_weather_store(data, user_id)
        cities = weather_store.get("cities", {}).copy()
        forecasts = weather_store.get("forecasts", {}).copy()

    result = []
    for city_name, city_info in cities.items():
        if city_name not in forecasts:
            continue
        result.append(
            {
                "name": city_name,
                "latitude": city_info.get("latitude"),
                "longitude": city_info.get("longitude"),
            }
        )

    return result


@app.get("/cities/{city_name}/weather")
async def get_city_weather(
    city_name: str,
    time: str,
    fields: str | None = None,
    user_id: int | None = None,
):
    city_name = city_name.strip()
    if not city_name:
        raise HTTPException(status_code=400, detail="City name cannot be empty")

    requested_fields = parse_requested_fields(fields)

    try:
        parsed_time = datetime.strptime(time, "%H:%M")
    except ValueError:
        raise HTTPException(status_code=400, detail="Time must be in HH:MM format")

    async with storage_lock:
        data = load_storage()
        weather_store = get_weather_store(data, user_id)
        city_forecast = weather_store.get("forecasts", {}).get(city_name)

    if not city_forecast:
        raise HTTPException(status_code=404, detail="Forecast for city not found")

    forecast_date = city_forecast.get("date")
    if not forecast_date:
        raise HTTPException(status_code=500, detail="Forecast date is missing")

    full_time_key = f"{forecast_date}T{parsed_time.strftime('%H:%M')}"
    weather_at_time = city_forecast.get("hourly", {}).get(full_time_key)

    if not weather_at_time:
        raise HTTPException(status_code=404, detail="No forecast for the specified time")

    result = {
        "city": city_name,
        "date": forecast_date,
        "time": time,
        "data": {
            key: value
            for key, value in weather_at_time.items()
            if key in requested_fields
        },
    }
    if user_id is not None:
        result["user_id"] = user_id

    return result


if __name__ == "__main__":
    if not os.path.exists(STORAGE_FILE):
        save_storage(empty_storage())

    uvicorn.run(app, host="127.0.0.1", port=8000)
