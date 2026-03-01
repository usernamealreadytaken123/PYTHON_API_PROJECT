import os
import tempfile

from fastapi.testclient import TestClient

import script


client = TestClient(script.app)


def setup_test_storage():
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    tmp.close()

    script.STORAGE_FILE = tmp.name
    script.save_storage({"users": {}, "next_user_id": 1})

    return tmp.name


def teardown_test_storage(path: str):
    if os.path.exists(path):
        os.remove(path)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_register_user():
    storage_path = setup_test_storage()
    try:
        response = client.post("/users", json={"name": "Artem"})
        assert response.status_code == 200

        data = response.json()
        assert data["user_id"] == 1
        assert data["name"] == "Artem"
    finally:
        teardown_test_storage(storage_path)


def test_add_city_for_user():
    storage_path = setup_test_storage()

    async def fake_fetch_today_hourly_forecast(lat: float, lon: float):
        return {
            "timezone": "Europe/Moscow",
            "date": "2026-03-01",
            "hourly": {
                "2026-03-01T18:00": {
                    "temperature": -3.4,
                    "humidity": 82,
                    "wind_speed": 5.7,
                    "precipitation": 0.0,
                }
            },
        }

    original = script.fetch_today_hourly_forecast
    script.fetch_today_hourly_forecast = fake_fetch_today_hourly_forecast

    try:
        # register user
        user_resp = client.post("/users", json={"name": "Artem"})
        user_id = user_resp.json()["user_id"]

        # add city
        response = client.post(
            f"/cities?user_id={user_id}",
            json={
                "name": "Moscow",
                "latitude": 55.7558,
                "longitude": 37.6173,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["city"] == "Moscow"
        assert data["forecast_date"] == "2026-03-01"

    finally:
        script.fetch_today_hourly_forecast = original
        teardown_test_storage(storage_path)


def test_list_cities_for_user():
    storage_path = setup_test_storage()

    async def fake_fetch_today_hourly_forecast(lat: float, lon: float):
        return {
            "timezone": "Europe/Moscow",
            "date": "2026-03-01",
            "hourly": {
                "2026-03-01T18:00": {
                    "temperature": -3.4,
                    "humidity": 82,
                    "wind_speed": 5.7,
                    "precipitation": 0.0,
                }
            },
        }

    original = script.fetch_today_hourly_forecast
    script.fetch_today_hourly_forecast = fake_fetch_today_hourly_forecast

    try:
        user_resp = client.post("/users", json={"name": "Artem"})
        user_id = user_resp.json()["user_id"]

        client.post(
            f"/cities?user_id={user_id}",
            json={
                "name": "Moscow",
                "latitude": 55.7558,
                "longitude": 37.6173,
            },
        )

        response = client.get(f"/cities?user_id={user_id}")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["name"] == "Moscow"

    finally:
        script.fetch_today_hourly_forecast = original
        teardown_test_storage(storage_path)


def test_get_city_weather_for_user():
    storage_path = setup_test_storage()

    async def fake_fetch_today_hourly_forecast(lat: float, lon: float):
        return {
            "timezone": "Europe/Moscow",
            "date": "2026-03-01",
            "hourly": {
                "2026-03-01T18:00": {
                    "temperature": -3.4,
                    "humidity": 82,
                    "wind_speed": 5.7,
                    "precipitation": 0.0,
                }
            },
        }

    original = script.fetch_today_hourly_forecast
    script.fetch_today_hourly_forecast = fake_fetch_today_hourly_forecast

    try:
        user_resp = client.post("/users", json={"name": "Artem"})
        user_id = user_resp.json()["user_id"]

        client.post(
            f"/cities?user_id={user_id}",
            json={
                "name": "Moscow",
                "latitude": 55.7558,
                "longitude": 37.6173,
            },
        )

        response = client.get(
            f"/cities/Moscow/weather?user_id={user_id}&time=18:00&fields=temperature,wind_speed"
        )

        assert response.status_code == 200
        data = response.json()

        assert data["city"] == "Moscow"
        assert data["time"] == "18:00"
        assert data["data"] == {
            "temperature": -3.4,
            "wind_speed": 5.7,
        }

    finally:
        script.fetch_today_hourly_forecast = original
        teardown_test_storage(storage_path)


def test_get_current_weather():
    storage_path = setup_test_storage()

    async def fake_fetch_current_weather(lat: float, lon: float):
        return {
            "latitude": lat,
            "longitude": lon,
            "timezone": "Europe/Moscow",
            "time": "2026-03-01T18:00",
            "temperature": -5.3,
            "wind_speed": 6.4,
            "pressure": 998.8,
        }

    original = script.fetch_current_weather
    script.fetch_current_weather = fake_fetch_current_weather

    try:
        response = client.get("/weather/current?lat=55.7558&lon=37.6173")
        assert response.status_code == 200

        data = response.json()
        assert data["temperature"] == -5.3
        assert data["wind_speed"] == 6.4
        assert data["pressure"] == 998.8

    finally:
        script.fetch_current_weather = original
        teardown_test_storage(storage_path)