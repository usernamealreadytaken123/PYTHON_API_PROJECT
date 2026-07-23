# Weather API

HTTP-сервер на FastAPI для получения текущей погоды по координатам и хранения
почасового прогноза на текущий день для отслеживаемых городов через Open-Meteo
API.

## Быстрый старт

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python3 script.py
```

Для Windows:

```powershell
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python script.py
```

После запуска сервер доступен по адресу:

```text
http://127.0.0.1:8000
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

## Что реализовано

1. Получение текущей погоды по координатам: температура, скорость ветра,
   атмосферное давление.
2. Добавление города в список отслеживаемых городов.
3. Хранение прогноза погоды на текущий день.
4. Автоматическое обновление сохраненных прогнозов каждые 15 минут.
5. Получение списка городов, для которых сохранен прогноз.
6. Получение прогноза для города на указанное время с выбором параметров:
   температура, влажность, скорость ветра, осадки.
7. Дополнительное задание: регистрация пользователей и отдельные списки
   городов для каждого пользователя.
8. Дополнительное задание: unit-тесты API.

Эндпоинты `/cities` и `/cities/{city_name}/weather` работают в двух режимах:

- без `user_id` - базовый режим из основного задания;
- с `user_id` - пользовательский режим из дополнительного задания.

## Запуск тестов

```bash
python -m pytest
```

Ожидаемый результат:

```text
8 passed
```

## API

### Проверка сервера

```http
GET /
```

Ответ:

```json
{
  "status": "ok"
}
```

Пример PowerShell-команды:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/"
```

### Получить текущую погоду по координатам

```http
GET /weather/current?lat=55.7558&lon=37.6173
```

Пример PowerShell-команды:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/weather/current?lat=55.7558&lon=37.6173"
```

Пример ответа:

```json
{
  "latitude": 55.7558,
  "longitude": 37.6173,
  "timezone": "Europe/Moscow",
  "time": "2026-03-01T18:00",
  "temperature": -5.3,
  "wind_speed": 6.4,
  "pressure": 998.8
}
```

### Добавить город

Базовый режим:

```http
POST /cities
Content-Type: application/json

{
  "name": "Paris",
  "latitude": 48.8566,
  "longitude": 2.3522
}
```

Пример PowerShell-команды:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/cities" `
  -ContentType "application/json" `
  -Body '{"name":"Paris","latitude":48.8566,"longitude":2.3522}'
```

Пользовательский режим:

```http
POST /cities?user_id=1
Content-Type: application/json

{
  "name": "Tomsk",
  "latitude": 56.4846,
  "longitude": 84.9476
}
```

### Получить список городов с сохраненным прогнозом

Базовый режим:

```http
GET /cities
```

Пример PowerShell-команды:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/cities"
```

Пользовательский режим:

```http
GET /cities?user_id=1
```

### Получить погоду для города на указанное время

Базовый режим, все поля:

```http
GET /cities/Paris/weather?time=18:00
```

Базовый режим, выбранные поля:

```http
GET /cities/Paris/weather?time=18:00&fields=temperature,wind_speed
```

Пример PowerShell-команды:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/cities/Paris/weather?time=18:00&fields=temperature,wind_speed"
```

Пользовательский режим:

```http
GET /cities/Tomsk/weather?user_id=1&time=18:00&fields=temperature,humidity,wind_speed,precipitation
```

Допустимые значения `fields`:

- `temperature`
- `humidity`
- `wind_speed`
- `precipitation`

Если `fields` не указан, возвращаются все доступные параметры.

### Зарегистрировать пользователя

```http
POST /users
Content-Type: application/json

{
  "name": "Artem"
}
```

Пример PowerShell-команды:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/users" `
  -ContentType "application/json" `
  -Body '{"name":"Artem"}'
```

Ответ:

```json
{
  "user_id": 1,
  "name": "Artem"
}
```

## Хранение данных

Данные сохраняются в локальный файл `storage.json`. Если файла нет, он будет
создан автоматически при запуске или первом изменении данных.

Файл `storage.json` не нужен для проверки решения: его можно не прикладывать к
архиву, потому что приложение создает его самостоятельно.

## Вспомогательные файлы

`requirements.txt` содержит зависимости для запуска проекта и тестов.

`test_api.py` содержит unit-тесты API.

`test_main.http` - необязательный вспомогательный файл с готовыми HTTP-запросами
для ручной проверки в PyCharm или VS Code.

