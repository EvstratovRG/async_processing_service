# Асинхронный процессинг платежей

FastAPI, Pydantic v2, PostgreSQL, SQLAlchemy, Alembic, RabbitMQ и FastStream. Python 3.12.

## Запуск

Нужны Docker и Docker Compose. Укажите свой `API_KEY` в `.env`:

```bash
cp .env.example .env
docker compose up --build -d --wait
```

API доступен на `http://localhost:8000`, RabbitMQ Management — на `http://localhost:15672` (логин и пароль `payments`). Остановка: `docker compose down`. Данные сохраняются в томах.

Настройки: `API_KEY`, `DATABASE_URL`, `RABBITMQ_URL`, `OUTBOX_POLL_INTERVAL`, `WEBHOOK_TIMEOUT`. Порты можно изменить через `API_PORT`, `POSTGRES_PORT`, `RABBITMQ_PORT`, `RABBITMQ_MANAGEMENT_PORT`. В Compose адреса БД и брокера задаются именами сервисов.

## API

```bash
export API_KEY='local-development-key'
curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" -H 'Idempotency-Key: order-123' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"123.45","currency":"RUB","description":"Заказ", "metadata":{"order_id":123},"webhook_url":"https://example.com/webhook"}'
curl -H "X-API-Key: $API_KEY" http://localhost:8000/api/v1/payments/PAYMENT_ID
```

Создание возвращает `202` с ID, статусом и временем. Валюты: RUB, USD, EUR; сумма положительная, до 18 цифр и 2 знаков после запятой. Повтор с тем же ключом и данными вернёт тот же платёж; другие данные — `409`. Ошибки: `401` — ключ API, `404` — платёж, `422` — входные данные. URL webhook должен быть доступен из контейнера consumer.

Платёж и событие outbox сохраняются одной транзакцией. Consumer эмулирует обработку за 2–5 секунд с вероятностью успеха 90%, сохраняет результат и отправляет webhook (`payment_id`, `status`, `processed_at`). Ошибки повторяются до трёх попыток с задержками 1 и 2 секунды, затем сообщение попадает в DLQ. Некорректные сообщения сразу попадают в DLQ. Доставка — at least once: получатель должен устранять дубли webhook по `payment_id`.

## Проверки

```bash
uv sync --frozen
uv run pytest tests/unit -q
uv run mypy app tests scripts
uv run ruff check app tests migrations scripts
uv run ruff format --check app tests migrations scripts
docker compose exec -T api python - < scripts/smoke.py
```

Smoke создаёт платёж и запускает временный HTTP-получатель внутри API-контейнера. Для интеграционных тестов нужны **отдельные** БД и vhost: тесты очищают таблицы и очереди.

```bash
docker compose exec -T postgres createdb -U payments payments_test
docker compose exec -T rabbitmq rabbitmqctl add_vhost aps_test
docker compose exec -T rabbitmq rabbitmqctl set_permissions -p aps_test payments '.*' '.*' '.*'
export TEST_DATABASE_URL='postgresql+asyncpg://payments:payments@localhost:5432/payments_test'
export TEST_RABBITMQ_URL='amqp://payments:payments@localhost:5672/aps_test'
uv run pytest -q
```
