# async_processing_service

Асинхронный сервис обработки платежей: FastAPI + Pydantic v2, SQLAlchemy 2.0 + asyncpg,
PostgreSQL, Alembic, RabbitMQ + FastStream. Python 3.12, зависимости зафиксированы в `uv.lock`.

## Архитектура

- `app/api` — HTTP-маршруты, авторизация, Pydantic-схемы и преобразование ошибок в HTTP-ответы.
- `app/services` — создание/получение платежей, обработка и публикация outbox;
  здесь определяются границы транзакций и интерфейсы зависимостей.
- `app/repositories` — SQLAlchemy-модели, SQL-запросы и реализация Unit of Work.
- `app/domain.py` — обычные dataclass-объекты и ошибки, без зависимостей от библиотек.
- `app/adapters` — эмулятор шлюза, HTTP webhook и RabbitMQ.
- `app/handlers` и `app/workers` — обработчик сообщений с retry/ack и цикл публикации outbox.
- `app/main.py` и `app/consumer.py` — сборка зависимостей и жизненный цикл процессов.

Направление вызовов: API/обработчики → сервисы → интерфейсы репозиториев и адаптеров.
Реализации подключаются при сборке приложения. Сервисы не импортируют FastAPI, Pydantic,
SQLAlchemy, HTTPX или FastStream и не возвращают ORM-объекты. Unit of Work предоставляет
оба репозитория на одной сессии: успешный выход коммитит транзакцию, исключение откатывает её.
Результат платежа и доставка webhook используют две отдельные транзакции, чтобы сбой
уведомления не отменял уже выполненную обработку.

## Запуск в Docker

Нужны Docker и Docker Compose.

```bash
cp .env.example .env
# Укажите собственный API_KEY в .env.
docker compose up --build -d --wait
```

Compose запускает четыре сервиса: `postgres`, `rabbitmq`, `api`, `consumer`.
API применяет миграции перед запуском; consumer ждёт готовности API.
Данные PostgreSQL и RabbitMQ сохраняются в именованных томах.

- API: `http://localhost:8000`.
- RabbitMQ Management: `http://localhost:15672`, пользователь/пароль `payments`/`payments`.
- Порты привязаны к localhost. Для смены портов задайте `API_PORT`, `POSTGRES_PORT`,
  `RABBITMQ_PORT`, `RABBITMQ_MANAGEMENT_PORT` в `.env`.

```bash
docker compose logs -f api consumer
docker compose stop
docker compose start
# Удаление контейнеров с сохранением данных:
docker compose down
```

## Настройки

| Переменная | Назначение | Значение для локального запуска |
| --- | --- | --- |
| `API_KEY` | Обязательный непустой статический ключ | Задаётся в `.env` |
| `DATABASE_URL` | SQLAlchemy URL PostgreSQL | `postgresql+asyncpg://payments:payments@localhost:5432/payments` |
| `RABBITMQ_URL` | URL RabbitMQ | `amqp://payments:payments@localhost:5672/` |
| `OUTBOX_POLL_INTERVAL` | Пауза опроса outbox при отсутствии событий или ошибке, секунды | `1` |
| `WEBHOOK_TIMEOUT` | Общий таймаут отправки webhook и таймаут операций HTTP, секунды | `10` |

В Docker адреса БД и брокера автоматически заменяются на имена сервисов.
Все прикладные эндпоинты требуют `X-API-Key`; публичные Swagger/OpenAPI отключены.

## API

В примерах используйте значение `API_KEY` из `.env`:

```bash
export API_KEY='local-development-key'

curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-123' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "123.45",
    "currency": "RUB",
    "description": "Оплата заказа 123",
    "metadata": {"order_id": 123},
    "webhook_url": "https://your-service.example/webhook"
  }'
```

`webhook_url` замените адресом своего получателя, доступным из контейнера consumer.
`localhost` внутри consumer обозначает сам контейнер.

Ответ `202 Accepted`:

```json
{
  "payment_id": "53762798-3c23-44c0-9522-55419b18df53",
  "status": "pending",
  "created_at": "2026-10-17T07:00:00Z"
}
```

Сумма должна быть положительной, не более 18 цифр всего и 2 после запятой;
допустимые валюты: `RUB`, `USD`, `EUR`. Описание обязательно, метаданные по умолчанию `{}`.
Webhook URL принимает схемы HTTP и HTTPS. Денежные значения в JSON-ответах представлены строками.

```bash
curl http://localhost:8000/api/v1/payments/53762798-3c23-44c0-9522-55419b18df53 \
  -H "X-API-Key: $API_KEY"
```

GET возвращает `payment_id`, сумму, валюту, описание, метаданные, статус, webhook URL,
`created_at` и `processed_at` (`null` до завершения обработки).
Внутренние поля idempotency key и отметка доставки webhook в ответ не включаются.

Повтор POST с тем же `Idempotency-Key` и теми же данными возвращает `202` и существующий
платёж с текущим статусом. Идемпотентность действует и для конкурентных запросов.
Другие данные с тем же ключом дают `409`. Длина ключа: 1–255 символов.

Ошибки: `401` — отсутствующий/неверный API ключ, `404` — платёж не найден,
`409` — конфликт idempotency key, `422` — некорректные данные или отсутствующий Idempotency-Key.

## Обработка, outbox и webhook

Платёж со статусом `pending` и событие outbox сохраняются одной транзакцией.
Фоновая задача API выбирает событие с `FOR UPDATE SKIP LOCKED`, отправляет persistent-сообщение
в `payments.new` и только после publisher confirm отмечает событие отправленным.
Неотправленные события повторно публикуются после восстановления БД/брокера.
Outbox хранится в БД и автоматически не очищается.

Один consumer выполняет все этапы: эмуляция платежа за 2–5 секунд (90% `succeeded`, 10% `failed`),
сохранение результата и времени обработки, затем POST на webhook URL:

```json
{
  "payment_id": "53762798-3c23-44c0-9522-55419b18df53",
  "status": "succeeded",
  "processed_at": "2026-10-17T07:00:03+00:00"
}
```

Результат `failed` также отправляется клиенту и сам по себе не является причиной retry.
Результат фиксируется до HTTP-запроса. Повтор сообщения не эмулирует платёж заново;
блокировка строки защищает от конкурентной обработки. После успешного webhook записывается
`webhook_sent_at`, и последующие доставки становятся пустой операцией.

Для успешного уведомления нужен HTTP `2xx`. Редиректы не выполняются, тело ответа не читается.
Время отправки ограничено общим таймаутом `WEBHOOK_TIMEOUT`.
Ошибки обработки, сетевые ошибки, таймауты и другие HTTP-статусы запускают retry:

1. Первая попытка → при ошибке очередь `payments.retry.1`, задержка 1 секунда.
2. Вторая попытка → при ошибке очередь `payments.retry.2`, задержка 2 секунды.
3. Третья попытка → при ошибке очередь `payments.dlq`.

Задержки реализованы TTL и dead-letter маршрутизацией обратно в `payments.new`.
Исходное сообщение подтверждается после успешной обработки либо подтверждённой публикации
в retry/DLQ. Если публикация не подтверждена, исходное сообщение возвращается в очередь.
Очереди durable, quorum; для dead-letter переходов включён режим `at-least-once`
([контракт RabbitMQ](https://www.rabbitmq.com/docs/quorum-queues#dead-lettering)).
Для аварийных переподключений/возвратов сообщения также действует стандартный delivery limit
quorum-очереди RabbitMQ; при его исчерпании сообщение направляется в DLQ.
Некорректные сообщения сразу направляются в DLQ с исходным содержимым.
Содержимое DLQ можно просматривать в RabbitMQ Management; автоматического повторного запуска DLQ нет.

Гарантия доставки — **at least once**. Сбой после публикации, но до фиксации outbox может
дать повторное сообщение. Сбой после принятия webhook получателем, но до записи
`webhook_sent_at` может дать повторный webhook. Получатель должен дедуплицировать
уведомления по `payment_id`. Сохранённый статус платежа не меняется при ошибке webhook;
неудачные уведомления после трёх попыток остаются в DLQ.
Один локальный узел RabbitMQ с томом не обеспечивает отказоустойчивость к потере диска.

## Локальная разработка и миграции

Установите Python 3.12 и uv, скопируйте `.env.example` в `.env`.
Если API/consumer уже запущены в Docker, остановите их перед запуском локальных процессов.

```bash
uv sync --frozen
docker compose up -d postgres rabbitmq
uv run alembic upgrade head
uv run uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8000
# В другом терминале:
uv run python -m app.consumer
```

```bash
uv run alembic current
uv run alembic check
```

## Проверки

Unit-тесты проверяют сервисы через их интерфейсы, а также шлюз и retry-обработчик;
внешние сервисы для них не нужны:

```bash
uv run pytest tests/unit -q
uv run mypy app tests scripts
uv run ruff check app tests migrations scripts
uv run ruff format --check app tests migrations scripts
```

Интеграционные тесты используют **отдельную** БД и vhost. Указанные тестовые таблицы
очищаются перед каждым тестом, а очереди — перед брокерными тестами. Не задавайте рабочую
БД или рабочий vhost. Без полного набора переменных `TEST_*` интеграционные тесты пропускаются.

```bash
docker compose up -d postgres rabbitmq
# Создать один раз:
docker compose exec -T postgres createdb -U payments payments_test
docker compose exec -T rabbitmq rabbitmqctl add_vhost aps_test
docker compose exec -T rabbitmq rabbitmqctl set_permissions -p aps_test payments '.*' '.*' '.*'

export TEST_DATABASE_URL='postgresql+asyncpg://payments:payments@localhost:5432/payments_test'
export TEST_RABBITMQ_URL='amqp://payments:payments@localhost:5672/aps_test'
uv run pytest -q
```

При изменении опубликованных портов скорректируйте TEST URL. Миграции тестовой БД
применяются автоматически. Тесты запускайте последовательно, без pytest-xdist.

Покрыты авторизация, валидация, конкурентная идемпотентность и конфликты, атомарность
payment/outbox, восстановление публикации, дубли после сбоя подтверждения, отсутствие
маршрута в RabbitMQ, повторная доставка, неизменность результата при ошибке webhook,
три попытки и DLQ, реальные TTL-задержки. Моки HTTPX/FastStream основаны на контрактах
зафиксированных в lock-файле версий; PostgreSQL и RabbitMQ в интеграциях настоящие.

Сквозная проверка с реальным HTTP-получателем webhook внутри API-контейнера:

```bash
docker compose up --build -d --wait
docker compose exec -T api python - < scripts/smoke.py
```

Скрипт временно слушает порт 9000 внутри контейнера, проверяет авторизацию, валидацию,
конкурентное создание одного платежа, конфликт ключа, обработку, webhook и GET.
Он создаёт один платёж с уникальным ключом в основной БД приложения.
