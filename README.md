# Payment Service

Асинхронный сервис процессинга платежей: принимает заявку, отдаёт её в обработку через брокер,
прогоняет через эмуляцию платёжного шлюза и уведомляет клиента webhook'ом.

Тестовое задание в `TASK.md`.

## Стек

FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), asyncpg, PostgreSQL 17, RabbitMQ 4 через FastStream,
Alembic, Docker Compose. Python 3.14.

## Быстрый старт

```bash
cp .env.example .env
docker compose up --build -d
```

Поднимутся `payment_db`, `rabbitmq`, `payment_api` (:8000), `payment_consumer`. Служебный сервис
`migrate` прогонит миграции и завершится — это нормально, он одноразовый.

Проверка:

```bash
curl http://localhost:8000/health
```

RabbitMQ Management UI доступен на <http://localhost:15672> (логин и пароль из `.env`).

> Порт PostgreSQL берётся из `POSTGRES_PORT` в `.env`, наружу публикуется `${POSTGRES_PORT}:5432`.
> Внутри docker-сети адрес всегда `payment_db:5432`.

## Конфигурация

Всё читается из окружения, дефолтов в compose нет — файл `.env` обязателен.

| Переменная | Назначение |
|---|---|
| `API_KEY` | ключ для заголовка `X-API-Key` |
| `DATABASE_URL` / `RABBITMQ_URL` | строки подключения |
| `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE` | период опроса и размер пачки relay |
| `MAX_ATTEMPTS`, `RETRY_BASE_DELAY` | лимит попыток и база экспоненциальной задержки |
| `WEBHOOK_MAX_ATTEMPTS`, `WEBHOOK_TIMEOUT`, `WEBHOOK_BASE_DELAY` | доставка webhook |
| `CIRCUIT_BREAKER_THRESHOLD`, `CIRCUIT_BREAKER_RESET_TIMEOUT` | circuit breaker |
| `GATEWAY_MIN_DELAY`, `GATEWAY_MAX_DELAY`, `GATEWAY_SUCCESS_RATE` | эмуляция шлюза |

Полный список с комментариями — в `.env.example`.

## API

Все эндпоинты требуют `X-API-Key`.

### Создать платёж

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me-before-production" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "1500.50",
    "currency": "RUB",
    "description": "subscription",
    "metadata_json": {"order_id": 42},
    "webhook_url": "https://example.com/hooks/payments"
  }'
```

`202 Accepted`:

```json
{
  "payment_id": "01a13e90-588f-748f-ba7f-3c73a04546f1",
  "status": "pending",
  "amount": "1500.50",
  "currency": "RUB",
  "metadata_json": {"order_id": 42},
  "created_at": "2026-10-15T07:56:49.680980Z",
  "processed_at": null
}
```

### Повтор того же запроса

Тот же `Idempotency-Key` возвращает `200 OK` с заголовком `Idempotent-Replay: true` и тем же
`payment_id`. Платеж не создаётся повторно.

Тот же ключ с другим телом тоже вернёт существующий платёж.

### Получить платёж

```bash
curl http://localhost:8000/api/v1/payments/01a13e90-588f-748f-ba7f-3c73a04546f1 \
  -H "X-API-Key: change-me-before-production"
```

`404`, если платежа нет.

### Webhook

После обработки клиенту уходит POST с телом:

```json
{
  "payment_id": "01a13e90-588f-748f-ba7f-3c73a04546f1",
  "status": "succeeded",
  "event": "payment.succeeded"
}
```

Подпись HMAC не реализована: ТЗ её не требует. Для продакшена её нужно добавить.

## Как это работает

```
POST /payments
      │
      ├─ одна транзакция: INSERT payments (pending) + INSERT outbox_messages
      ▼
   202 Accepted клиенту
      │
      ▼
   relay (в процессе API)
      │  SELECT ... WHERE published_at IS NULL
      │  ORDER BY created_at LIMIT n
      │  FOR UPDATE SKIP LOCKED
      ▼
   RabbitMQ: exchange payments -> queue payments.new (persistent)
      │
      ▼
   consumer
      │  если статус не pending — шлюз не вызывается
      │  иначе эмуляция 2–5 с, 90% успех
      │  UPDATE payments SET status, processed_at
      │  webhook с 3 попытками и backoff
      ▼
   при неудаче: payments.retry.1 → payments.retry.2 → payments.dlq
```

### Outbox

Платёж и событие пишутся одной транзакцией, поэтому невозможно состояние «платёж создан, событие
потеряно» или наоборот. Relay читает только неопубликованные строки под `FOR UPDATE SKIP LOCKED`,
что позволяет нескольким инстансам API работать параллельно, не блокируя друг друга.

### Гарантии доставки

| Секция | Что гарантируется |
|---|---|
| API → БД | атомарно, одна транзакция |
| БД → RabbitMQ | at-least-once: при падении между `publish` и `commit` сообщение уйдёт повторно |
| RabbitMQ → consumer | at-least-once: после 3 попыток — `payments.dlq` |
| Обработка | effectively-once: статус выставляется только из `pending` |

Exactly-once в распределённой системе недостижим без транзакционного координатора между БД и
брокером. Здесь его роль выполняет идемпотентный consumer: повторное сообщение не меняет уже
принятое решение, поэтому повтор безопасен.

Сообщения публикуются с `delivery_mode: 2` (persistent) в durable-очередь, а publisher confirms
включены по умолчанию в aio_pika. Без первого перезапуск RabbitMQ терял бы уже помеченные в outbox
сообщения.

### Повторные попытки

`payments.retry.1` и `payments.retry.2` несут TTL 2 и 4 секунды и `x-dead-letter-exchange`, поэтому
сообщение возвращается в основной обменник силами брокера — consumer при этом не занят ожиданием.
Счётчик попыток едет в заголовке `x-attempt`. После третьей неудачи сообщение уходит в
`payments.dlx` и попадает в `payments.dlq` для разбора.

### Circuit breaker

Если endpoint клиента не отвечает, breaker открывается после пяти неудач подряд и пропускает
доставку без сетевых запросов, пока не истечёт таймаут. Это защищает и сеть, и сам consumer от
пустых вызовов в заведомо мёртвый адрес.

## Схема данных

`payments`: уникальный индекс на `idempotency_key`, индекс на `status`.
`outbox_messages`: индекс на `aggregate_id`, частичный индекс
`WHERE published_at IS NULL` — relay читает только неопубликованные строки.

Идентификаторы — UUIDv7: они монотонны по времени и не раздувают B-tree при вставках, в отличие от
UUIDv4. Деньги хранятся в `Numeric(18, 2)`, `float` не используется нигде.

## Тесты

```bash
docker compose up -d payment_db rabbitmq
pytest
```

Тесты идут по отдельной базе `payments_test` и создают её сами при первом запуске, так что рабочие
данные не затрагиваются.

Требуются обе инфраструктуры: тесты работают против настоящего PostgreSQL и RabbitMQ, схему создаёт
настоящая миграция `alembic upgrade head`, а не `create_all`. Без запущенных сервисов прогон падает,
а не скипается — молчаливо пропущенные тесты хуже упавших.

Покрыты идемпотентность повторов, атомарность платежа и outbox, поведение relay при падении между
`publish` и `commit`, retry-топология с TTL и уходом в DLQ, circuit breaker и эмуляция шлюза.

## Ограничения

- Подпись webhook не реализована.
- Единственный пользователь API, ролей нет.
- `payments.retry.*` нельзя переобъявить с другим TTL: RabbitMQ требует удалить очередь. При смене
  `RETRY_BASE_DELAY` в работающем окружении старые очереди нужно снести вручную.
- Ошибки декодирования сообщения возникают до входа в обработчик и в retry не попадают.
- Размер пула соединений к БД не подобран под конкурентность consumer'а.

## Решения, которые стоит обсудить

**Relay живёт в процессе API, а не отдельным сервисом.** ТЗ требует четырёх сервисов в compose.
Цена: relay конкурирует с API за ресурсы. При росте нагрузки выносится в отдельный процесс без
изменений в коде — он уже изолирован в `app/outbox/relay.py`.

**Обработка платежа — хореографическая saga** без координатора: шаги реагируют на события. Координатор
дал бы единую картину процесса, но здесь шагов два, и координатор был бы лишней сущностью.

**Outbox вместо прямой публикации в брокер.** Прямая публикация после коммита оставляет окно между
транзакцией и брокером, обратный порядок — окно между брокером и транзакцией. Outbox закрывает оба.
