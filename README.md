# Комплексное задание: Асинхронный веб-краулер (концептуально усложнённый вариант)

🎯 Цель проекта — разработать высокопроизводительный асинхронный веб-краулер, способный эффективно собирать данные с множества веб-страниц параллельно, обрабатывать ошибки, соблюдать правила вежливости и сохранять результаты. Все части проекта должны быть реализованы с использованием асинхронного программирования на Python (asyncio, aiohttp).

Проект состоит из 7 этапов, соответствующих 7 дням разработки, и финальной интеграции.

## 📋 Общая концепция проекта

Вы разрабатываете модульный асинхронный краулер, поддерживающий:

- 🌐 параллельную загрузку множества веб-страниц
- 📄 парсинг HTML-контента с извлечением данных
- ⚡ управление конкурентностью и ограничение скорости запросов
- 🔄 обработку ошибок и автоматические повторы
- 💾 сохранение данных в файлы и базы данных
- 🤖 соблюдение robots.txt и задержек между запросами
- 🔧 расширяемость для различных типов сайтов и данных

## 🏗️ Ключевые концепции

В проекте должны учитываться:

- ⚡ асинхронное программирование (async/await)
- 🔄 конкурентность и параллелизм
- 💾 управление ресурсами (connection pooling)
- ⚠️ обработка исключений в асинхронном контексте
- 📝 структурированное логирование
- 🧪 тестирование асинхронного кода
- ⚡ производительность и оптимизация

## 📦 Структура проекта

```
second_task_crawler/
├── README.md
├── requirements.txt
├── pytest.ini
├── crawler.py         # точка входа из задания: python crawler.py --urls ...
├── config.example.yaml  # пример конфигурации с комментариями
├── src/
│   ├── crawler.py     # AsyncCrawler — HTTP-клиент и обход сайта
│   ├── parser.py      # HTMLParser — парсинг HTML и извлечение данных
│   ├── queue.py       # CrawlerQueue — очередь URL с приоритетами
│   ├── semaphore.py   # SemaphoreManager — лимиты конкурентности
│   ├── ratelimit.py   # RateLimiter — лимит скорости, задержки, backoff
│   ├── robots.py      # RobotsParser — разбор и соблюдение robots.txt
│   ├── sitemap.py     # SitemapParser — карты сайта и индексы карт
│   ├── errors.py      # классификация ошибок: временные, постоянные, сетевые
│   ├── retry.py       # RetryStrategy — повторы с экспоненциальным backoff
│   ├── circuit.py     # CircuitBreaker — отключение упавшего домена
│   ├── filters.py     # UrlFilter — фильтрация URL для очереди
│   ├── storage.py     # DataStorage: JSON, CSV, SQLite, MultiStorage
│   ├── config.py      # CrawlerConfig — конфигурация из YAML или JSON
│   ├── stats.py       # CrawlerStats — статистика прогона, JSON и HTML-отчёт
│   ├── advanced.py    # AdvancedCrawler — финальная интеграция всех дней
│   ├── benchmark.py   # замер производительности против синхронного обхода
│   ├── models.py      # FetchResult, PageRecord, CrawlStats
│   ├── utils.py       # логирование, замер времени, вывод и сохранение
│   └── main.py        # демо-скрипт
├── tests/
│   ├── conftest.py    # локальный тестовый сервер (без выхода в интернет)
│   ├── test_main.py   # тесты HTTP-клиента
│   ├── test_parser.py # тесты парсинга
│   ├── test_queue.py  # тесты очереди, семафоров и фильтров
│   ├── test_crawl.py  # тесты обхода сайта
│   ├── test_ratelimit.py  # тесты лимита скорости и задержек
│   ├── test_robots.py     # тесты разбора robots.txt
│   ├── test_politeness.py # тесты вежливости в обходе
│   ├── test_errors.py     # тесты классификации ошибок
│   ├── test_retry.py      # тесты стратегии повторов
│   ├── test_circuit.py    # тесты circuit breaker
│   ├── test_retry_crawler.py  # тесты повторов в краулере
│   ├── test_storage.py        # тесты хранилищ данных
│   ├── test_storage_crawler.py  # тесты сохранения при обходе
│   ├── test_sitemap.py        # тесты разбора карт сайта
│   ├── test_sitemap_crawler.py  # тесты карт в связке с краулером
│   ├── test_stats.py          # тесты статистики и отчётов
│   ├── test_config.py         # тесты конфигурации
│   ├── test_advanced.py       # тесты интеграции и слияния CLI с конфигом
│   └── test_benchmark.py      # тесты методики замеров
└── docs/
    ├── algorithm.md   # день 1: асинхронная загрузка
    ├── parsing.md     # день 2: парсинг HTML
    ├── queueing.md    # день 3: очередь, конкурентность, обход
    ├── politeness.md  # день 4: rate limiting и robots.txt
    ├── errors.md      # день 5: ошибки, повторы, circuit breaker
    ├── storage.md     # день 6: сохранение в файлы и базу
    └── advanced.md    # день 7: карты сайта, конфиг, отчёты, бенчмарк
```

## 🚀 Как запустить

Установка зависимостей:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Демонстрация параллельной загрузки (день 1):

```bash
python -m src.main                      # параллельная загрузка 8 URL
python -m src.main --sequential         # плюс сравнение с последовательной
python -m src.main --max-concurrent 10 --log-level DEBUG
python -m src.main --urls https://example.com https://httpbin.org/get
```

Демонстрация парсинга (день 2):

```bash
python -m src.main --parse              # загрузка + разбор HTML со сводкой
python -m src.main --parse --same-domain  # только внутренние ссылки
python -m src.main --parse --urls https://example.com
```

Демонстрация обхода сайта (день 3):

```bash
python -m src.main --crawl                          # quotes.toscrape.com, глубина 2, до 20 страниц
python -m src.main --crawl --same-domain --max-depth 1 --max-pages 10
python -m src.main --crawl --exclude '/tag/' '/login'   # не ходить в метки и логин
python -m src.main --crawl --urls https://example.com --max-per-domain 2
python -m src.main --crawl --no-save --progress-interval 0  # без файла и без строки прогресса
```

Результаты обхода по умолчанию складываются в `data/crawl_<время>.json`
(каталог `data/` не версионируется).

Демонстрация вежливости (день 4):

```bash
python -m src.main --robots              # таблица решений robots.txt без обхода
python -m src.main --robots --user-agent HTTrack   # агенту закрыт весь python.org
python -m src.main --polite --max-pages 8 --same-domain  # обход с robots.txt и 2 зап/с
python -m src.main --crawl --respect-robots --rps 1 --jitter 0.3
python -m src.main --crawl --rotate-ua "BotA/1.0" "BotB/1.0" --rps 2
```

Демонстрация повторов и обработки ошибок (день 5):

```bash
python -m src.main --retry                 # адреса с 503, 429, 404, таймаутом и мёртвым DNS
python -m src.main --retry --no-retry      # то же без повторов, для сравнения
python -m src.main --retry --max-retries 4 --retry-base-delay 1
python -m src.main --crawl --max-pages 6 --circuit-breaker --circuit-threshold 3
```

Отчёт об ошибках режима `--retry` складывается в `data/errors_<время>.json`.

Демонстрация сохранения данных (день 6):

```bash
python -m src.main --store                 # обход + JSON, CSV и SQLite + чтение данных обратно
python -m src.main --store --max-pages 30 --batch-size 10
python -m src.main --crawl --storage sqlite --max-pages 10
python -m src.main --crawl --storage jsonl csv --storage-path data/my_crawl
python -m src.main --parse --storage json --save-failed
```

Файлы хранилищ по умолчанию складываются в `data/pages_<время>.{json,csv,db}`.

Продвинутые возможности и финальная интеграция (день 7):

```bash
python crawler.py --config config.example.yaml       # весь прогон по конфигурации
python crawler.py --urls https://example.com --max-pages 100 --output results.json
python -m src.main --advanced --sitemap auto --report data/report.html
python -m src.main --crawl --sitemap https://www.sitemaps.org/sitemap.xml --max-pages 5
python -m src.main --crawl --report data/report.html --stats-json data/stats.json
python -m src.main --benchmark --benchmark-pages 100 500 1000
python -m src.main --advanced --log-file logs/crawler.log --rate-limit 2
```

Корневой `crawler.py` — то же приложение, что `python -m src.main`, с одним
отличием: без явного флага режима он обходит сайт через `AdvancedCrawler` (день
7), а не загружает список адресов (день 1). Поэтому команда из задания
обходит сайт и сохраняет результат, а не игнорирует два флага из трёх.

Тесты (работают оффлайн, поднимают локальный `aiohttp`-сервер):

```bash
pytest -q
```

## 🧩 Использование `AsyncCrawler`

```python
import asyncio
from src.crawler import AsyncCrawler


async def main():
    async with AsyncCrawler(max_concurrent=5) as crawler:
        results = await crawler.fetch_urls([
            "https://example.com",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/2",
        ])
        print(f"Загружено {len(results)} страниц")
        print(crawler.stats)


asyncio.run(main())
```

Основные методы:

| Метод | Возвращает | Поведение при ошибке |
| --- | --- | --- |
| `fetch_url(url)` | текст страницы | поднимает `FetchError` |
| `fetch_one(url)` | `FetchResult` | ошибка внутри результата, исключений нет |
| `fetch_urls(urls)` | `dict[url, html]` | неуспешные URL просто не попадают в словарь |
| `fetch_and_parse(url)` | `dict` с данными страницы | та же форма словаря, `ok=False` |
| `fetch_and_parse_many(urls)` | `list[dict]` | ошибки видны в полях `ok`/`error_type` |
| `crawl(start_urls, max_pages)` | `list[dict]` | неудачные страницы тоже в списке, с `ok=False` |
| `close()` | — | идемпотентно, закрывает сессию и пул |

Подробности алгоритма, таблица таймаутов и классификация ошибок — в
[docs/algorithm.md](docs/algorithm.md).

## 🧩 Парсинг страниц (День 2)

```python
import asyncio
from src.crawler import AsyncCrawler
from src.parser import HTMLParser


async def main():
    parser = HTMLParser(same_domain_only=True)
    async with AsyncCrawler(max_concurrent=5, parser=parser) as crawler:
        page = await crawler.fetch_and_parse("https://example.com")
        print(page["title"], page["text_length"], page["links_count"])
        print(page["metadata"])


asyncio.run(main())
```

`fetch_and_parse` возвращает словарь с полями `url`, `final_url`, `title`,
`text`, `text_length`, `links`, `links_count`, `images`, `images_count`,
`headings`, `tables`, `lists`, `metadata`, `warnings`, а также данными загрузки
(`status`, `elapsed`, `truncated`, `ok`, `error`, `error_type`). Форма словаря
одинакова и при успехе, и при ошибке — проверять нужно только `ok`.

Методы `HTMLParser` (`extract_links`, `extract_text`, `extract_metadata`,
`extract_images`, `extract_headings`, `extract_tables`, `extract_lists`),
правила нормализации ссылок и политика частичных результатов описаны в
[docs/parsing.md](docs/parsing.md).

## 🧩 Обход сайта (День 3)

```python
import asyncio
from src.crawler import AsyncCrawler


async def main():
    async with AsyncCrawler(max_concurrent=10, max_depth=2) as crawler:
        results = await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=50,
            same_domain_only=True,
        )
        print(f"Обработано: {len(results)} страниц")
        print(crawler.get_crawl_stats())


asyncio.run(main())
```

Обход делает пул из `max_concurrent` воркеров, которые берут URL из
`CrawlerQueue`: найденные ссылки с глубины `d` попадают в очередь с глубиной
`d + 1` и приоритетом `-(d + 1)`, поэтому мелкие уровни разбираются раньше.
Конкурентность держат два семафора — общий и на домен (домен считается по
нормализованному ключу, так что `example.com` и `www.example.com` делят один
лимит).

Чтобы одна страница не попала в результаты дважды, URL нормализуются (регистр
схемы и хоста, порт по умолчанию, пустой путь и запрос, фрагмент), а адрес после
редиректа запоминается: ссылку на цель редиректа краулер уже не загрузит.

| Параметр `crawl` | Смысл |
| --- | --- |
| `max_pages` | жёсткий лимит загрузок, соблюдается точно |
| `max_depth` | глубина обхода; стартовые URL — глубина 0 |
| `same_domain_only` | не выходить за домены стартовых URL (`www.` и порт игнорируются) |
| `include_patterns` / `exclude_patterns` | регулярные выражения по URL; запрет сильнее разрешения |
| `on_progress`, `progress_interval` | callback со статистикой в реальном времени |

Состояние обхода доступно после запуска:

| Поле | Содержимое |
| --- | --- |
| `visited_urls: set` | URL, которые забрали из очереди и загрузили |
| `processed_urls: dict` | URL -> разобранная страница |
| `failed_urls: dict` | URL -> текст ошибки |
| `skipped: Counter` | пропуски ссылок: `depth`, `filtered`, `duplicate`, `closed`, `robots` |
| `blocked_urls: dict` | URL -> правило robots.txt, из-за которого его не загрузили |
| `queue`, `semaphores` | `CrawlerQueue` и `SemaphoreManager` со своими `get_stats()` |
| `rate_limiter`, `robots` | `RateLimiter` и `RobotsParser` со своими `get_stats()` |

Протокол завершения очереди (почему пустая очередь — ещё не конец обхода),
порядок захвата семафоров и правила фильтров разобраны в
[docs/queueing.md](docs/queueing.md).

## 🧩 Вежливость: rate limiting и robots.txt (День 4)

```python
import asyncio
from src.crawler import AsyncCrawler


async def main():
    async with AsyncCrawler(
        max_concurrent=5,
        requests_per_second=2.0,   # 2 запроса в секунду на домен
        respect_robots=True,       # соблюдать robots.txt
        min_delay=0.5,             # минимум 0.5 с между запросами
        jitter=0.2,                # случайная добавка к паузе
        user_agent="MyBot/1.0",
    ) as crawler:
        pages = await crawler.crawl(["https://quotes.toscrape.com/"], max_pages=20)
        print(f"Обработано: {len(pages)}, запрещено robots.txt: {len(crawler.blocked_urls)}")
        print(crawler.rate_limiter.get_stats())


asyncio.run(main())
```

По умолчанию вежливость **выключена** (`requests_per_second=None`,
`respect_robots=False`), поэтому простая загрузка списка URL из дней 1-2 не
становится медленной неожиданно.

| Параметр | Смысл |
| --- | --- |
| `requests_per_second` | лимит скорости; `None`/`0` — без собственного лимита |
| `per_domain_rate` | лимит отдельный для каждого домена (по умолчанию) или общий |
| `min_delay` | минимальная пауза между запросами к домену |
| `jitter` | случайная добавка к паузе, только вверх |
| `respect_robots` | проверять robots.txt перед каждым запросом |
| `robots_allow_on_error` | считать сайт открытым, если robots.txt не загрузился |
| `backoff_factor`, `max_backoff` | насколько и до какого предела замедляться после ошибок |
| `user_agent`, `user_agents` | основной агент и список для ротации по кругу |

Задержка между запросами — максимум из `1/requests_per_second`, `min_delay` и
`Crawl-delay` из robots.txt, плюс штраф за ошибки и `jitter`. Отсчитывается она
от времени последнего запроса, поэтому новое требование действует сразу: первая
же страница сайта с `Crawl-delay: 30` ждёт полминуты. Лимитер стоит перед
семафорами, поэтому ожидающая задача не держит слот конкурентности. Замедление
включают только ошибки, похожие на перегрузку (таймаут, сеть, 5xx, 408, 429) —
на 404 скорость не меняется. Штраф ставится один раз на URL, по итогу всех
попыток: паузы между попытками — дело повторов из дня 5.

Темп считается по домену (или общий на весь краулер при
`per_domain_rate=False`), а `Crawl-delay` и штраф — всегда по сайту: при общем
лимите домены делят одни часы, но не чужие задержки. robots.txt же кэшируется по
серверу (схема, хост, порт): `host:8001` и `host:8002` — разные сервисы со
своими правилами.

Запрещённые robots.txt URL не считаются ошибками: они попадают в
`blocked_urls` (URL -> сработавшее правило) и `skipped["robots"]`, не тратят
бюджет `max_pages` и пишутся в лог вместе с правилом и агентом.

| Класс / метод | Назначение |
| --- | --- |
| `RateLimiter.acquire(domain)` | дождаться разрешения на запрос |
| `RateLimiter.penalize/reward(domain)` | замедлить домен после ошибки / ослабить штраф |
| `RobotsParser.fetch_robots(base_url)` | загрузить robots.txt сервера (кэш на схему, хост и порт) |
| `RobotsParser.can_fetch(url, user_agent)` | проверить URL по загруженным правилам |
| `RobotsParser.get_crawl_delay(user_agent, url=...)` | задержка из robots.txt |

Свой парсер robots.txt поддерживает группы `User-agent`, шаблоны `*` и `$`,
приоритет самого длинного правила и `Crawl-delay`; `urllib.robotparser` из
стандартной библиотеки сравнивает только префиксы и такие правила пропускает.
Группа выбирается по имени краулера — тексту до первого `/`, — поэтому слова из
ссылки в User-Agent не притягивают чужие запреты.
Подробности — в [docs/politeness.md](docs/politeness.md).

## 🧩 Ошибки и повторы (День 5)

```python
import asyncio
from src.crawler import AsyncCrawler
from src.errors import NetworkError, TransientError
from src.retry import RetryStrategy


async def main():
    retry_strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=2.0,
        retry_on=[TransientError, NetworkError],
    )
    async with AsyncCrawler(retry_strategy=retry_strategy) as crawler:
        # Таймаут и 503 будут повторены, 404 — нет.
        result = await crawler.fetch_one("https://httpbin.org/status/503")
        print(result.error_class, result.attempts)
        print(crawler.get_error_report())


asyncio.run(main())
```

Повторы **включены по умолчанию** и умеренно: `max_retries=2`, базовая пауза
0.5 с, экспоненциальный рост. Временная ошибка не должна стоить страницы, но и
долбить сайт краулер не станет. Circuit breaker по умолчанию **выключен**.

| Ответ или исключение | Класс ошибки | Повтор |
| --- | --- | --- |
| 429 | `RateLimitError` | да, с увеличенной паузой и учётом `Retry-After` |
| 500, 502, 503, 504 | `ServerError` | да, на один раз меньше обычного |
| 408, 425 | `TransientError` | да |
| таймаут | `CrawlerTimeoutError` | да, таймаут следующей попытки больше |
| DNS, connection refused | `NetworkError` | да |
| 401, 403, 404, 410 | `PermanentError` | нет |
| отказ разбора страницы | `ParseError` | нет |
| отказ circuit breaker'а | `CircuitOpenError` | нет |
| запрет robots.txt | `RobotsBlockedError` | нет |

| Параметр | Смысл |
| --- | --- |
| `max_retries` | максимум повторов запроса (0 — без повторов) |
| `retry_base_delay`, `retry_backoff_factor` | базовая пауза и во сколько раз она растёт |
| `retry_max_delay`, `retry_jitter` | предел паузы и случайная добавка |
| `timeout_growth`, `timeout_max_total` | насколько растут таймауты с попытками и их потолок |
| `circuit_breaker` | отключать домен, который отдаёт одни ошибки |
| `circuit_threshold`, `circuit_window`, `circuit_recovery` | порог неудачных запросов, окно и время до пробного запроса |
| `retry_strategy`, `breaker` | готовые `RetryStrategy` и `CircuitBreaker` вместо скаляров |

Готовый объект настраивает себя сам, поэтому переданные рядом с ним скаляры
краулер не применяет и предупреждает о каждом из них в логе.

`FetchResult` и страницы обхода получили `attempts`, `error_class` и
`retry_waited`, а `get_crawl_stats()` — разделы `retry`, `circuit` и `errors`.
Отчёт `get_error_report()` содержит распределение ошибок по типам и классам,
число спасённых повторами URL и список постоянных ошибок.

| Класс / метод | Назначение |
| --- | --- |
| `RetryStrategy.execute_with_retry(coro, *args)` | выполнить с повторами по классу ошибки |
| `RetryStrategy.policy_for(error)` / `delay_for(error, attempt)` | политика и пауза для ошибки |
| `RetryStrategy.get_stats()` / `reset_stats()` | статистика повторов |
| `CircuitBreaker.allow(url)` | можно ли сейчас идти к домену |
| `CircuitBreaker.record_success/record_failure(url, error)` | сообщить об исходе запроса |
| `AsyncCrawler.timeout_for_attempt(attempt)` | таймауты конкретной попытки |
| `AsyncCrawler.get_error_report()` | сводка по ошибкам последнего запуска |
| `errors.from_exception(exc)` / `from_http_status(status)` | классификация ошибки |

Подробности, формулы backoff и осознанные ограничения —
в [docs/errors.md](docs/errors.md).

## 🧩 Сохранение данных (День 6)

```python
import asyncio
from src.crawler import AsyncCrawler
from src.storage import JSONStorage, MultiStorage, SQLiteStorage


async def main():
    # Один формат...
    json_storage = JSONStorage("results.json")
    # ...или сразу несколько
    storage = MultiStorage([json_storage, SQLiteStorage("crawler.db")])

    crawler = AsyncCrawler(storage=storage)
    await crawler.crawl(start_urls=["https://example.com"], max_pages=20)
    await crawler.close()  # досылает буфер и закрывает хранилища

    print(await json_storage.load(limit=5))


asyncio.run(main())
```

Хранилища по умолчанию **не подключены**: писать файлы — не то решение, которое
краулер принимает за пользователя. Сохраняются разобранные страницы, неудачные —
только по просьбе (`storage_save_failed=True`).

| Формат | Класс | Когда выбирать |
| --- | --- | --- |
| JSON-массив | `JSONStorage(path)` | прочитать данные целиком одной строкой кода |
| JSONL | `JSONStorage(path, lines=True)` | длинный обход и дописывание между запусками |
| CSV | `CSVStorage(path, encoding=...)` | открыть в редакторе, залить в таблицу |
| SQLite | `SQLiteStorage(path)` | искать, фильтровать и обновлять страницы |
| несколько сразу | `MultiStorage([...])` | одни данные нужны в разных видах |

| Параметр | Смысл |
| --- | --- |
| `storage` | объект `DataStorage`, куда уходит каждая разобранная страница |
| `storage_save_failed` | сохранять и неудачные страницы (по умолчанию нет) |
| `close_storage` | закрывать хранилище вместе с краулером (по умолчанию да) |
| `batch_size` | сколько записей копить перед записью (файлы 1, SQLite 50) |
| `text_limit` | обрезать текст страницы до N символов |
| `encoding`, `errors`, `delimiter` | кодировка и разделитель для CSV |
| `lines`, `indent` | режим JSONL и отступы JSON |

| Класс / метод | Назначение |
| --- | --- |
| `DataStorage.save(page)` | сохранить страницу; `False` вместо исключения при сбое (с батчем `True` значит «принято в буфер») |
| `DataStorage.save_many(pages)` / `flush()` | записать пачку / досрочно сбросить буфер |
| `DataStorage.load(limit)` | прочитать сохранённое обратно |
| `DataStorage.close()` / `get_stats()` | дописать остаток и закрыть / статистика записи |
| `SQLiteStorage.init_db()` | создать таблицу и индексы заранее |
| `SQLiteStorage.count()` / `search(text)` | сколько страниц в базе / поиск по тексту |
| `PageRecord.from_page(page)` | стандартная форма записи из словаря страницы |

Ошибка записи не останавливает обход: она попадает в лог и в счётчик `failed`,
а временные сбои (`OSError`) повторяются стратегией дня 5. `FetchResult` и
страницы получили `content_type` и `fetched_at`, а `get_crawl_stats()` — раздел
`storage`.

Схема таблицы, цена батчей и осознанные ограничения —
в [docs/storage.md](docs/storage.md).

## 🧩 Продвинутые возможности (День 7)

```python
import asyncio
from src.advanced import AdvancedCrawler


async def main():
    crawler = AdvancedCrawler.from_config("config.yaml")
    await crawler.crawl()

    stats = crawler.get_stats()
    print(f"Обработано: {stats['total_pages']}")
    print(f"Успешно: {stats['successful']}")
    print(f"Ошибок: {stats['failed']}")

    crawler.export_to_html_report("report.html")
    await crawler.close()


asyncio.run(main())
```

`AdvancedCrawler` — композиция, а не наследник `AsyncCrawler`: он решает, что
обходить и что делать с результатом, а краулер остаётся библиотекой, которую
можно взять без конфигов и отчётов. `from_config` настраивает ещё и
логирование — это точка входа приложения; прямой конструктор корневой логгер не
трогает.

| Метод | Назначение |
| --- | --- |
| `AdvancedCrawler.from_config(path)` | собрать прогон по YAML или JSON |
| `crawl(start_urls=None)` | обойти сайт: адреса из аргумента, карты сайта или конфига |
| `resolve_start_urls()` | только выбор стартовых адресов, без обхода |
| `get_stats()` | сводка словарём: `total_pages`, `successful`, `failed`, скорость, домены |
| `export_to_json(file)` / `export_to_html_report(file)` | отчёты (путь можно взять из конфига) |
| `export_reports()` | записать те отчёты, что заданы в конфигурации |
| `close()` | закрыть хранилища и сессию (есть и `async with`) |

### Карта сайта как источник адресов

```python
async with AsyncCrawler(parser=HTMLParser()) as crawler:
    # Прямо по адресу карты...
    urls = await crawler.sitemap.fetch_sitemap("https://example.com/sitemap.xml")
    # ...или найти её самому и получить готовые стартовые адреса
    seeds = await crawler.seeds_from_sitemaps(["auto"], base_urls=["https://example.com/"])
```

Индексные карты разворачиваются рекурсивно, сжатые (`sitemap.xml.gz`)
распаковываются, адреса идут по убыванию `<priority>` (без него — 0.5 по
спецификации), дубли отбрасываются. Пределы:
10 МБ на файл карты — и до, и после распаковки, иначе крошечный архив
развернулся бы в память гигабайтами, —
глубина индексов 3, 50 файлов, 50 000 адресов и множество уже загруженных —
карта, ссылающаяся сама на себя, загружается один раз. Запрос карты идёт мимо
проверки robots.txt (иначе для самого robots.txt вышла бы рекурсия), но через
лимит скорости и семафоры.

| Метод | Назначение |
| --- | --- |
| `SitemapParser.fetch_sitemap(url)` | список адресов страниц из карты |
| `SitemapParser.fetch_entries(urls)` | записи с `lastmod`, `changefreq`, `priority` |
| `SitemapParser.get_stats()` | файлы, индексы, адреса, дубли, ошибки, `truncated` |
| `AsyncCrawler.discover_sitemaps(url)` | карты из robots.txt или `/sitemap.xml` |
| `AsyncCrawler.seeds_from_sitemaps(urls, base_urls=, limit=)` | стартовые адреса по приоритету |
| `RobotsParser.sitemaps(url)` | адреса карт из директив `Sitemap:` |

### Статистика и отчёты

```python
from src.stats import CrawlerStats

stats = CrawlerStats.from_crawler(crawler, pages)
print(stats.get_stats()["by_status"], stats.top_domains(limit=5))
stats.export_to_json("data/stats.json")
stats.export_to_html_report("data/report.html")
```

`CrawlStats` (день 1) — итог одной загрузки, `CrawlerStats` — отчёт по целому
прогону: статус-коды, типы ошибок, глубина, типы содержимого, топ доменов,
скорость, время и настройки, которыми прогон получен. Своего состояния класс не
ведёт: тот же прогон можно посчитать дважды и получить тот же отчёт.

HTML-отчёт — один самодостаточный файл: инлайновый CSS, диаграммы на `div`-ах,
никакого JS и ни одной внешней ссылки, чтобы он открывался с диска и без сети.
Всё пользовательское проходит через `html.escape`, таблица страниц обрезается
до 50 строк.

### Конфигурация

```yaml
start_urls:
  - https://quotes.toscrape.com/
# sitemaps: [auto]        # искать карту в robots.txt, иначе /sitemap.xml

crawler:
  max_pages: 30
  max_depth: 2
politeness:
  rate_limit: 2.0         # синоним requests_per_second
  respect_robots: true
storage_settings:
  storage: [jsonl, sqlite]
  storage_path: data/demo
output:
  report_html: data/report.html
```

Полный пример с комментариями — [config.example.yaml](config.example.yaml).
Разделы нужны только для читаемости: набор ключей плоский, имена совпадают с
флагами CLI, поэтому конфиг читается как сохранённая командная строка.
Неизвестный ключ — ошибка с подсказкой («`max_page`, похоже на `max_pages`»): с
предупреждением опечатка стоила бы целого прогона. Одна настройка под двумя
именами (`urls` и `start_urls`) тоже отвергается, а пути отчётов проверяются до
начала обхода.

Приоритет такой: **значение по умолчанию -> пресет -> конфигурация -> явный
флаг**. `--config run.yaml --max-pages 7` оставит 7, а остальное возьмёт из
файла. Пресеты (`--polite`, `--retry`, `--store`) заполняют только незаданные
значения, поэтому `rate_limit` из файла сильнее пресетных 2 зап/с.

### Новые флаги CLI

| Флаг | Смысл |
| --- | --- |
| `--config FILE` | настройки из YAML или JSON; без других режимов включает `--advanced` |
| `--advanced` | прогон через `AdvancedCrawler` с отчётами |
| `--sitemap URL [URL ...]` | брать стартовые адреса из карты; `auto` — найти её по robots.txt |
| `--rate-limit N` | синоним `--rps`: запросов в секунду на домен |
| `--report FILE` | HTML-отчёт о прогоне |
| `--stats-json FILE` | статистика прогона в JSON |
| `--log-file FILE` | писать лог ещё и в файл, с ротацией |
| `--log-max-bytes N`, `--log-backups N` | размер файла лога до ротации и число копий |
| `--benchmark` | сравнить асинхронный обход с синхронным на локальном сайте |
| `--benchmark-pages N [N ...]` | размеры для `--benchmark` (по умолчанию 100, 500, 1000) |

Строка прогресса показывает бар, процент, скорость и ETA:

```
[#####################···]  88% | 7/8 Страниц: 7 | в очереди: 40 | в работе: 1 | 7.0 стр/с | осталось: 3 с
```

Остаток считается как `min(max_pages - сделано, в очереди + в работе)` — обход
закончится по тому пределу, который наступит раньше, поэтому процент может
расти рывками.

### Производительность

`python -m src.main --benchmark` поднимает локальный сайт нужного размера и
обходит его дважды: пулом воркеров на `aiohttp` и последовательно на `urllib`.
Задержка ответа (10 мс) изображает настоящий сайт — без неё замер показывал бы
только накладные расходы asyncio.

| Страниц | Синхронно | Асинхронно | Ускорение |
| --- | --- | --- | --- |
| 100 | 1.57 с (63 стр/с) | 0.51 с (196 стр/с) | x3.1 |
| 300 | 4.84 с (62 стр/с) | 0.84 с (357 стр/с) | x5.8 |

Устройство карт сайта и их пределы, состав статистики, сборка HTML-отчёта,
справочник ключей конфигурации, формула ETA, методика замеров и осознанные
ограничения (прокси, cookies, JS-рендеринг, распределённый обход) —
в [docs/advanced.md](docs/advanced.md).
