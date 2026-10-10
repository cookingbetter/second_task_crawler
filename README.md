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
├── src/
│   ├── crawler.py     # AsyncCrawler — HTTP-клиент и обход сайта
│   ├── parser.py      # HTMLParser — парсинг HTML и извлечение данных
│   ├── queue.py       # CrawlerQueue — очередь URL с приоритетами
│   ├── semaphore.py   # SemaphoreManager — лимиты конкурентности
│   ├── ratelimit.py   # RateLimiter — лимит скорости, задержки, backoff
│   ├── robots.py      # RobotsParser — разбор и соблюдение robots.txt
│   ├── filters.py     # UrlFilter — фильтрация URL для очереди
│   ├── models.py      # FetchResult, CrawlStats
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
│   └── test_politeness.py # тесты вежливости в обходе
└── docs/
    ├── algorithm.md   # день 1: асинхронная загрузка
    ├── parsing.md     # день 2: парсинг HTML
    ├── queueing.md    # день 3: очередь, конкурентность, обход
    └── politeness.md  # день 4: rate limiting и robots.txt
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
на 404 скорость не меняется. Повторов URL в дне 4 нет.

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
