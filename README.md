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
│   ├── crawler.py     # AsyncCrawler — асинхронный HTTP-клиент
│   ├── parser.py      # HTMLParser — парсинг HTML и извлечение данных
│   ├── models.py      # FetchResult, CrawlStats
│   ├── utils.py       # логирование, замер времени, вывод таблиц и сводок
│   └── main.py        # демо-скрипт
├── tests/
│   ├── conftest.py    # локальный тестовый сервер (без выхода в интернет)
│   ├── test_main.py   # тесты HTTP-клиента
│   └── test_parser.py # тесты парсинга
└── docs/
    ├── algorithm.md   # день 1: асинхронная загрузка
    └── parsing.md     # день 2: парсинг HTML
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
