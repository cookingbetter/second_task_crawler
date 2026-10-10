"""Локальный тестовый сервер: тесты не обращаются в интернет."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import AsyncIterator

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

OK_BODY = "<html><body><h1>Hello, crawler!</h1></body></html>"
CHUNK = b"A" * 1000
CHUNKED_SIZE = len(CHUNK) * 2
NO_CHARSET_BODY = "<html>без charset в заголовке</html>"
HITS_KEY = web.AppKey("hits", Counter)

EXTERNAL_LINK = "https://external.example.org/outside"

PAGE_BODY = f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <title>  Тестовая   страница  </title>
  <meta name="description" content="Страница для проверки парсера">
  <meta name="keywords" content="краулер, парсинг , , тесты">
  <meta property="og:title" content="OG заголовок">
  <link rel="canonical" href="/page">
  <style>.hidden {{ display: none }}</style>
  <script>var tracker = "мусор из script";</script>
</head>
<body>
  <h1>Главный заголовок</h1>
  <div class="content">
    <h2>Подзаголовок</h2>
    <p>Основной    текст страницы.</p>
    <h3>Мелкий заголовок</h3>
  </div>
  <noscript>включите javascript</noscript>

  <a href="/about">абсолютный путь</a>
  <a href="nested/page.html">относительный путь</a>
  <a href="/about#top">дубль с якорем</a>
  <a href="#section">только якорь</a>
  <a href="mailto:crawler@example.org">почта</a>
  <a href="javascript:void(0)">скрипт</a>
  <a href="{EXTERNAL_LINK}">внешняя ссылка</a>

  <img src="/static/logo.png" alt="Логотип">
  <img src="photo.jpg">
  <img alt="картинка без src">

  <table>
    <tr><th>Город</th><th>Страниц</th></tr>
    <tr><td>Москва</td><td>12</td></tr>
    <tr><td>Казань</td><td>7</td></tr>
  </table>

  <ul><li>первый</li><li>второй</li></ul>
  <ol><li>шаг 1</li><li>шаг 2</li><li>шаг 3</li></ol>
</body>
</html>
"""

# Незакрытые теги, оборванный атрибут и мусор после разметки.
BROKEN_BODY = """<html><body><p>Текст без закрытия
<div class="x"><a href=/broken/link>ссылка<span>
<ul><li>пункт
</body>&&&<<>>"""


async def handle_ok(request: web.Request) -> web.Response:
    return web.Response(text=OK_BODY, content_type="text/html")


async def handle_slow(request: web.Request) -> web.Response:
    delay = float(request.query.get("delay", "0.3"))
    await asyncio.sleep(delay)
    return web.Response(text=f"slept {delay}", content_type="text/plain")


async def handle_status(request: web.Request) -> web.Response:
    code = int(request.match_info["code"])
    return web.Response(status=code, text=f"status {code}")


async def handle_chunked(request: web.Request) -> web.StreamResponse:
    """Отдаёт тело двумя чанками с паузой — как медленный реальный сервер."""
    response = web.StreamResponse(
        headers={"Content-Type": "text/html; charset=utf-8"}
    )
    await response.prepare(request)
    await response.write(CHUNK)
    await asyncio.sleep(0.2)
    await response.write(CHUNK)
    await response.write_eof()
    return response


async def handle_no_charset(request: web.Request) -> web.Response:
    """Content-Type без charset: aiohttp не может определить кодировку сам."""
    return web.Response(
        body=NO_CHARSET_BODY.encode("utf-8"),
        content_type="application/octet-stream",
    )


async def handle_page(request: web.Request) -> web.Response:
    """Насыщенная страница: ссылки, мета-теги, изображения, таблица, списки."""
    return web.Response(text=PAGE_BODY, content_type="text/html")


async def handle_broken(request: web.Request) -> web.Response:
    return web.Response(text=BROKEN_BODY, content_type="text/html")


async def handle_empty(request: web.Request) -> web.Response:
    return web.Response(text="", content_type="text/html")


async def handle_counted(request: web.Request) -> web.Response:
    request.app[HITS_KEY]["/counted"] += 1
    return web.Response(text="counted")


@pytest.fixture
async def server() -> AsyncIterator[TestServer]:
    """Поднимает aiohttp-приложение на случайном порту на время теста."""
    app = web.Application()
    app[HITS_KEY] = Counter()
    app.router.add_get("/ok", handle_ok)
    app.router.add_get("/slow", handle_slow)
    app.router.add_get("/status/{code}", handle_status)
    app.router.add_get("/chunked", handle_chunked)
    app.router.add_get("/nocharset", handle_no_charset)
    app.router.add_get("/counted", handle_counted)
    app.router.add_get("/page", handle_page)
    app.router.add_get("/broken", handle_broken)
    app.router.add_get("/empty", handle_empty)

    test_server = TestServer(app)
    await test_server.start_server()
    try:
        yield test_server
    finally:
        await test_server.close()


@pytest.fixture
def url(server: TestServer):
    """Собирает абсолютный URL к тестовому серверу по относительному пути."""

    def _url(path: str) -> str:
        return str(server.make_url(path))

    return _url


@pytest.fixture
def hits(server: TestServer) -> Counter:
    """Счётчик обращений к маршрутам, которые его ведут."""
    return server.app[HITS_KEY]
