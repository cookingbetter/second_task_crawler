"""Локальный тестовый сервер: тесты не обращаются в интернет."""

from __future__ import annotations

import asyncio
import gzip
from collections import Counter
from collections.abc import AsyncIterator, Callable

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

OK_BODY = "<html><body><h1>Hello, crawler!</h1></body></html>"
CHUNK = b"A" * 1000
CHUNKED_SIZE = len(CHUNK) * 2
NO_CHARSET_BODY = "<html>без charset в заголовке</html>"
HITS_KEY = web.AppKey("hits", Counter)
# Содержимое robots.txt подменяют тесты; None означает «файла нет» (404),
# поэтому тесты дней 1-3 видят сервер точно таким, как раньше.
ROBOTS_KEY = web.AppKey("robots", dict)
# (путь, User-Agent) каждого запроса — для проверки ротации агентов.
AGENTS_KEY = web.AppKey("agents", list)
# Сколько раз просили каждый «нестабильный» адрес: по нему видно повторы.
FLAKY_KEY = web.AppKey("flaky", Counter)

FLAKY_BODY = "<html><body><h1>Наконец-то</h1></body></html>"
# Код, которым тест просит «не отвечать вовремя» вместо ошибки статусом.
FLAKY_TIMEOUT = 0

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

# Граф сайта для тестов обхода: ветвление, цикл назад, битая страница и ссылка
# на чужой домен.
SITE_LINKS: dict[str, list[str]] = {
    "/site/": ["/site/a", "/site/b", "/site/missing", EXTERNAL_LINK],
    # /site/redirect ведёт на /site/b1, на который есть и прямая ссылка:
    # цель редиректа не должна загружаться дважды.
    "/site/a": ["/site/a1", "/site/"],
    "/site/b": ["/site/b1", "/site/redirect"],
    "/site/a1": [],
    "/site/b1": [],
}
REDIRECT_PATH = "/site/redirect"
REDIRECT_TARGET = "/site/b1"

# Карты сайта. Приоритеты заданы вразнобой, чтобы было видно сортировку:
# ожидаемый порядок — /site/b, /site/a1, /site/ (по умолчанию 0.5), /site/a.
SITEMAP_PATH = "/sitemap.xml"
SITEMAP_GZ_PATH = "/sitemap-gz.xml.gz"
SITEMAP_INDEX_PATH = "/sitemap-index.xml"
SITEMAP_PARTS = ("/sitemap-part1.xml", "/sitemap-part2.xml")
SITEMAP_BROKEN_PATH = "/sitemap-broken.xml"
SITEMAP_EMPTY_PATH = "/sitemap-empty.xml"
SITEMAP_LOOP_PATH = "/sitemap-loop.xml"
SITEMAP_MISSING_PATH = "/sitemap-missing.xml"
SITEMAP_BOMB_PATH = "/sitemap-bomb.xml.gz"
# Адресов в сжатой карте: при x200 сжатия крошечный архив разворачивается в
# сотни килобайт — этого хватает, чтобы перебить уменьшенный в тесте лимит.
SITEMAP_BOMB_URLS = 5_000

SITEMAP_ENTRIES: tuple[tuple[str, str | None], ...] = (
    ("/site/a", "0.1"),
    ("/site/b", "0.9"),
    ("/site/", None),
    ("/site/a1", "0.7"),
)
# Полный порядок с учётом относительного адреса (priority 0.4).
SITEMAP_ORDER = ("/site/b", "/site/a1", "/site/", "/site/b1", "/site/a")


def _url_tag(loc: str, priority: str | None) -> str:
    """Один <url> карты сайта; у первого адреса ещё и картинка с тем же <loc>."""
    extra = (
        f"<image:image><image:loc>{loc}/logo.png</image:loc></image:image>"
        if priority == "0.1"
        else ""
    )
    tail = f"<priority>{priority}</priority>" if priority is not None else ""
    return (
        f"<url><loc>{loc}</loc><lastmod>2026-01-02</lastmod>"
        f"<changefreq>daily</changefreq>{tail}{extra}</url>"
    )


def _urlset(*entries: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"'
        ' xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">'
        f"{''.join(entries)}</urlset>"
    )


def _sitemapindex(*locs: str) -> str:
    inner = "".join(
        f"<sitemap><loc>{loc}</loc><lastmod>2026-01-02</lastmod></sitemap>"
        for loc in locs
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        f"{inner}</sitemapindex>"
    )


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


async def handle_redirect(request: web.Request) -> web.Response:
    """Перенаправляет на страницу, на которую есть и прямая ссылка."""
    raise web.HTTPFound(REDIRECT_TARGET)


async def handle_robots(request: web.Request) -> web.Response:
    """Отдаёт robots.txt, который задал тест, или 404, если тест его не задавал."""
    text = request.app[ROBOTS_KEY]["text"]
    if text is None:
        return web.Response(status=404, text="robots.txt не найден")
    return web.Response(text=text, content_type="text/plain")


async def handle_flaky(request: web.Request) -> web.Response:
    """Падает первые `fail` обращений, а потом отдаёт страницу.

    Параметры запроса: `key` — отдельный счётчик для каждого теста, `fail` —
    сколько первых попыток провалить, `status` — каким кодом (0 означает «не
    ответить вовремя»), `retry_after` — значение одноимённого заголовка,
    `delay` — сколько спать в режиме таймаута.
    """
    key = request.query.get("key", "default")
    fail_times = int(request.query.get("fail", "1"))
    status = int(request.query.get("status", "503"))

    attempt = request.app[FLAKY_KEY][key] = request.app[FLAKY_KEY][key] + 1
    if attempt > fail_times:
        return web.Response(text=FLAKY_BODY, content_type="text/html")

    if status == FLAKY_TIMEOUT:
        # Клиент не дождётся ответа и получит таймаут.
        await asyncio.sleep(float(request.query.get("delay", "1.0")))
        return web.Response(text="слишком поздно", content_type="text/html")

    headers = {}
    if (retry_after := request.query.get("retry_after")) is not None:
        headers["Retry-After"] = retry_after
    return web.Response(status=status, text=f"flaky {status}", headers=headers)


@web.middleware
async def record_agent(request: web.Request, handler: Callable) -> web.StreamResponse:
    """Запоминает User-Agent каждого запроса: так видно ротацию агентов."""
    request.app[AGENTS_KEY].append(
        (request.path, request.headers.get("User-Agent", ""))
    )
    return await handler(request)


async def handle_site(request: web.Request) -> web.Response:
    """Страница графа `SITE_LINKS`, считающая обращения к себе."""
    path = request.path
    links = SITE_LINKS.get(path)
    if links is None:
        return web.Response(status=404, text=f"нет такой страницы: {path}")

    request.app[HITS_KEY][path] += 1
    anchors = "".join(f'<a href="{link}">{link}</a>\n' for link in links)
    body = (
        f"<html><head><title>Страница {path}</title></head>"
        f"<body><h1>{path}</h1>{anchors}</body></html>"
    )
    return web.Response(text=body, content_type="text/html")


async def handle_sitemap(request: web.Request) -> web.Response:
    """Обычная карта сайта: абсолютные адреса, один относительный и дубль."""
    request.app[HITS_KEY][request.path] += 1
    origin = str(request.url.origin())
    entries = [
        _url_tag(f"{origin}{path}", priority) for path, priority in SITEMAP_ENTRIES
    ]
    # Относительный <loc> встречается в живых картах, хотя стандарт требует
    # абсолютный: он должен достроиться по адресу самой карты.
    entries.append(_url_tag("site/b1", "0.4"))
    # Дубль первого адреса: карта не должна отдать его дважды.
    entries.append(_url_tag(f"{origin}/site/a", "0.1"))
    return web.Response(text=_urlset(*entries), content_type="application/xml")


async def handle_sitemap_gz(request: web.Request) -> web.Response:
    """Сжатая карта: крупные сайты отдают только такие."""
    request.app[HITS_KEY][request.path] += 1
    origin = str(request.url.origin())
    body = gzip.compress(
        _urlset(_url_tag(f"{origin}/site/a1", "0.6")).encode("utf-8")
    )
    return web.Response(body=body, content_type="application/x-gzip")


async def handle_sitemap_bomb(request: web.Request) -> web.Response:
    """Маленький архив с огромным содержимым: проверка лимита на распаковку."""
    request.app[HITS_KEY][request.path] += 1
    origin = str(request.url.origin())
    entries = [
        _url_tag(f"{origin}/site/page{number}", "0.5")
        for number in range(SITEMAP_BOMB_URLS)
    ]
    body = gzip.compress(_urlset(*entries).encode("utf-8"))
    return web.Response(body=body, content_type="application/x-gzip")


async def handle_sitemap_part(request: web.Request) -> web.Response:
    """Половина карты из индекса: по одному адресу на файл."""
    request.app[HITS_KEY][request.path] += 1
    origin = str(request.url.origin())
    page = "/site/a" if request.path == SITEMAP_PARTS[0] else "/site/b"
    return web.Response(
        text=_urlset(_url_tag(f"{origin}{page}", "0.5")),
        content_type="application/xml",
    )


async def handle_sitemap_index(request: web.Request) -> web.Response:
    """Индекс карт: две рабочие части и одна отсутствующая."""
    request.app[HITS_KEY][request.path] += 1
    origin = str(request.url.origin())
    return web.Response(
        text=_sitemapindex(
            *(f"{origin}{part}" for part in SITEMAP_PARTS),
            f"{origin}{SITEMAP_MISSING_PATH}",
        ),
        content_type="application/xml",
    )


async def handle_sitemap_loop(request: web.Request) -> web.Response:
    """Индекс, ссылающийся на себя: защита от бесконечного обхода."""
    request.app[HITS_KEY][request.path] += 1
    origin = str(request.url.origin())
    return web.Response(
        text=_sitemapindex(f"{origin}{SITEMAP_LOOP_PATH}"),
        content_type="application/xml",
    )


async def handle_sitemap_broken(request: web.Request) -> web.Response:
    """Не XML, а обрывок: разбор не должен уронить краулер."""
    request.app[HITS_KEY][request.path] += 1
    return web.Response(
        text="<urlset><url><loc>/site/a</loc", content_type="application/xml"
    )


async def handle_sitemap_empty(request: web.Request) -> web.Response:
    """Корректная, но пустая карта."""
    request.app[HITS_KEY][request.path] += 1
    return web.Response(text=_urlset(), content_type="application/xml")


@pytest.fixture
async def server() -> AsyncIterator[TestServer]:
    """Поднимает aiohttp-приложение на случайном порту на время теста."""
    app = web.Application(middlewares=[record_agent])
    app[HITS_KEY] = Counter()
    app[ROBOTS_KEY] = {"text": None}
    app[AGENTS_KEY] = []
    app[FLAKY_KEY] = Counter()
    app.router.add_get("/robots.txt", handle_robots)
    app.router.add_get("/flaky", handle_flaky)
    app.router.add_get("/ok", handle_ok)
    app.router.add_get("/slow", handle_slow)
    app.router.add_get("/status/{code}", handle_status)
    app.router.add_get("/chunked", handle_chunked)
    app.router.add_get("/nocharset", handle_no_charset)
    app.router.add_get("/counted", handle_counted)
    app.router.add_get("/page", handle_page)
    app.router.add_get("/broken", handle_broken)
    app.router.add_get("/empty", handle_empty)
    app.router.add_get("/site/", handle_site)
    app.router.add_get(REDIRECT_PATH, handle_redirect)
    app.router.add_get("/site/{page}", handle_site)
    app.router.add_get(SITEMAP_PATH, handle_sitemap)
    app.router.add_get(SITEMAP_GZ_PATH, handle_sitemap_gz)
    app.router.add_get(SITEMAP_BOMB_PATH, handle_sitemap_bomb)
    app.router.add_get(SITEMAP_INDEX_PATH, handle_sitemap_index)
    for part in SITEMAP_PARTS:
        app.router.add_get(part, handle_sitemap_part)
    app.router.add_get(SITEMAP_LOOP_PATH, handle_sitemap_loop)
    app.router.add_get(SITEMAP_BROKEN_PATH, handle_sitemap_broken)
    app.router.add_get(SITEMAP_EMPTY_PATH, handle_sitemap_empty)

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


@pytest.fixture
def robots_txt(server: TestServer):
    """Задаёт содержимое /robots.txt тестового сервера."""

    def _set(text: str | None) -> None:
        server.app[ROBOTS_KEY]["text"] = text

    return _set


@pytest.fixture
def agents(server: TestServer) -> list[tuple[str, str]]:
    """Пары (путь, User-Agent) по всем запросам к серверу."""
    return server.app[AGENTS_KEY]


@pytest.fixture
def flaky_hits(server: TestServer) -> Counter:
    """Сколько попыток пришлось на каждый «нестабильный» адрес."""
    return server.app[FLAKY_KEY]
