"""Фильтрация URL перед постановкой в очередь обхода."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

ALLOWED_SCHEMES = frozenset({"http", "https"})
DEFAULT_PORTS = {"http": "80", "https": "443"}

REJECT_SCHEME = "scheme"
REJECT_DOMAIN = "domain"
REJECT_INCLUDE = "include"
REJECT_EXCLUDE = "exclude"


def domain_key(url_or_host: str) -> str:
    """Ключ домена для сравнения: без схемы, порта, регистра и префикса `www.`.

    Без отбрасывания `www.` обход `example.com` терял бы половину сайта: многие
    страницы ссылаются на себя через `www`, и это тот же сайт.
    """
    try:
        host = urlparse(url_or_host).netloc or url_or_host
    except ValueError:
        host = url_or_host
    host = host.split("/", 1)[0]  # строка без схемы: отбросить путь
    host = host.rsplit("@", 1)[-1].lower()  # отбросить user:pass@
    host = host.split(":", 1)[0]  # отбросить порт
    return host[4:] if host.startswith("www.") else host


def origin_key(url: str) -> str:
    """Схема, хост и порт — то, к чему привязан robots.txt.

    В отличие от `domain_key`, порт и `www.` сохраняются: у сервера на другом
    порту свой robots.txt, и путать их правила нельзя. Схема остаётся по той же
    причине, хотя на практике файлы http и https почти всегда совпадают.
    """
    try:
        parsed = urlparse(url)
    except ValueError:
        return domain_key(url)
    if not parsed.scheme or not parsed.netloc:
        return domain_key(url)

    scheme = parsed.scheme.lower()
    hostport = parsed.netloc.rsplit("@", 1)[-1]  # отбросить user:pass@
    host, _, port = hostport.partition(":")
    host = host.lower()
    if port and port == DEFAULT_PORTS.get(scheme):
        port = ""
    return f"{scheme}://{host}" + (f":{port}" if port else "")


def normalize_url(url: str) -> str:
    """Приводит URL к каноничному виду, чтобы одна страница не грузилась дважды.

    Схема и хост — в нижний регистр, порт по умолчанию и пустой запрос
    отбрасываются, пустой путь становится `/`. Путь и запрос по регистру не
    меняются: на большинстве серверов они регистрозависимы, и `/About` — не то
    же самое, что `/about`.
    """
    try:
        parsed = urlparse(url)
    except ValueError:  # например, битый IPv6-литерал
        return url
    if not parsed.scheme or not parsed.netloc:
        return url  # относительные и мусорные ссылки нормализовать нечего

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc
    userinfo, _, hostport = netloc.rpartition("@")
    host, _, port = hostport.partition(":")
    host = host.lower()
    if port and port == DEFAULT_PORTS.get(scheme):
        port = ""

    netloc = host + (f":{port}" if port else "")
    if userinfo:
        netloc = f"{userinfo}@{netloc}"

    return urlunparse(
        (scheme, netloc, parsed.path or "/", parsed.params, parsed.query, "")
    )


@dataclass(slots=True)
class UrlFilter:
    """Решает, годится ли URL для обхода.

    Фильтр влияет только на очередь: ссылки в данных страницы остаются полными,
    иначе внешние ссылки молча исчезали бы из результатов краулинга.
    """

    allowed_hosts: frozenset[str] = frozenset()
    include_patterns: tuple[re.Pattern[str], ...] = ()
    exclude_patterns: tuple[re.Pattern[str], ...] = ()

    @classmethod
    def build(
        cls,
        start_urls: Iterable[str] = (),
        *,
        same_domain_only: bool = False,
        include_patterns: Iterable[str | re.Pattern[str]] | None = None,
        exclude_patterns: Iterable[str | re.Pattern[str]] | None = None,
    ) -> UrlFilter:
        """Собирает фильтр: компилирует паттерны и список разрешённых доменов."""
        hosts: frozenset[str] = frozenset()
        if same_domain_only:
            hosts = frozenset(
                key for url in start_urls if (key := domain_key(url))
            )
            logger.debug("Обход ограничен доменами: %s", ", ".join(sorted(hosts)))

        return cls(
            allowed_hosts=hosts,
            include_patterns=cls._compile(include_patterns),
            exclude_patterns=cls._compile(exclude_patterns),
        )

    @staticmethod
    def _compile(
        patterns: Iterable[str | re.Pattern[str]] | None,
    ) -> tuple[re.Pattern[str], ...]:
        """Компилирует паттерны один раз, а не на каждый URL."""
        compiled: list[re.Pattern[str]] = []
        for pattern in patterns or ():
            if isinstance(pattern, re.Pattern):
                compiled.append(pattern)
                continue
            try:
                compiled.append(re.compile(pattern))
            except re.error as exc:
                # Битый паттерн из командной строки не должен ронять обход.
                logger.warning("Некорректный паттерн %r пропущен: %s", pattern, exc)
        return tuple(compiled)

    def allows(self, url: str) -> bool:
        return self.reject_reason(url) is None

    def reject_reason(self, url: str) -> str | None:
        """Причина отказа или None, если URL проходит все проверки."""
        try:
            parsed = urlparse(url)
        except ValueError:  # например, битый IPv6-литерал
            return REJECT_SCHEME

        if parsed.scheme not in ALLOWED_SCHEMES or not parsed.netloc:
            return REJECT_SCHEME
        if self.allowed_hosts and domain_key(parsed.netloc) not in self.allowed_hosts:
            return REJECT_DOMAIN
        # Запрет сильнее разрешения: «не ходить в /logout» должно работать даже
        # если URL попал под include-паттерн.
        if any(pattern.search(url) for pattern in self.exclude_patterns):
            return REJECT_EXCLUDE
        if self.include_patterns and not any(
            pattern.search(url) for pattern in self.include_patterns
        ):
            return REJECT_INCLUDE
        return None
