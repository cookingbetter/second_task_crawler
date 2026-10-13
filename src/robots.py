"""Разбор robots.txt и проверка разрешений с кэшем на домен."""

from __future__ import annotations

import asyncio
import logging
import re
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlsplit

from src.filters import domain_key, origin_key

logger = logging.getLogger(__name__)

ALL_AGENTS = "*"
ROBOTS_PATH = "/robots.txt"
TOO_MANY_REQUESTS = 429

SOURCE_OK = "ok"
SOURCE_MISSING = "missing"
SOURCE_ERROR = "error"


@dataclass(slots=True)
class RobotsResponse:
    """Ответ на запрос robots.txt — ровно то, что нужно парсеру."""

    status: int | None = None
    text: str = ""
    error: str | None = None


Fetcher = Callable[[str], Awaitable[RobotsResponse]]


@dataclass(slots=True, frozen=True)
class RobotRule:
    """Одно правило `Allow`/`Disallow` с готовым регулярным выражением."""

    raw: str
    regex: re.Pattern[str]
    allowed: bool

    @property
    def length(self) -> int:
        """Длина исходного пути: более длинное правило считается точнее."""
        return len(self.raw)


@dataclass(slots=True, frozen=True)
class RobotsGroup:
    """Правила одной группы `User-agent`."""

    name: str
    rules: tuple[RobotRule, ...] = ()
    crawl_delay: float | None = None

    def decide(self, target: str) -> tuple[bool, str | None]:
        """Решение для пути и сработавшее правило.

        Побеждает самое длинное совпавшее правило, при равной длине —
        разрешающее: так описано в спецификации Google, и так ведут себя
        настоящие краулеры. Без правил группа ничего не запрещает.
        """
        best: RobotRule | None = None
        for rule in self.rules:
            if not rule.regex.match(target):
                continue
            if (
                best is None
                or rule.length > best.length
                or (rule.length == best.length and rule.allowed)
            ):
                best = rule

        if best is None:
            return True, None
        return best.allowed, f"{'Allow' if best.allowed else 'Disallow'}: {best.raw}"


@dataclass(slots=True, frozen=True)
class RobotsRules:
    """Разобранный robots.txt одного домена."""

    groups: dict[str, RobotsGroup] = field(default_factory=dict)
    status: int | None = None
    source: str = SOURCE_OK
    # Адреса карт сайта из директив `Sitemap`: они относятся ко всему сайту, а
    # не к группе агента, поэтому лежат рядом с группами, а не внутри них.
    sitemaps: tuple[str, ...] = ()

    @classmethod
    def allow_all(cls, status: int | None = None, source: str = SOURCE_MISSING) -> RobotsRules:
        """Нет robots.txt — нет и запретов."""
        return cls(groups={}, status=status, source=source)

    @classmethod
    def deny_all(cls, status: int | None = None, source: str = SOURCE_ERROR) -> RobotsRules:
        """Правила недоступны и настроено «не рисковать» — запретить всё."""
        rule = RobotRule(raw="/", regex=_compile_path("/"), allowed=False)
        return cls(
            groups={ALL_AGENTS: RobotsGroup(ALL_AGENTS, (rule,))},
            status=status,
            source=source,
        )

    def group_for(self, user_agent: str) -> RobotsGroup | None:
        """Группа, которая относится к этому User-Agent.

        Сравнивается не вся строка агента, а имя продукта — то, что стоит до
        первого `/`. Иначе достаточно упомянуть в агенте свой сайт, чтобы
        случайно попасть под чужую группу: `AsyncCrawler/1.0
        (+https://example.com/bot)` подошёл бы и под `User-agent: bot`, и под
        `User-agent: example`.

        Имя группы сравнивается с началом имени продукта, а не целиком: так
        `Googlebot-News/1.0` слушается группы `googlebot`, когда своей группы
        для него нет. Из подошедших берётся самая длинная (то есть самая
        точная), иначе работает группа `*`, а если нет и её — правил нет вовсе.

        Отсюда следует ограничение: агента вида
        `Mozilla/5.0 (compatible; MyBot/1.0)` правила для `MyBot` не найдут —
        имя краулера должно стоять в начале строки.
        """
        token = _product_token(user_agent)
        best: RobotsGroup | None = None
        for name, group in self.groups.items():
            if name == ALL_AGENTS or not name:
                continue
            if token.startswith(name) and (best is None or len(name) > len(best.name)):
                best = group
        return best or self.groups.get(ALL_AGENTS)

    def decide(self, target: str, user_agent: str) -> tuple[bool, str | None]:
        group = self.group_for(user_agent)
        if group is None:
            return True, None
        return group.decide(target)

    def crawl_delay_for(self, user_agent: str) -> float:
        group = self.group_for(user_agent)
        if group is None or group.crawl_delay is None:
            return 0.0
        return group.crawl_delay

    def summary(self, user_agent: str) -> dict[str, Any]:
        """Сводка для вывода: группы, правила и выбранная группа."""
        group = self.group_for(user_agent)
        return {
            "status": self.status,
            "source": self.source,
            "available": self.source == SOURCE_OK,
            "groups": {
                name: {"rules": len(item.rules), "crawl_delay": item.crawl_delay}
                for name, item in sorted(self.groups.items())
            },
            "group": group.name if group else None,
            "rules": len(group.rules) if group else 0,
            "crawl_delay": self.crawl_delay_for(user_agent),
            "sitemaps": list(self.sitemaps),
        }


def _product_token(user_agent: str) -> str:
    """Имя краулера из User-Agent: всё до первого `/`, в нижнем регистре."""
    return user_agent.split("/", 1)[0].strip().lower()


def _compile_path(path: str) -> re.Pattern[str]:
    """Переводит путь из robots.txt в регулярное выражение.

    Поддерживаются `*` (любая подстрока) и `$` на конце (конец адреса) —
    именно их чаще всего используют настоящие сайты, а `urllib.robotparser`
    из стандартной библиотеки умеет только префиксы.
    """
    anchored = path.endswith("$")
    body = path[:-1] if anchored else path
    pattern = "".join(".*" if char == "*" else re.escape(char) for char in body)
    return re.compile(f"^{pattern}{'$' if anchored else ''}")


def _target_of(url: str) -> str:
    """Часть адреса, к которой применяются правила: путь вместе с запросом.

    Разбор через `urlsplit`, а не `urlparse`: последний отрезает `;params`
    последнего сегмента в отдельное поле, и путь `/page;sessionid=7` дошёл бы
    до правил как `/page`.
    """
    parsed = urlsplit(url)
    target = parsed.path or "/"
    if parsed.query:
        target = f"{target}?{parsed.query}"
    return target


def parse_robots(text: str, *, status: int | None = None) -> RobotsRules:
    """Разбирает текст robots.txt в правила по группам `User-agent`.

    Подряд идущие строки `User-agent` образуют одну группу: правила после них
    относятся ко всем перечисленным агентам. Первое же `Allow`/`Disallow`
    закрывает перечисление, и следующий `User-agent` начинает новую группу.
    """
    rules: dict[str, list[RobotRule]] = {}
    delays: dict[str, float] = {}
    sitemaps: list[str] = []
    agents: list[str] = []
    collecting_agents = True

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue

        name, _, value = line.partition(":")
        directive = name.strip().lower()
        value = value.strip()

        if directive == "user-agent":
            if not collecting_agents:
                agents = []
                collecting_agents = True
            if value:
                agents.append(value.lower())
                rules.setdefault(value.lower(), [])
            continue

        if directive == "sitemap":
            # Директива не про группы: она адресована всем и может стоять до
            # первого User-agent. Поэтому и перечисление агентов она не
            # закрывает — иначе строка Sitemap между двумя User-agent разбила
            # бы одну группу на две.
            if value and value not in sitemaps:
                sitemaps.append(value)
            continue

        collecting_agents = False
        if not agents:
            # Правила до первой строки User-agent никому не адресованы.
            logger.debug("Строка robots.txt без User-agent пропущена: %s", line)
            continue

        if directive in ("allow", "disallow"):
            if not value:
                # Пустое значение ничего не ограничивает: `Disallow:` — это
                # «можно всё», и отдельного правила для этого не нужно.
                continue
            rule = RobotRule(
                raw=value, regex=_compile_path(value), allowed=directive == "allow"
            )
            for agent in agents:
                rules[agent].append(rule)
        elif directive == "crawl-delay":
            try:
                delay = float(value)
            except ValueError:
                logger.warning("Нечисловой Crawl-delay в robots.txt: %r", value)
                continue
            if delay < 0:
                logger.warning("Отрицательный Crawl-delay в robots.txt: %r", value)
                continue
            for agent in agents:
                delays[agent] = delay
        # Host и прочие директивы обхода не касаются.

    groups = {
        agent: RobotsGroup(agent, tuple(agent_rules), delays.get(agent))
        for agent, agent_rules in rules.items()
    }
    return RobotsRules(
        groups=groups, status=status, source=SOURCE_OK, sitemaps=tuple(sitemaps)
    )


class RobotsParser:
    """Загружает robots.txt каждого сервера один раз и отвечает на вопросы о нём.

    Кэш ключуется по origin (схема, хост, порт), а не по домену: у сервера на
    другом порту свой robots.txt, и правила соседа к нему не относятся.

    Загрузку делает не сам: `fetcher` передаётся снаружи, поэтому парсер не
    знает ни про aiohttp, ни про сессию краулера, а в тестах обходится без сети.
    """

    def __init__(
        self,
        fetcher: Fetcher,
        *,
        user_agent: str = ALL_AGENTS,
        allow_on_error: bool = True,
    ) -> None:
        self._fetcher = fetcher
        self.user_agent = user_agent
        # Сеть отвечает ошибкой или 5xx: строгое чтение спецификации требует
        # считать сайт закрытым. По умолчанию выбран мягкий вариант — иначе
        # одна неудачная загрузка robots.txt останавливала бы весь обход, —
        # но сайт, которому это важно, можно уважить флагом.
        self.allow_on_error = allow_on_error

        self._rules: dict[str, RobotsRules] = {}
        self._locks: dict[str, asyncio.Lock] = {}

        self.checks = 0
        self.blocked = 0
        self.fetched = 0
        self.missing = 0
        self.errors = 0
        self.blocked_by_domain: Counter[str] = Counter()

    async def fetch_robots(self, base_url: str) -> dict[str, Any]:
        """Загружает (или берёт из кэша) robots.txt сервера и отдаёт сводку."""
        rules = await self._rules_for(base_url)
        summary = rules.summary(self.user_agent)
        summary["origin"] = origin_key(base_url)
        summary["domain"] = domain_key(base_url)
        summary["url"] = urljoin(base_url, ROBOTS_PATH)
        summary["user_agent"] = self.user_agent
        return summary

    def can_fetch(self, url: str, user_agent: str = ALL_AGENTS) -> bool:
        """Разрешён ли URL по уже загруженным правилам.

        Синхронная проверка: если robots.txt сервера ещё не загружали, URL
        считается разрешённым. Чтобы правила гарантированно учитывались,
        нужен `allows()` — он при необходимости догрузит их.
        """
        rules = self._rules.get(origin_key(url))
        if rules is None:
            logger.debug("Правила robots.txt для %s ещё не загружены", url)
            return True
        allowed, _rule = rules.decide(_target_of(url), user_agent)
        return allowed

    def get_crawl_delay(
        self, user_agent: str = ALL_AGENTS, *, url: str | None = None
    ) -> float:
        """`Crawl-delay` из robots.txt; 0, если он не задан.

        Сервер берётся из `url`. Без него работает случай одного сайта: если в
        кэше ровно один сервер, имеется в виду он.
        """
        if url is not None:
            rules = self._rules.get(origin_key(url))
        elif len(self._rules) == 1:
            rules = next(iter(self._rules.values()))
        else:
            logger.debug(
                "get_crawl_delay без url, а в кэше серверов: %d", len(self._rules)
            )
            return 0.0
        return rules.crawl_delay_for(user_agent) if rules else 0.0

    async def allows(
        self, url: str, user_agent: str | None = None
    ) -> tuple[bool, str | None]:
        """Проверяет URL, загружая robots.txt домена при первом обращении.

        Возвращает решение и сработавшее правило — его видно в логах и в
        отчёте, поэтому понятно, почему именно URL не обошли.
        """
        agent = user_agent or self.user_agent
        rules = await self._rules_for(url)
        group = rules.group_for(agent)
        allowed, rule = group.decide(_target_of(url)) if group else (True, None)

        self.checks += 1
        if not allowed:
            self.blocked += 1
            self.blocked_by_domain[domain_key(url)] += 1
            logger.info(
                "robots.txt запрещает %s (%s, группа %s, агент %s)",
                url,
                rule,
                group.name if group else "-",
                agent,
            )
        return allowed, rule

    def cached_rules(self, url: str) -> RobotsRules | None:
        """Правила сервера из кэша — нужны для отчётов, без загрузки."""
        return self._rules.get(origin_key(url))

    async def sitemaps(self, url: str) -> list[str]:
        """Адреса карт сайта из robots.txt, загружая файл при необходимости.

        Карты объявляет сам сайт, и директива `Sitemap` — единственный
        стандартный способ их найти. Проверка разрешений тут не нужна: мы
        спрашиваем не «можно ли», а «что есть».
        """
        rules = await self._rules_for(url)
        return list(rules.sitemaps)

    def reset_stats(self) -> None:
        """Обнуляет счётчики проверок; разобранные правила остаются в кэше."""
        self.checks = 0
        self.blocked = 0
        self.fetched = 0
        self.missing = 0
        self.errors = 0
        self.blocked_by_domain.clear()

    def get_stats(self) -> dict[str, Any]:
        """Сколько проверок сделано, сколько URL запрещено и что в кэше."""
        return {
            "user_agent": self.user_agent,
            "checks": self.checks,
            "blocked": self.blocked,
            "allowed": self.checks - self.blocked,
            "blocked_by_domain": dict(self.blocked_by_domain),
            "fetched": self.fetched,
            "missing": self.missing,
            "errors": self.errors,
            "domains": len(self._rules),
            # Кэш живёт по origin, а в отчёте домен читается лучше: http и
            # https одного сайта почти всегда отдают один и тот же файл.
            "crawl_delays": {
                domain_key(origin): delay
                for origin, rules in self._rules.items()
                if (delay := rules.crawl_delay_for(self.user_agent))
            },
        }

    async def _rules_for(self, url: str) -> RobotsRules:
        """Правила сервера: из кэша, иначе одна загрузка на всех ожидающих."""
        key = origin_key(url)
        cached = self._rules.get(key)
        if cached is not None:
            return cached

        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock

        async with lock:
            # Пока ждали лок, robots.txt мог загрузить другой воркер.
            cached = self._rules.get(key)
            if cached is not None:
                return cached
            rules = await self._load(url)
            self._rules[key] = rules
            return rules

    async def _load(self, url: str) -> RobotsRules:
        robots_url = urljoin(url, ROBOTS_PATH)
        try:
            response = await self._fetcher(robots_url)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # падение загрузчика не должно рвать обход
            logger.warning(
                "Не удалось загрузить %s: %s: %s", robots_url, type(exc).__name__, exc
            )
            return self._on_error(None)

        status = response.status
        if response.error is not None:
            logger.warning("Ошибка загрузки %s: %s", robots_url, response.error)
            return self._on_error(status)
        if status == TOO_MANY_REQUESTS:
            # Это не «файла нет», а «сейчас не отдам»: правила неизвестны.
            logger.warning("robots.txt отдал 429: %s", robots_url)
            return self._on_error(status)
        if status is not None and 400 <= status < 500:
            # Нет файла — нет запретов: самый частый случай на малых сайтах.
            self.missing += 1
            logger.info("robots.txt недоступен (%d), разрешено всё: %s", status, robots_url)
            return RobotsRules.allow_all(status)
        if status is not None and status >= 500:
            logger.warning("robots.txt отдал %d: %s", status, robots_url)
            return self._on_error(status)

        self.fetched += 1
        rules = parse_robots(response.text, status=status)
        logger.info(
            "robots.txt разобран: %s, групп %d, Crawl-delay %.2f с",
            robots_url,
            len(rules.groups),
            rules.crawl_delay_for(self.user_agent),
        )
        return rules

    def _on_error(self, status: int | None) -> RobotsRules:
        self.errors += 1
        if self.allow_on_error:
            return RobotsRules.allow_all(status, source=SOURCE_ERROR)
        return RobotsRules.deny_all(status)
