"""Ограничение скорости запросов: лимит на домен или общий на весь краулер."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import Counter
from typing import Any

from src.filters import domain_key

logger = logging.getLogger(__name__)

# Ключ, под которым живёт общий лимит: домена с таким именем не существует.
GLOBAL_KEY = "*"


class RateLimiter:
    """Выдаёт разрешения на запросы не чаще, чем задано.

    Лимит соблюдается не «вёдром токенов», а равномерным шагом: на каждый ключ
    хранится время последнего разрешённого запроса, и следующий уходит не раньше
    чем через шаг после него. Поэтому всплесков в начале обхода не бывает — сайт
    видит ровный поток, а новые требования (`Crawl-delay`, штраф за ошибку)
    действуют уже на следующем запросе, а не через один.

    Семафоры (`SemaphoreManager`) ограничивают, сколько запросов идёт
    одновременно, а лимитер — как часто они начинаются. Это разные вещи: один
    медленный запрос не даёт права отправить следующий немедленно.
    """

    def __init__(
        self,
        requests_per_second: float | None = 1.0,
        per_domain: bool = True,
        *,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        backoff_factor: float = 2.0,
        backoff_start: float = 1.0,
        max_backoff: float = 60.0,
        rng: random.Random | None = None,
    ) -> None:
        if requests_per_second is not None and requests_per_second < 0:
            raise ValueError("requests_per_second не может быть отрицательным")
        if min_delay < 0:
            raise ValueError("min_delay не может быть отрицательным")
        if jitter < 0:
            raise ValueError("jitter не может быть отрицательным")
        if backoff_factor < 1:
            raise ValueError("backoff_factor должен быть >= 1")
        if backoff_start < 0 or max_backoff < 0:
            raise ValueError("backoff_start и max_backoff не могут быть отрицательными")

        self.requests_per_second = requests_per_second
        self.per_domain = per_domain
        self.min_delay = min_delay
        self.jitter = jitter
        self.backoff_factor = backoff_factor
        self.backoff_start = backoff_start
        self.max_backoff = max_backoff
        # None и 0 означают «своего лимита скорости нет»: задержку могут задать
        # min_delay, Crawl-delay из robots.txt или backoff после ошибок.
        self.base_interval = (
            1.0 / requests_per_second
            if requests_per_second
            else 0.0
        )

        self._rng = rng or random.Random()
        self._locks: dict[str, asyncio.Lock] = {}
        # Сколько задач держит лок ключа или стоит за ним в очереди.
        self._refs: Counter[str] = Counter()
        # Когда по ключу темпа последний раз уходил запрос: от этого времени и
        # считается следующая пауза.
        self._last_allowed: dict[str, float] = {}
        # Требования и штрафы — всегда по сайту, даже когда темп общий: это
        # свойства сайта, а не нашего канала.
        self._crawl_delays: dict[str, float] = {}
        self._backoff: dict[str, float] = {}
        # Только для статистики `avg_gap`, сбрасывается вместе со счётчиками.
        self._last_request: dict[str, float] = {}

        self.requests = 0
        self.waited_total = 0.0
        self.max_wait = 0.0
        self.penalties = 0
        self._gap_total = 0.0
        self._gap_count = 0
        self._started = time.monotonic()

    @property
    def enabled(self) -> bool:
        """Может ли лимитер вообще задержать запрос."""
        return bool(
            self.base_interval
            or self.min_delay
            or self.jitter
            or self._crawl_delays
            or self._backoff
        )

    async def acquire(self, domain: str | None = None) -> float:
        """Ждёт разрешения на запрос и возвращает, сколько пришлось ждать.

        Принимает и домен, и полный URL: ключ считается тем же `domain_key`,
        что в фильтрах и семафорах, поэтому `example.com`, `www.example.com` и
        `example.com:443` делят один лимит.

        Ожидание идёт под локом ключа: `asyncio.Lock` выдаёт его в порядке
        очереди, поэтому запросы к домену уходят по одному, строго по шагу и в
        том порядке, в котором их попросили. Без лока все ожидающие проснулись
        бы по одному и тому же сроку и ушли бы разом.

        Пауза считается от времени последнего запроса, а не заранее, в конце
        предыдущего: иначе всё, что стало известно потом — штраф за ошибку или
        `Crawl-delay` из robots.txt, — вступало бы в силу только через запрос.
        """
        key = self._key(domain)
        lock = self._lock_for(key)
        # Счётчик ссылок растёт до первого await, поэтому лок не удалят
        # из-под задачи, которая его ждёт.
        self._refs[key] += 1
        try:
            async with lock:
                now = time.monotonic()
                waited = 0.0
                last = self._last_allowed.get(key)
                if last is not None:
                    ready = last + self._delay(domain)
                    if ready > now:
                        waited = ready - now
                        logger.debug(
                            "Лимит скорости: ждём %.3f с перед запросом к %s",
                            waited,
                            key,
                        )
                        await asyncio.sleep(waited)
                        now = time.monotonic()

                self._last_allowed[key] = now
                self._record(key, waited, now)
                return waited
        finally:
            self._refs[key] -= 1
            if self._refs[key] <= 0:
                # Лок никому не нужен: иначе словарь копил бы по локу на каждый
                # встреченный домен. Шаг хранится отдельно и не теряется.
                del self._refs[key]
                self._locks.pop(key, None)

    def interval_for(self, domain: str | None = None) -> float:
        """Шаг между запросами к домену без случайной добавки.

        Из всех ограничений берётся самое строгое: своё (`requests_per_second`),
        `min_delay` и `Crawl-delay` из robots.txt. Штраф за ошибки добавляется
        сверху — он не альтернатива лимиту, а наказание поверх него.

        Требование сайта и штраф ищутся по самому сайту, даже когда темп общий:
        при `per_domain=False` все домены делят одни часы, но не чужие
        задержки.
        """
        site = self._site_key(domain)
        interval = max(
            self.base_interval,
            self.min_delay,
            self._crawl_delays.get(site, 0.0),
        )
        return interval + self._backoff.get(site, 0.0)

    def set_crawl_delay(self, domain: str, delay: float) -> None:
        """Запоминает `Crawl-delay` из robots.txt для сайта."""
        site = self._site_key(domain)
        if delay <= 0:
            self._crawl_delays.pop(site, None)
            return
        if self._crawl_delays.get(site) == delay:
            return
        self._crawl_delays[site] = delay
        logger.info("robots.txt задал Crawl-delay %.2f с для %s", delay, site)

    def penalize(self, domain: str | None = None) -> float:
        """Увеличивает задержку домена после ошибки и возвращает новый штраф.

        Первая ошибка стоит `backoff_start`, каждая следующая — вдвое (точнее,
        в `backoff_factor` раз) дороже, но не больше `max_backoff`. Повторной
        загрузки URL здесь нет: краулер лишь перестаёт давить на сайт, которому
        плохо.

        Штраф привязан к сайту: в режиме общего темпа ошибка одного сервера не
        должна замедлять обход всех остальных.
        """
        site = self._site_key(domain)
        current = self._backoff.get(site, 0.0)
        penalty = self.backoff_start if current <= 0 else current * self.backoff_factor
        penalty = min(penalty, self.max_backoff)
        self._backoff[site] = penalty
        self.penalties += 1
        logger.info(
            "Ошибка на %s: задержка увеличена до +%.2f с (штраф %d)",
            site,
            penalty,
            self.penalties,
        )
        return penalty

    def reward(self, domain: str | None = None) -> None:
        """Уменьшает штраф домена вдвое после успешного ответа.

        Не обнуляет: один удачный ответ после серии ошибок ещё не значит, что
        сайт восстановился, а плавное снижение не возвращает нагрузку рывком.
        """
        site = self._site_key(domain)
        current = self._backoff.get(site, 0.0)
        if not current:
            return
        reduced = current / 2
        if reduced < 0.01:  # остаток меньше тика планировщика — просто убрать
            del self._backoff[site]
            logger.debug("Штраф для %s снят", site)
            return
        self._backoff[site] = reduced
        logger.debug("Штраф для %s уменьшен до +%.2f с", site, reduced)

    def backoff_for(self, domain: str | None = None) -> float:
        """Текущий штраф сайта, 0 — если его нет."""
        return self._backoff.get(self._site_key(domain), 0.0)

    def reset_stats(self) -> None:
        """Обнуляет счётчики перед новым обходом.

        Время последнего запроса, штрафы и `Crawl-delay` остаются: это
        состояние вежливости, а не история, и забыть его означало бы снова
        разогнаться до лимита.
        """
        self.requests = 0
        self.waited_total = 0.0
        self.max_wait = 0.0
        self.penalties = 0
        self._gap_total = 0.0
        self._gap_count = 0
        self._last_request.clear()
        self._started = time.monotonic()

    def get_stats(self) -> dict[str, Any]:
        """Настройки лимитера и то, как они сказались на запросах."""
        elapsed = time.monotonic() - self._started
        return {
            "enabled": self.enabled,
            "requests_per_second": self.requests_per_second,
            "per_domain": self.per_domain,
            "min_delay": self.min_delay,
            "jitter": self.jitter,
            "base_interval": self.base_interval,
            "requests": self.requests,
            "waited_total": self.waited_total,
            "avg_wait": self.waited_total / self.requests if self.requests else 0.0,
            "max_wait": self.max_wait,
            # Средний фактический промежуток между запросами к одному ключу —
            # не то же самое, что ожидание в лимитере: сюда входит и время
            # самого запроса.
            "avg_gap": self._gap_total / self._gap_count if self._gap_count else 0.0,
            "actual_rps": self.requests / elapsed if elapsed > 0 else 0.0,
            "elapsed": elapsed,
            "penalties": self.penalties,
            "domains": len(self._last_allowed),
            "crawl_delays": dict(self._crawl_delays),
            "backoff": dict(self._backoff),
            "interval_by_domain": {
                key: self.interval_for(key) for key in sorted(self._last_allowed)
            },
        }

    def _key(self, domain: str | None) -> str:
        """Ключ темпа: один на все домены или свой у каждого."""
        if not self.per_domain:
            return GLOBAL_KEY
        return self._site_key(domain)

    @staticmethod
    def _site_key(domain: str | None) -> str:
        """Ключ сайта: к нему привязаны `Crawl-delay` и штраф за ошибки."""
        return domain_key(domain) if domain else GLOBAL_KEY

    def _lock_for(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def _delay(self, domain: str | None) -> float:
        """Шаг до следующего запроса вместе со случайной добавкой."""
        delay = self.interval_for(domain)
        if self.jitter:
            # Разброс только вверх: запрос не должен уйти раньше лимита.
            delay += self._rng.uniform(0.0, self.jitter)
        return delay

    def _record(self, key: str, waited: float, now: float) -> None:
        self.requests += 1
        self.waited_total += waited
        self.max_wait = max(self.max_wait, waited)
        previous = self._last_request.get(key)
        if previous is not None:
            self._gap_total += now - previous
            self._gap_count += 1
        self._last_request[key] = now
