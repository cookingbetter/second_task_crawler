"""Точка входа из задания: `python crawler.py --urls https://example.com`.

Сам краулер живёт в пакете `src`, а демонстрации семи дней — в `src/main.py`.
Этот файл существует, чтобы работала команда из условия, и отличается от
`python -m src.main` одним: без явного флага режима он обходит сайт через
`AdvancedCrawler` (день 7), а не загружает список адресов (день 1). Логика не
дублируется — тот же разбор аргументов и те же режимы.
"""

from __future__ import annotations

import asyncio

from src.main import default_mode_argv, main

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(default_mode_argv())))
