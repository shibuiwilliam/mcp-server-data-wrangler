import asyncio

from . import server, utils
from .configurations import Settings
from .make_logger import make_logger


def main() -> None:
    asyncio.run(server.main())


__all__ = ["main", "server", "utils", "make_logger", "Settings"]
