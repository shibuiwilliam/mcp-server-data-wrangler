import asyncio

from . import server, utils
from .make_logger import make_logger


def main():
    asyncio.run(server.main())


__all__ = ["main", "server", "utils", "make_logger"]
