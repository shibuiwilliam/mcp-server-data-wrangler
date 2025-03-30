from typing import Generator

import pytest


@pytest.fixture(scope="function", autouse=False)
def scope_function() -> Generator:
    yield
