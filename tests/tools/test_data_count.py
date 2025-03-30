import json
import os
import tempfile
from datetime import datetime
from typing import Any

import polars as pl
import pytest

from mcp_server_data_wrangler.tools import handle_data_count
from mcp_server_data_wrangler.tools.model import SupportedFileType


@pytest.mark.asyncio
@pytest.mark.usefixtures("scope_function")
@pytest.mark.parametrize(
    ("extension",),
    [
        (SupportedFileType.csv.value[1],),
        (SupportedFileType.tsv.value[1],),
        (SupportedFileType.parquet.value[1],),
    ],
)
async def test_handle_data_count(
    mocker: Any,
    scope_function: Any,
    extension: str,
) -> None:
    # Create test data with some null values
    data = [
        {"a": i, "b": j / 10 if i % 2 == 0 else None, "c": k, "d": datetime.now()}
        for i, j, k in zip(
            range(10),
            range(-10, 10, 2),
            ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        )
    ]

    df = pl.DataFrame(data)
    want = {
        "counts": {
            "a": 10,  # All values are non-null
            "b": 5,  # Only even indices have values
            "c": 10,  # All values are non-null
            "d": 10,  # All values are non-null
        },
    }

    with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as tmp_file:
        if extension == SupportedFileType.csv.value[1]:
            df.write_csv(tmp_file.name)
        elif extension == SupportedFileType.tsv.value[1]:
            df.write_csv(tmp_file.name, separator="\t")
        elif extension == SupportedFileType.parquet.value[1]:
            df.write_parquet(tmp_file.name)
        tmp_file_path = tmp_file.name

    arguments = {"input_data_file_path": tmp_file_path}
    got = await handle_data_count(arguments=arguments)
    text = json.loads(got[0].text)

    assert "counts" in text
    assert len(text["counts"]) == len(want["counts"])
    for k, v in want["counts"].items():
        assert text["counts"][k] == v

    os.unlink(tmp_file_path)
