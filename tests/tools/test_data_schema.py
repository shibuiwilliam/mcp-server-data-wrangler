import json
import os
import tempfile
from datetime import datetime
from typing import Any

import polars as pl
import pytest

from mcp_server_data_wrangler.tools import handle_data_schema
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
async def test_handle_data_schema(
    mocker: Any,
    scope_function: Any,
    extension: str,
) -> None:
    # Create test data with various types to test schema detection
    data = [
        {
            "a": i,  # Int64
            "b": j / 10 if i % 2 == 0 else None,  # Float64 with nulls
            "c": k,  # String
            "d": datetime.now(),  # Datetime
            "e": True if i % 2 == 0 else False,  # Boolean
            "f": None,  # Null
        }
        for i, j, k in zip(
            range(10),
            range(-10, 10, 2),
            ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        )
    ]

    df = pl.DataFrame(data)
    want = {
        "schema": {
            "a": "Int64",
            "b": "Float64",
            "c": "String",
            "d": "Datetime(time_unit='us', time_zone=None)" if extension == ".parquet" else "String",
            "e": "Boolean",
            "f": "Null" if extension == ".parquet" else "String",
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
    got = await handle_data_schema(arguments=arguments)
    text = json.loads(got[0].text)

    assert "schema" in text
    assert len(text["schema"]) == len(want["schema"])
    for key, value in want["schema"].items():
        assert key in text["schema"]
        assert text["schema"][key] == value

    os.unlink(tmp_file_path)
