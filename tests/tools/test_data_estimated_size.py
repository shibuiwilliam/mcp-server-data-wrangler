import json
import os
import tempfile
from datetime import datetime
from typing import Any

import polars as pl
import pytest

from mcp_server_data_wrangler.tools import handle_data_estimated_size
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
async def test_handle_data_estimated_size(
    mocker: Any,
    scope_function: Any,
    extension: str,
) -> None:
    # Create test data with various types to test size estimation
    data = [
        {
            "a": i,
            "b": j / 10 if i % 2 == 0 else None,
            "c": k,
            "d": datetime.now(),
            "e": "x" * 100,  # Long string to increase size
        }
        for i, j, k in zip(
            range(10),
            range(-10, 10, 2),
            ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        )
    ]

    df = pl.DataFrame(data)

    with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as tmp_file:
        if extension == SupportedFileType.csv.value[1]:
            df.write_csv(tmp_file.name)
        elif extension == SupportedFileType.tsv.value[1]:
            df.write_csv(tmp_file.name, separator="\t")
        elif extension == SupportedFileType.parquet.value[1]:
            df.write_parquet(tmp_file.name)
        tmp_file_path = tmp_file.name

    # Test with different units
    test_cases = [
        {
            "unit": "b",
            "want": {
                "size": df.estimated_size(unit="b"),
                "unit": "b",
            },
        },
        {
            "unit": "kb",
            "want": {
                "size": df.estimated_size(unit="kb"),
                "unit": "kb",
            },
        },
        {
            "unit": "mb",
            "want": {
                "size": df.estimated_size(unit="mb"),
                "unit": "mb",
            },
        },
    ]

    for case in test_cases:
        arguments = {
            "input_data_file_path": tmp_file_path,
            "unit": case["unit"],
        }
        got = await handle_data_estimated_size(arguments=arguments)
        text = json.loads(got[0].text)
        s = case["want"]["size"]  # type: ignore
        assert text["unit"] == case["want"]["unit"]  # type: ignore
        assert s * 0.8 <= text["size"] <= s * 1.2, f"Expected size to be around {s}, but got {text['size']}"

    os.unlink(tmp_file_path)
