import json
import os
import tempfile
from datetime import datetime
from typing import Any

import polars as pl
import pytest

from mcp_server_data_wrangler.tools import handle_data_max, handle_data_max_horizontal
from mcp_server_data_wrangler.tools.model import SupportedFileType
from mcp_server_data_wrangler.utils.datetime_utils import str_to_datetime


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
async def test_handle_data_max(
    mocker: Any,
    scope_function: Any,
    extension: str,
) -> None:
    # Create test data with various types to test max
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
    max_df = df.max()

    # Create expected output
    want = {
        "max_values": {col: str(val) if val is not None else None for col, val in zip(max_df.columns, max_df.row(0))},
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
    got = await handle_data_max(arguments=arguments)
    text = json.loads(got[0].text)

    # Verify the structure and content
    assert "max_values" in text
    assert len(text["max_values"]) == len(want["max_values"])

    # Verify each column's max value
    for col in want["max_values"]:
        assert col in text["max_values"]
        wc = want["max_values"][col]
        tc = text["max_values"][col]
        if wc is not None:
            # For numeric values, compare as floats to handle precision differences
            try:
                assert float(tc) == float(wc)
            except (ValueError, TypeError):
                # For non-numeric values, compare as strings
                if col == "d":
                    cd = str_to_datetime(tc)
                    wd = str_to_datetime(wc)
                    assert cd == wd
                else:
                    assert tc == wc
        else:
            assert tc is None

    os.unlink(tmp_file_path)


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
async def test_handle_data_max_horizontal_success(
    mocker: Any,
    scope_function: Any,
    extension: str,
) -> None:
    # Create test data with numeric columns for horizontal max
    data = [
        {
            "a": i,
            "b": j / 10,
            "c": k,
        }
        for i, j, k in zip(
            range(10),
            range(-10, 10, 2),
            range(5, 15),
        )
    ]

    df = pl.DataFrame(data)
    max_horizontal_df = df.max_horizontal()

    # Create expected output
    want = {
        "description": "Maximum values across columns for each row",
        "max_values": {str(i): str(val) if val is not None else None for i, val in enumerate(max_horizontal_df)},
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
    got = await handle_data_max_horizontal(arguments=arguments)
    text = json.loads(got[0].text)

    # Verify the structure and content
    assert text["description"] == want["description"]
    assert "max_values" in text
    assert len(text["max_values"]) == len(want["max_values"])

    # Verify each row's max value
    for row in want["max_values"]:
        assert row in text["max_values"]
        tr = text["max_values"][row]
        wr = want["max_values"][row]  # type: ignore
        if wr is not None:
            assert float(tr) == float(wr)
        else:
            assert tr is None

    os.unlink(tmp_file_path)
