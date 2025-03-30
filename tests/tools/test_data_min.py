import json
import os
import tempfile
from datetime import datetime
from typing import Any

import polars as pl
import pytest

from mcp_server_data_wrangler.tools import handle_data_min, handle_data_min_horizontal
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
async def test_handle_data_min(
    mocker: Any,
    scope_function: Any,
    extension: str,
) -> None:
    # Create test data with various types to test min
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
    min_df = df.min()

    # Create expected output
    want = {
        "min_values": {col: str(val) if val is not None else None for col, val in zip(min_df.columns, min_df.row(0))},
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
    got = await handle_data_min(arguments=arguments)
    text = json.loads(got[0].text)

    # Verify the structure and content
    assert "min_values" in text
    assert len(text["min_values"]) == len(want["min_values"])

    # Verify each column's min value
    for col in want["min_values"]:
        assert col in text["min_values"]
        tc = text["min_values"][col]
        wc = want["min_values"][col]
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
async def test_handle_data_min_horizontal_success(
    mocker: Any,
    scope_function: Any,
    extension: str,
) -> None:
    # Create test data with numeric columns for horizontal min
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
    min_horizontal_df = df.min_horizontal()

    # Create expected output
    want = {
        "description": "Minimum values across columns for each row",
        "min_values": {str(i): str(val) if val is not None else None for i, val in enumerate(min_horizontal_df)},
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
    got = await handle_data_min_horizontal(arguments=arguments)
    text = json.loads(got[0].text)

    # Verify the structure and content
    assert text["description"] == want["description"]
    assert "min_values" in text
    assert len(text["min_values"]) == len(want["min_values"])

    # Verify each row's min value
    for row in want["min_values"]:
        tr = text["min_values"][row]
        wr = want["min_values"][row]  # type: ignore
        assert row in text["min_values"]
        if wr is not None:
            assert float(tr) == float(wr)
        else:
            assert tr is None

    os.unlink(tmp_file_path)
