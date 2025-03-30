import json
import os
import tempfile
from datetime import datetime
from typing import Any

import polars as pl
import pytest

from mcp_server_data_wrangler.tools import handle_describe_data
from mcp_server_data_wrangler.tools.model import SupportedFileType
from mcp_server_data_wrangler.utils.datetime_utils import str_to_datetime


@pytest.mark.asyncio
@pytest.mark.usefixtures("scope_function")
@pytest.mark.parametrize(
    ("extension", "percentiles", "interpolation"),
    [
        (SupportedFileType.csv.value[1], [0.25, 0.5, 0.75], "nearest"),
        (SupportedFileType.tsv.value[1], [0.1, 0.5, 0.9], "linear"),
        (SupportedFileType.parquet.value[1], [0.25, 0.5, 0.75], "midpoint"),
    ],
)
async def test_handle_describe_data(
    mocker: Any,
    scope_function: Any,
    extension: str,
    percentiles: list[float],
    interpolation: str,
) -> None:
    # Create test data with various types to test description
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
    describe_df = df.describe(percentiles=percentiles, interpolation=interpolation)

    # Create expected output
    want = {
        "statistics": {
            col: {
                row: str(val) if val is not None else None
                for row, val in zip(describe_df["statistic"], describe_df[col])
            }
            for col in describe_df.columns
            if col != "statistic"
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

    arguments = {
        "input_data_file_path": tmp_file_path,
        "percentiles": percentiles,
        "interpolation": interpolation,
    }
    got = await handle_describe_data(arguments=arguments)
    text = json.loads(got[0].text)

    # Verify the structure and content
    assert "statistics" in text
    assert len(text["statistics"]) == len(want["statistics"])

    # Verify each column's statistics
    for col in want["statistics"]:
        assert col in text["statistics"]
        col_stats = text["statistics"][col]
        want_col_stats = want["statistics"][col]

        # Verify each statistic
        for stat in want_col_stats:
            assert stat in col_stats
            if want_col_stats[stat] is not None:
                # For numeric values, compare as floats to handle precision differences
                try:
                    assert float(col_stats[stat]) == float(want_col_stats[stat])  # type: ignore
                except (ValueError, TypeError):
                    # For non-numeric values, compare as strings
                    if col == "d" and col_stats[stat] is not None and want_col_stats[stat] is not None:
                        cd = str_to_datetime(col_stats[stat])
                        wd = str_to_datetime(want_col_stats[stat])
                        assert cd == wd
                    else:
                        assert col_stats[stat] == want_col_stats[stat] or col_stats[stat] is None
            else:
                assert col_stats[stat] is None

    os.unlink(tmp_file_path)
