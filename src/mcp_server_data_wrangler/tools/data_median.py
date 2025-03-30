import json
from typing import Any

from mcp import types
from pydantic import ConfigDict

from ..make_logger import make_logger
from .model import Data

logger = make_logger(__name__)


class DataMedianInputSchema(Data):
    model_config = ConfigDict(
        validate_assignment=True,
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    @staticmethod
    def input_schema() -> dict:
        return {
            "type": "object",
            "properties": {
                "input_data_file_path": {
                    "type": "string",
                    "description": "Path to the input data file",
                },
            },
        }

    @staticmethod
    def from_schema(input_data_file_path: str) -> "DataMedianInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataMedianInputSchema(df=data.df)

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataMedianInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        return DataMedianInputSchema.from_schema(input_data_file_path=input_data_file_path)


async def handle_data_median(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_median_input = DataMedianInputSchema.from_args(arguments)
    median_df = data_median_input.df.median()

    # Convert the DataFrame to a dictionary format
    median_dict = {
        "description": "Median values for each column",
        "median_values": {
            col: str(val) if val is not None else None for col, val in zip(median_df.columns, median_df.row(0))
        },
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(median_dict),
        )
    ]
