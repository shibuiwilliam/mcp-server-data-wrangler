import json
from typing import Any

from mcp import types
from pydantic import ConfigDict

from ..make_logger import make_logger
from .model import Data

logger = make_logger(__name__)


class DataMinInputSchema(Data):
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
    def from_schema(input_data_file_path: str) -> "DataMinInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataMinInputSchema(df=data.df)

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataMinInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        return DataMinInputSchema.from_schema(input_data_file_path=input_data_file_path)


async def handle_data_min(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_min_input = DataMinInputSchema.from_args(arguments)
    min_df = data_min_input.df.min()

    # Convert the DataFrame to a dictionary format
    min_dict = {
        "description": "Minimum values for each column",
        "min_values": {col: str(val) if val is not None else None for col, val in zip(min_df.columns, min_df.row(0))},
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(min_dict),
        )
    ]


async def handle_data_min_horizontal(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_min_input = DataMinInputSchema.from_args(arguments)
    try:
        min_horizontal_df = data_min_input.df.min_horizontal()

        # Convert the DataFrame to a dictionary format
        min_horizontal_dict = {
            "description": "Minimum values across columns for each row",
            "min_values": {str(i): str(val) if val is not None else None for i, val in enumerate(min_horizontal_df)},
        }

        return [
            types.TextContent(
                type="text",
                text=json.dumps(min_horizontal_dict),
            )
        ]
    except Exception as e:
        logger.error(f"Error calculating min: {e}")
        return [
            types.TextContent(
                type="text",
                text=json.dumps(
                    {
                        "error": "Failed to calculate min values.",
                        "message": str(e),
                    }
                ),
            )
        ]
