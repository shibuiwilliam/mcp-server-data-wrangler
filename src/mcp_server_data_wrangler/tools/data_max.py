import json
from typing import Any

from mcp import types
from pydantic import ConfigDict

from ..make_logger import make_logger
from .model import Data

logger = make_logger(__name__)


class DataMaxInputSchema(Data):
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
    def from_schema(input_data_file_path: str) -> "DataMaxInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataMaxInputSchema(df=data.df)

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataMaxInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        return DataMaxInputSchema.from_schema(input_data_file_path=input_data_file_path)


async def handle_data_max(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_max_input = DataMaxInputSchema.from_args(arguments)
    max_df = data_max_input.df.max()

    # Convert the DataFrame to a dictionary format
    max_dict = {
        "description": "Maximum values for each column",
        "max_values": {col: str(val) if val is not None else None for col, val in zip(max_df.columns, max_df.row(0))},
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(max_dict),
        )
    ]


async def handle_data_max_horizontal(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_max_input = DataMaxInputSchema.from_args(arguments)
    try:
        max_horizontal_df = data_max_input.df.max_horizontal()

        # Convert the DataFrame to a dictionary format
        max_horizontal_dict = {
            "description": "Maximum values across columns for each row",
            "max_values": {str(i): str(val) if val is not None else None for i, val in enumerate(max_horizontal_df)},
        }

        return [
            types.TextContent(
                type="text",
                text=json.dumps(max_horizontal_dict),
            )
        ]
    except Exception as e:
        logger.error(f"Error calculating max: {e}")
        return [
            types.TextContent(
                type="text",
                text=json.dumps(
                    {
                        "error": "Failed to calculate max values.",
                        "message": str(e),
                    }
                ),
            )
        ]
