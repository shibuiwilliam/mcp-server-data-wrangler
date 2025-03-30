import json
from typing import Any

from mcp import types
from pydantic import ConfigDict

from ..make_logger import make_logger
from .model import Data

logger = make_logger(__name__)


class DataMeanInputSchema(Data):
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
    def from_schema(input_data_file_path: str) -> "DataMeanInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataMeanInputSchema(df=data.df)

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataMeanInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        return DataMeanInputSchema.from_schema(input_data_file_path=input_data_file_path)


async def handle_data_mean(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_mean_input = DataMeanInputSchema.from_args(arguments)
    mean_df = data_mean_input.df.mean()

    # Convert the DataFrame to a dictionary format
    mean_dict = {
        "description": "Mean values for each column",
        "mean_values": {
            col: str(val) if val is not None else None for col, val in zip(mean_df.columns, mean_df.row(0))
        },
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(mean_dict),
        )
    ]


async def handle_data_mean_horizontal(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_mean_input = DataMeanInputSchema.from_args(arguments)
    try:
        mean_horizontal_df = data_mean_input.df.mean_horizontal()

        # Convert the DataFrame to a dictionary format
        mean_horizontal_dict = {
            "description": "Mean values across columns for each row",
            "mean_values": {str(i): str(val) if val is not None else None for i, val in enumerate(mean_horizontal_df)},
        }

        return [
            types.TextContent(
                type="text",
                text=json.dumps(mean_horizontal_dict),
            )
        ]
    except Exception as e:
        logger.error(f"Error calculating mean: {e}")
        return [
            types.TextContent(
                type="text",
                text=json.dumps(
                    {
                        "error": "Failed to calculate mean values.",
                        "message": str(e),
                    }
                ),
            )
        ]
