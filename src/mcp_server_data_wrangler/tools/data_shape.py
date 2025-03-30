import json
from typing import Any

from mcp import types
from pydantic import ConfigDict

from .model import Data


class DataShapeInputSchema(Data):
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
    def from_schema(inuput_data_file_path: str) -> "DataShapeInputSchema":
        data = Data.from_file(inuput_data_file_path)
        return DataShapeInputSchema(df=data.df)

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataShapeInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        return DataShapeInputSchema.from_schema(inuput_data_file_path=input_data_file_path)


async def handle_data_shape(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_shape_input = DataShapeInputSchema.from_args(arguments)
    data_shape = data_shape_input.df.shape
    num_rows = data_shape[0]
    num_cols = data_shape[1]
    return [
        types.TextContent(
            type="text",
            text=json.dumps(
                {
                    "description": "Data shape of the input data",
                    "num_rows": num_rows,
                    "num_cols": num_cols,
                }
            ),
        )
    ]
