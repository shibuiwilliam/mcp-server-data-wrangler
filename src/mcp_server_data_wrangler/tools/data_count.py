import json
from typing import Any

from mcp import types
from pydantic import ConfigDict

from .model import Data


class DataCountInputSchema(Data):
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
    def from_schema(input_data_file_path: str) -> "DataCountInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataCountInputSchema(df=data.df)

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataCountInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        return DataCountInputSchema.from_schema(input_data_file_path=input_data_file_path)


async def handle_data_count(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_count_input = DataCountInputSchema.from_args(arguments)
    count_df = data_count_input.df.count()

    # Convert the DataFrame to a dictionary format
    count_dict = {
        "description": "Number of non-null elements for each column",
        "counts": {col: int(val) for col, val in zip(count_df.columns, count_df.row(0))},
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(count_dict),
        )
    ]
