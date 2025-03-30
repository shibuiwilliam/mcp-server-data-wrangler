import json
from typing import Any

from mcp import types
from pydantic import ConfigDict

from .model import Data


class DataSchemaInputSchema(Data):
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
    def from_schema(input_data_file_path: str) -> "DataSchemaInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataSchemaInputSchema(df=data.df)

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataSchemaInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        return DataSchemaInputSchema.from_schema(input_data_file_path=input_data_file_path)


async def handle_data_schema(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_schema_input = DataSchemaInputSchema.from_args(arguments)
    schema = data_schema_input.df.schema
    schema_dict = {col: str(dtype) for col, dtype in schema.items()}
    return [
        types.TextContent(
            type="text",
            text=json.dumps(
                {
                    "description": "Data schema of the input data",
                    "schema": schema_dict,
                }
            ),
        )
    ]
