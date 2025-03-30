import json
from typing import Any

from mcp import types
from pydantic import ConfigDict, Field

from .model import Data


class DataEstimatedSizeInputSchema(Data):
    model_config = ConfigDict(
        validate_assignment=True,
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    unit: str = Field(
        default="b",
        description="Unit for the estimated size. One of: 'b' (bytes), 'kb', 'mb', 'gb', 'tb'",
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
                "unit": {
                    "type": "string",
                    "enum": ["b", "kb", "mb", "gb", "tb"],
                    "description": "Unit for the estimated size",
                    "default": "b",
                },
            },
        }

    @staticmethod
    def from_schema(
        input_data_file_path: str,
        unit: str = "b",
    ) -> "DataEstimatedSizeInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataEstimatedSizeInputSchema(
            df=data.df,
            unit=unit,
        )

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataEstimatedSizeInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        unit = arguments.get("unit", "b")
        return DataEstimatedSizeInputSchema.from_schema(
            input_data_file_path=input_data_file_path,
            unit=unit,
        )


async def handle_data_estimated_size(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_estimated_size_input = DataEstimatedSizeInputSchema.from_args(arguments)
    estimated_size = data_estimated_size_input.df.estimated_size(unit=data_estimated_size_input.unit)

    result_dict = {
        "description": "Estimated size of the input data",
        "size": estimated_size,
        "unit": data_estimated_size_input.unit,
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(result_dict),
        )
    ]
