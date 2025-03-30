import json
from typing import Any

from mcp import types
from pydantic import ConfigDict

from ..make_logger import make_logger
from .model import Data

logger = make_logger(__name__)


class DataProductInputSchema(Data):
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
    def from_schema(input_data_file_path: str) -> "DataProductInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataProductInputSchema(df=data.df)

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataProductInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        return DataProductInputSchema.from_schema(input_data_file_path=input_data_file_path)


async def handle_data_product(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_product_input = DataProductInputSchema.from_args(arguments)
    product_df = data_product_input.df.product()

    # Convert the DataFrame to a dictionary format
    product_dict = {
        "description": "Product values for each column",
        "product_values": {
            col: str(val) if val is not None else None for col, val in zip(product_df.columns, product_df.row(0))
        },
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(product_dict),
        )
    ]
