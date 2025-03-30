import json
from typing import Any

from mcp import types
from pydantic import ConfigDict, Field

from ..make_logger import make_logger
from .model import Data

logger = make_logger(__name__)


class DataQuantileInputSchema(Data):
    model_config = ConfigDict(
        validate_assignment=True,
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    quantile: float = Field(default=0.5, description="Quantile value between 0.0 and 1.0", gt=0.0, lt=1.0)
    interpolation: str = Field(
        default="nearest",
        description="Interpolation method for quantile. One of: 'nearest', 'higher', 'lower', 'midpoint', 'linear'",
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
                "quantile": {
                    "type": "number",
                    "description": "Quantile between 0.0 and 1.0",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "default": 0.5,
                },
                "interpolation": {
                    "type": "string",
                    "description": "Interpolation method",
                    "enum": ["nearest", "higher", "lower", "midpoint", "linear"],
                    "default": "nearest",
                },
            },
            "required": ["input_data_file_path", "quantile"],
        }

    @staticmethod
    def from_schema(
        input_data_file_path: str, quantile: float, interpolation: str = "nearest"
    ) -> "DataQuantileInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataQuantileInputSchema(
            df=data.df,
            quantile=quantile,
            interpolation=interpolation,
        )

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataQuantileInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        quantile = arguments["quantile"]
        interpolation = arguments.get("interpolation", "nearest")
        return DataQuantileInputSchema.from_schema(
            input_data_file_path=input_data_file_path,
            quantile=quantile,
            interpolation=interpolation,
        )


async def handle_data_quantile(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_quantile_input = DataQuantileInputSchema.from_args(arguments)
    quantile_df = data_quantile_input.df.quantile(
        quantile=data_quantile_input.quantile,
        interpolation=data_quantile_input.interpolation,
    )

    # Convert the DataFrame to a dictionary format
    quantile_dict = {
        "description": f"Quantile values for each column at {arguments['quantile']}",
        "quantile_values": {
            col: str(val) if val is not None else None for col, val in zip(quantile_df.columns, quantile_df.row(0))
        },
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(quantile_dict),
        )
    ]
