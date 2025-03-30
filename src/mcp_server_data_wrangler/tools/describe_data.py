import json
from typing import Any, List

from mcp import types
from pydantic import ConfigDict, Field

from .model import Data


class DescribeDataInputSchema(Data):
    model_config = ConfigDict(
        validate_assignment=True,
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    percentiles: List[float] = Field(
        default=[0.25, 0.5, 0.75],
        description="List of percentiles to include in the summary statistics. All values must be in the range [0, 1].",
    )
    interpolation: str = Field(
        default="nearest",
        description="Interpolation method used when calculating percentiles. One of: 'nearest', 'higher', 'lower', 'midpoint', 'linear'",
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
                "percentiles": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "List of percentiles to include in the summary statistics. All values must be in the range [0, 1].",
                    "default": [0.25, 0.5, 0.75],
                },
                "interpolation": {
                    "type": "string",
                    "enum": ["nearest", "higher", "lower", "midpoint", "linear"],
                    "description": "Interpolation method used when calculating percentiles",
                    "default": "nearest",
                },
            },
        }

    @staticmethod
    def from_schema(
        input_data_file_path: str,
        percentiles: List[float] = [0.25, 0.5, 0.75],
        interpolation: str = "nearest",
    ) -> "DescribeDataInputSchema":
        data = Data.from_file(input_data_file_path)
        return DescribeDataInputSchema(
            df=data.df,
            percentiles=percentiles,
            interpolation=interpolation,
        )

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DescribeDataInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        percentiles = arguments.get("percentiles", [0.25, 0.5, 0.75])
        interpolation = arguments.get("interpolation", "nearest")
        return DescribeDataInputSchema.from_schema(
            input_data_file_path=input_data_file_path,
            percentiles=percentiles,
            interpolation=interpolation,
        )


async def handle_describe_data(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    describe_data_input = DescribeDataInputSchema.from_args(arguments)
    describe_df = describe_data_input.df.describe(
        percentiles=describe_data_input.percentiles,
        interpolation=describe_data_input.interpolation,
    )

    # Convert the DataFrame to a dictionary format
    describe_dict = {
        "description": "Summary statistics of the input data",
        "statistics": {
            col: {
                row: str(val) if val is not None else None
                for row, val in zip(describe_df["statistic"], describe_df[col])
            }
            for col in describe_df.columns
            if col != "statistic"
        },
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(describe_dict),
        )
    ]
