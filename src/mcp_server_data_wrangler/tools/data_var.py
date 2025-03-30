import json
from typing import Any

from mcp import types
from pydantic import ConfigDict, Field

from ..make_logger import make_logger
from .model import Data

logger = make_logger(__name__)


class DataVarInputSchema(Data):
    model_config = ConfigDict(
        validate_assignment=True,
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    ddof: int = Field(
        default=1, description="Delta Degrees of Freedom: the divisor used in the calculation is N - ddof", ge=0
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
                "ddof": {
                    "type": "integer",
                    "description": "Delta Degrees of Freedom: the divisor used in the calculation is N - ddof",
                    "minimum": 0,
                    "default": 1,
                },
            },
            "required": ["input_data_file_path"],
        }

    @staticmethod
    def from_schema(input_data_file_path: str, ddof: int = 1) -> "DataVarInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataVarInputSchema(
            df=data.df,
            ddof=ddof,
        )

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataVarInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        ddof = arguments.get("ddof", 1)
        return DataVarInputSchema.from_schema(
            input_data_file_path=input_data_file_path,
            ddof=ddof,
        )


async def handle_data_var(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_var_input = DataVarInputSchema.from_args(arguments)
    var_df = data_var_input.df.var(ddof=data_var_input.ddof)

    # Convert the DataFrame to a dictionary format
    var_dict = {
        "description": f"Variance values for each column with ddof={data_var_input.ddof}",
        "var_values": {col: str(val) if val is not None else None for col, val in zip(var_df.columns, var_df.row(0))},
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(var_dict),
        )
    ]
