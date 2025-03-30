import json
from typing import Any

from mcp import types
from pydantic import ConfigDict, Field

from ..make_logger import make_logger
from .model import Data

logger = make_logger(__name__)


class DataStdInputSchema(Data):
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
    def from_schema(input_data_file_path: str, ddof: int = 1) -> "DataStdInputSchema":
        data = Data.from_file(input_data_file_path)
        return DataStdInputSchema(
            df=data.df,
            ddof=ddof,
        )

    @staticmethod
    def from_args(arguments: dict[str, Any]) -> "DataStdInputSchema":
        input_data_file_path = arguments["input_data_file_path"]
        ddof = arguments.get("ddof", 1)
        return DataStdInputSchema.from_schema(
            input_data_file_path=input_data_file_path,
            ddof=ddof,
        )


async def handle_data_std(
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    data_std_input = DataStdInputSchema.from_args(arguments)
    std_df = data_std_input.df.std(ddof=data_std_input.ddof)

    # Convert the DataFrame to a dictionary format
    std_dict = {
        "description": f"Standard deviation values for each column with ddof={data_std_input.ddof}",
        "std_values": {col: str(val) if val is not None else None for col, val in zip(std_df.columns, std_df.row(0))},
    }

    return [
        types.TextContent(
            type="text",
            text=json.dumps(std_dict),
        )
    ]
