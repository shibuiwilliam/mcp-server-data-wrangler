from enum import Enum

from mcp import types

from .data_schema import DataSchemaInputSchema
from .data_shape import DataShapeInputSchema


class MCPServerDataWrangler(Enum):
    data_shape = ("data_shape", "Data shape of the input data")
    data_schema = ("data_schema", "Data schema of the input data")

    @staticmethod
    def from_str(name: str) -> "MCPServerDataWrangler":
        for d in MCPServerDataWrangler:
            if d.value[0] == name:
                return d
        raise ValueError(f"Invalid MCPServerDataWrangler: {name}")

    @staticmethod
    def tools() -> list[types.Tool]:
        return [
            types.Tool(
                name=MCPServerDataWrangler.data_shape.value[0],
                description=MCPServerDataWrangler.data_shape.value[1],
                inputSchema=DataShapeInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_schema.value[0],
                description=MCPServerDataWrangler.data_schema.value[1],
                inputSchema=DataSchemaInputSchema.input_schema(),
            ),
        ]
