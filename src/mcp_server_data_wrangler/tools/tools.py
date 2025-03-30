from enum import Enum
from typing import Callable

from mcp import types

from . import (
    DataCountInputSchema,
    DataEstimatedSizeInputSchema,
    DataMaxInputSchema,
    DataMeanInputSchema,
    DataMedianInputSchema,
    DataMinInputSchema,
    DataProductInputSchema,
    DataQuantileInputSchema,
    DataSchemaInputSchema,
    DataShapeInputSchema,
    DataStdInputSchema,
    DataVarInputSchema,
    DescribeDataInputSchema,
    handle_data_count,
    handle_data_estimated_size,
    handle_data_max,
    handle_data_max_horizontal,
    handle_data_mean,
    handle_data_mean_horizontal,
    handle_data_median,
    handle_data_min,
    handle_data_min_horizontal,
    handle_data_product,
    handle_data_quantile,
    handle_data_schema,
    handle_data_shape,
    handle_data_std,
    handle_data_var,
    handle_describe_data,
)


class MCPServerDataWrangler(Enum):
    data_shape = ("data_shape", "Data shape of the input data")
    data_schema = ("data_schema", "Data schema of the input data")
    describe_data = ("describe_data", "Summary statistics of the input data")
    data_estimated_size = ("data_estimated_size", "Estimated size of the input data")
    data_count = ("data_count", "Number of non-null elements for each column")
    data_max = ("data_max", "Maximum values for each column")
    data_max_horizontal = ("data_max_horizontal", "Maximum values across columns for each row")
    data_min = ("data_min", "Minimum values for each column")
    data_min_horizontal = ("data_min_horizontal", "Minimum values across columns for each row")
    data_mean = ("data_mean", "Mean values for each column")
    data_mean_horizontal = ("data_mean_horizontal", "Mean values across columns for each row")
    data_median = ("data_median", "Median values for each column")
    data_product = ("data_product", "Product values for each column")
    data_quantile = ("data_quantile", "Quantile values for each column")
    data_std = ("data_std", "Standard deviation values for each column")
    data_var = ("data_var", "Variance values for each column")

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
            types.Tool(
                name=MCPServerDataWrangler.describe_data.value[0],
                description=MCPServerDataWrangler.describe_data.value[1],
                inputSchema=DescribeDataInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_estimated_size.value[0],
                description=MCPServerDataWrangler.data_estimated_size.value[1],
                inputSchema=DataEstimatedSizeInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_count.value[0],
                description=MCPServerDataWrangler.data_count.value[1],
                inputSchema=DataCountInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_max.value[0],
                description=MCPServerDataWrangler.data_max.value[1],
                inputSchema=DataMaxInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_max_horizontal.value[0],
                description=MCPServerDataWrangler.data_max_horizontal.value[1],
                inputSchema=DataMaxInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_min.value[0],
                description=MCPServerDataWrangler.data_min.value[1],
                inputSchema=DataMinInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_min_horizontal.value[0],
                description=MCPServerDataWrangler.data_min_horizontal.value[1],
                inputSchema=DataMinInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_mean.value[0],
                description=MCPServerDataWrangler.data_mean.value[1],
                inputSchema=DataMeanInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_mean_horizontal.value[0],
                description=MCPServerDataWrangler.data_mean_horizontal.value[1],
                inputSchema=DataMeanInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_median.value[0],
                description=MCPServerDataWrangler.data_median.value[1],
                inputSchema=DataMedianInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_product.value[0],
                description=MCPServerDataWrangler.data_product.value[1],
                inputSchema=DataProductInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_quantile.value[0],
                description=MCPServerDataWrangler.data_quantile.value[1],
                inputSchema=DataQuantileInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_std.value[0],
                description=MCPServerDataWrangler.data_std.value[1],
                inputSchema=DataStdInputSchema.input_schema(),
            ),
            types.Tool(
                name=MCPServerDataWrangler.data_var.value[0],
                description=MCPServerDataWrangler.data_var.value[1],
                inputSchema=DataVarInputSchema.input_schema(),
            ),
        ]

    @staticmethod
    def tool_to_handler() -> dict[str, Callable]:
        return {
            MCPServerDataWrangler.data_shape.value[0]: handle_data_shape,
            MCPServerDataWrangler.data_schema.value[0]: handle_data_schema,
            MCPServerDataWrangler.describe_data.value[0]: handle_describe_data,
            MCPServerDataWrangler.data_estimated_size.value[0]: handle_data_estimated_size,
            MCPServerDataWrangler.data_count.value[0]: handle_data_count,
            MCPServerDataWrangler.data_max.value[0]: handle_data_max,
            MCPServerDataWrangler.data_max_horizontal.value[0]: handle_data_max_horizontal,
            MCPServerDataWrangler.data_min.value[0]: handle_data_min,
            MCPServerDataWrangler.data_min_horizontal.value[0]: handle_data_min_horizontal,
            MCPServerDataWrangler.data_mean.value[0]: handle_data_mean,
            MCPServerDataWrangler.data_mean_horizontal.value[0]: handle_data_mean_horizontal,
            MCPServerDataWrangler.data_median.value[0]: handle_data_median,
            MCPServerDataWrangler.data_product.value[0]: handle_data_product,
            MCPServerDataWrangler.data_quantile.value[0]: handle_data_quantile,
            MCPServerDataWrangler.data_std.value[0]: handle_data_std,
            MCPServerDataWrangler.data_var.value[0]: handle_data_var,
        }
