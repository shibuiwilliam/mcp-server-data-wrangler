from typing import Any, Callable, Coroutine

from mcp import stdio_server, types
from mcp.server import InitializationOptions, NotificationOptions, Server

from .configurations import Settings
from .make_logger import make_logger
from .tools.tools import MCPServerDataWrangler

logger = make_logger(__name__)

settings = Settings()
server = Server(settings.APP_NAME)


@server.list_prompts()
async def list_prompts() -> list[types.Prompt]:
    """List available prompts for the data-wrangler MCP server.

    Returns:
        List of available prompts.
    """
    return []


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    """List available tools for the data-wrangler MCP server.

    Returns:
        List of available tools.
    """
    return MCPServerDataWrangler.tools()


@server.call_tool()
async def call_tool(
    tool_name: str,
    arguments: dict[str, Any],
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """Call a specific tool with the given arguments.

    Args:
        tool_name: Name of the tool to call.
        arguments: Arguments to pass to the tool.

    Returns:
        List of content items returned by the tool.

    Raises:
        ValueError: If the tool name is not recognized.
    """
    tool_handlers: dict[
        str,
        Callable[
            [dict[str, Any]], Coroutine[Any, Any, list[types.TextContent | types.ImageContent | types.EmbeddedResource]]
        ],
    ] = MCPServerDataWrangler.tool_to_handler()

    if tool_name not in tool_handlers:
        raise ValueError(f"Tool {tool_name} not found")

    return await tool_handlers[tool_name](arguments)


async def main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name=settings.APP_NAME,
                server_version=settings.APP_VERSION,
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(resources_changed=True),
                    experimental_capabilities={},
                ),
            ),
        )
