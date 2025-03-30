# mcp-data-wrangler: MCP server for Data Wrangling

## Overview

This is a Model Context Protocol server for Data Wrangling, providing a standardized interface for data preprocessing, transformation, and analysis tasks. It enables seamless integration of data wrangling operations into the MCP ecosystem.

## Features

* Data aggregation
* Descriptive statistics

## Run this project locally

This project is not yet set up for ephemeral environments (e.g. `uvx` usage). Run this project locally by cloning this repo:

```bash
git clone https://github.com/yourusername/mcp-data-wrangler.git
cd mcp-data-wrangler
```

You can launch the MCP inspector via npm:

```bash
npx @modelcontextprotocol/inspector uv --directory=src/mcp_data_wrangler run mcp-data-wrangler
```

Upon launching, the Inspector will display a URL that you can access in your browser to begin debugging.

OR Add this tool as a MCP server:

```json
{
  "data-wrangler": {
    "command": "uv",
    "args": [
      "--directory",
      "/path/to/mcp-data-wrangler",
      "run",
      "mcp-data-wrangler"
    ]
  }
}
```

## Development

1. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -e ".[dev]"
```

3. Run tests:

```bash
pytest -s -v tests/
```

## [License](LICENSE)
