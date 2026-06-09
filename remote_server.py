"""Remote MCP wrapper for pykrx-mcp (Streamable HTTP).

Exposes the pykrx MCP server over HTTP behind a secret URL path, so only
clients that know the token can reach it. Two important fixes are applied:

1) Streamable HTTP transport (path: /<MCP_TOKEN>/mcp) — this is what
   Claude's custom connectors expect (legacy SSE is rejected).
2) DNS-rebinding / Host-header protection is disabled, otherwise FastMCP
   returns "421 Misdirected Request" for requests arriving via a tunnel or
   cloud host (any Host header other than localhost).

Run locally:
    set MCP_TOKEN=your-long-random-token        (Windows)
    export MCP_TOKEN=your-long-random-token     (macOS/Linux)
    uvicorn remote_server:app --host 0.0.0.0 --port 8000

The MCP endpoint clients connect to is:
    https://<your-host>/<MCP_TOKEN>/mcp
"""
import os

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route

from mcp.server.transport_security import TransportSecuritySettings
from pykrx_mcp.server import mcp

TOKEN = os.environ.get("MCP_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit("MCP_TOKEN environment variable is required")

# Allow requests that arrive via a tunnel / cloud host (any Host header).
mcp.settings.transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=False
)


async def health(request):
    # Public health check used by hosting platforms. Does not leak the token.
    return PlainTextResponse("ok")


app = Starlette(
    routes=[
        Route("/", health),
        Mount(f"/{TOKEN}", app=mcp.streamable_http_app()),
    ]
)
