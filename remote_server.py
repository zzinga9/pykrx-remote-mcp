"""Remote MCP wrapper for pykrx-mcp (Streamable HTTP)."""
import contextlib
import os

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route

from mcp.server.transport_security import TransportSecuritySettings
from pykrx_mcp.server import mcp

TOKEN = os.environ.get("MCP_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit("MCP_TOKEN environment variable is required")

# Allow requests arriving via a tunnel / cloud host (any Host header).
mcp.settings.transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=False
)

# Build the Streamable HTTP sub-app (this also creates the session manager).
streamable_app = mcp.streamable_http_app()


async def health(request):
    return PlainTextResponse("ok")


# Starlette does NOT run a mounted sub-app's lifespan, so we run the MCP
# session manager from the OUTER app's lifespan. Without this the streamable
# session manager's task group is never initialized.
@contextlib.asynccontextmanager
async def lifespan(app):
    async with mcp.session_manager.run():
        yield


app = Starlette(
    routes=[
        Route("/", health),
        Mount(f"/{TOKEN}", app=streamable_app),
    ],
    lifespan=lifespan,
)
