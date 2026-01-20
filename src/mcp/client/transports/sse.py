"""SSE transport for MCP clients (legacy)."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from mcp.client.sse import sse_client
from mcp.shared._httpx_utils import McpHttpClientFactory, create_mcp_http_client
from mcp.shared.message import SessionMessage


class SSETransport:
    """Transport for SSE connections (legacy).

    This transport wraps the sse_client context manager to provide
    a consistent transport interface. Use HttpTransport for new
    implementations when possible.
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        timeout: float = 5,
        sse_read_timeout: float = 60 * 5,
        httpx_client_factory: McpHttpClientFactory = create_mcp_http_client,
        auth: httpx.Auth | None = None,
    ) -> None:
        """Initialize the SSE transport.

        Args:
            url: The SSE endpoint URL.
            headers: Optional headers to include in requests.
            timeout: HTTP timeout for regular operations.
            sse_read_timeout: Timeout for SSE read operations.
            httpx_client_factory: Factory function for creating HTTP clients.
            auth: Optional HTTPX authentication handler.
        """
        self.url = url
        self.headers = headers
        self.timeout = timeout
        self.sse_read_timeout = sse_read_timeout
        self.httpx_client_factory = httpx_client_factory
        self.auth = auth

    @asynccontextmanager
    async def connect(
        self,
    ) -> AsyncIterator[
        tuple[
            MemoryObjectReceiveStream[SessionMessage | Exception],
            MemoryObjectSendStream[SessionMessage],
        ]
    ]:
        """Connect to the server using SSE.

        Yields:
            A tuple of (read_stream, write_stream) for bidirectional communication.
        """
        async with sse_client(
            url=self.url,
            headers=self.headers,
            timeout=self.timeout,
            sse_read_timeout=self.sse_read_timeout,
            httpx_client_factory=self.httpx_client_factory,
            auth=self.auth,
        ) as (read_stream, write_stream):
            yield read_stream, write_stream
