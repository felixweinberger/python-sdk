"""HTTP transport for MCP clients using StreamableHTTP."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import httpx
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from mcp.client.streamable_http import streamablehttp_client
from mcp.shared._httpx_utils import McpHttpClientFactory, create_mcp_http_client
from mcp.shared.message import SessionMessage


class HttpTransport:
    """Transport for HTTP connections using StreamableHTTP.

    This transport wraps the streamablehttp_client context manager
    to provide a consistent transport interface.
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        timeout: timedelta = timedelta(seconds=30),
        sse_read_timeout: timedelta = timedelta(seconds=60 * 5),
        terminate_on_close: bool = True,
        httpx_client_factory: McpHttpClientFactory = create_mcp_http_client,
        auth: httpx.Auth | None = None,
    ) -> None:
        """Initialize the HTTP transport.

        Args:
            url: The StreamableHTTP endpoint URL.
            headers: Optional headers to include in requests.
            timeout: HTTP timeout for regular operations.
            sse_read_timeout: Timeout for SSE read operations.
            terminate_on_close: Whether to terminate the session on close.
            httpx_client_factory: Factory function for creating HTTP clients.
            auth: Optional HTTPX authentication handler.
        """
        self.url = url
        self.headers = headers
        self.timeout = timeout
        self.sse_read_timeout = sse_read_timeout
        self.terminate_on_close = terminate_on_close
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
        """Connect to the server using StreamableHTTP.

        Yields:
            A tuple of (read_stream, write_stream) for bidirectional communication.
        """
        async with streamablehttp_client(
            url=self.url,
            headers=self.headers,
            timeout=self.timeout,
            sse_read_timeout=self.sse_read_timeout,
            terminate_on_close=self.terminate_on_close,
            httpx_client_factory=self.httpx_client_factory,
            auth=self.auth,
        ) as (read_stream, write_stream, _get_session_id):
            yield read_stream, write_stream
