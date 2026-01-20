"""In-memory transport for direct Server/FastMCP connections."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from mcp.shared.message import SessionMessage

if TYPE_CHECKING:
    from mcp.server import FastMCP
    from mcp.server.lowlevel.server import Server


class InMemoryTransport:
    """Transport for in-memory connections to a Server or FastMCP instance.

    This transport creates memory streams for direct communication with
    a server running in the same process, without any network overhead.
    """

    def __init__(self, server: "Server[Any] | FastMCP") -> None:
        """Initialize the in-memory transport.

        Args:
            server: The Server or FastMCP instance to connect to.
        """
        self._server = server

    def _get_mcp_server(self) -> "Server[Any]":
        """Get the underlying MCP Server instance."""
        # FastMCP wraps Server in _mcp_server attribute
        if hasattr(self._server, "_mcp_server"):
            return self._server._mcp_server  # type: ignore[union-attr]
        return self._server  # type: ignore[return-value]

    @asynccontextmanager
    async def connect(
        self,
    ) -> AsyncIterator[
        tuple[
            MemoryObjectReceiveStream[SessionMessage | Exception],
            MemoryObjectSendStream[SessionMessage],
        ]
    ]:
        """Connect to the server using in-memory streams.

        Yields:
            A tuple of (read_stream, write_stream) for bidirectional communication.
        """
        # Create streams for both directions
        # Server-to-client can contain exceptions (for transport errors)
        server_to_client_send, server_to_client_receive = (
            anyio.create_memory_object_stream[SessionMessage | Exception](1)
        )
        # Client-to-server only contains SessionMessage
        client_to_server_send, client_to_server_receive = (
            anyio.create_memory_object_stream[SessionMessage](1)
        )

        mcp_server = self._get_mcp_server()

        async with anyio.create_task_group() as tg:
            async with (
                server_to_client_receive,
                client_to_server_send,
                client_to_server_receive,
                server_to_client_send,
            ):
                # Server.run expects SessionMessage | Exception for read stream,
                # but we only send SessionMessage. Type ignore is safe here.
                tg.start_soon(
                    mcp_server.run,
                    client_to_server_receive,  # type: ignore[arg-type]
                    server_to_client_send,
                    mcp_server.create_initialization_options(),
                )

                try:
                    yield server_to_client_receive, client_to_server_send
                finally:
                    tg.cancel_scope.cancel()
