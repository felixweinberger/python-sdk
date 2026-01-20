"""Base transport protocol for MCP clients."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol, runtime_checkable

from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from mcp.shared.message import SessionMessage


@runtime_checkable
class Transport(Protocol):
    """Protocol defining the interface for MCP client transports.

    Transports provide a way to establish bidirectional communication
    with an MCP server. Each transport implementation handles the
    specifics of the underlying communication mechanism (HTTP, stdio, etc).

    The connect() method returns an async context manager that yields
    a tuple of (read_stream, write_stream) for communication.
    """

    @asynccontextmanager
    async def connect(
        self,
    ) -> AsyncIterator[
        tuple[
            MemoryObjectReceiveStream[SessionMessage | Exception],
            MemoryObjectSendStream[SessionMessage],
        ]
    ]:
        """Connect to the MCP server.

        Yields:
            A tuple of (read_stream, write_stream) for bidirectional communication.
            - read_stream: Stream for receiving messages from the server
            - write_stream: Stream for sending messages to the server
        """
        ...
        yield  # type: ignore[misc]
