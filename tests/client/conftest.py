from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import patch

import anyio
import pytest
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from mcp.client.transports._memory import InMemoryTransport
from mcp.shared.message import SessionMessage
from mcp.types import (
    JSONRPCNotification,
    JSONRPCRequest,
)


class SpyMemoryObjectSendStream:
    def __init__(self, original_stream: MemoryObjectSendStream[Any]):
        self.original_stream = original_stream
        self.sent_messages: list[SessionMessage] = []

    async def send(self, message: SessionMessage) -> None:
        self.sent_messages.append(message)
        await self.original_stream.send(message)

    async def aclose(self) -> None:
        await self.original_stream.aclose()

    async def __aenter__(self) -> "SpyMemoryObjectSendStream":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()


class StreamSpyCollection:
    def __init__(
        self,
        client_spy: SpyMemoryObjectSendStream,
        server_spy: SpyMemoryObjectSendStream,
    ):
        self.client = client_spy
        self.server = server_spy

    def clear(self) -> None:
        """Clear all captured messages."""
        self.client.sent_messages.clear()
        self.server.sent_messages.clear()

    def get_client_requests(self, method: str | None = None) -> list[JSONRPCRequest]:
        """Get client-sent requests, optionally filtered by method."""
        return [
            req.message.root
            for req in self.client.sent_messages
            if isinstance(req.message.root, JSONRPCRequest)
            and (method is None or req.message.root.method == method)
        ]

    def get_server_requests(self, method: str | None = None) -> list[JSONRPCRequest]:
        """Get server-sent requests, optionally filtered by method."""
        return [
            req.message.root
            for req in self.server.sent_messages
            if isinstance(req.message.root, JSONRPCRequest)
            and (method is None or req.message.root.method == method)
        ]

    def get_client_notifications(
        self, method: str | None = None
    ) -> list[JSONRPCNotification]:
        """Get client-sent notifications, optionally filtered by method."""
        return [
            notif.message.root
            for notif in self.client.sent_messages
            if isinstance(notif.message.root, JSONRPCNotification)
            and (method is None or notif.message.root.method == method)
        ]

    def get_server_notifications(
        self, method: str | None = None
    ) -> list[JSONRPCNotification]:
        """Get server-sent notifications, optionally filtered by method."""
        return [
            notif.message.root
            for notif in self.server.sent_messages
            if isinstance(notif.message.root, JSONRPCNotification)
            and (method is None or notif.message.root.method == method)
        ]


@pytest.fixture
def stream_spy():
    """Fixture that provides spies for both client and server write streams.

    Example usage:
        async def test_something(stream_spy):
            # ... set up server and client ...

            spies = stream_spy()

            # Run some operation that sends messages
            await client.some_operation()

            # Check the messages
            requests = spies.get_client_requests(method="some/method")
            assert len(requests) == 1

            # Clear for the next operation
            spies.clear()
    """
    client_spy: SpyMemoryObjectSendStream | None = None
    server_spy: SpyMemoryObjectSendStream | None = None

    # Store references to our spy objects
    def capture_spies(
        c_spy: SpyMemoryObjectSendStream, s_spy: SpyMemoryObjectSendStream
    ) -> None:
        nonlocal client_spy, server_spy
        client_spy = c_spy
        server_spy = s_spy

    @asynccontextmanager
    async def patched_connect(
        self: InMemoryTransport,
    ) -> AsyncIterator[
        tuple[
            MemoryObjectReceiveStream[SessionMessage | Exception],
            MemoryObjectSendStream[SessionMessage],
        ]
    ]:
        """Patched connect method that wraps streams with spies."""
        # Create streams for both directions
        server_to_client_send, server_to_client_receive = (
            anyio.create_memory_object_stream[SessionMessage | Exception](1)
        )
        client_to_server_send, client_to_server_receive = (
            anyio.create_memory_object_stream[SessionMessage](1)
        )

        # Create spy wrappers for the send streams
        spy_client_write = SpyMemoryObjectSendStream(client_to_server_send)
        spy_server_write = SpyMemoryObjectSendStream(server_to_client_send)

        # Capture references for the test to use
        capture_spies(spy_client_write, spy_server_write)

        mcp_server = self._get_mcp_server()

        async with anyio.create_task_group() as tg:
            async with (
                server_to_client_receive,
                spy_client_write,  # type: ignore[arg-type]
                client_to_server_receive,
                spy_server_write,  # type: ignore[arg-type]
            ):
                tg.start_soon(
                    mcp_server.run,
                    client_to_server_receive,  # type: ignore[arg-type]
                    spy_server_write,  # type: ignore[arg-type]
                    mcp_server.create_initialization_options(),
                )

                try:
                    # Client reads from server_to_client, writes via spy
                    yield server_to_client_receive, spy_client_write  # type: ignore[misc]
                finally:
                    tg.cancel_scope.cancel()

    # Apply the patch for the duration of the test
    with patch.object(InMemoryTransport, "connect", patched_connect):
        # Return a collection with helper methods
        def get_spy_collection() -> StreamSpyCollection:
            assert client_spy is not None, "client_spy was not initialized"
            assert server_spy is not None, "server_spy was not initialized"
            return StreamSpyCollection(client_spy, server_spy)

        yield get_spy_collection
