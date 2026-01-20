from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from typing import Any, Generic, TypeVar
from unittest.mock import patch

import anyio
import pytest
from anyio.streams.memory import (
    MemoryObjectReceiveStream,
    MemoryObjectSendStream,
    MemoryObjectStreamStatistics,
)

from mcp.client.transports._memory import InMemoryTransport
from mcp.shared.message import SessionMessage
from mcp.types import (
    JSONRPCNotification,
    JSONRPCRequest,
)

T = TypeVar("T")


class SpyMemoryObjectSendStream(Generic[T]):
    """A send stream wrapper that records all messages for test inspection."""

    def __init__(self, original_stream: MemoryObjectSendStream[T]):
        self._original_stream = original_stream
        self.sent_messages: list[T] = []

    async def send(self, item: T) -> None:
        self.sent_messages.append(item)
        await self._original_stream.send(item)

    def send_nowait(self, item: T) -> None:
        self.sent_messages.append(item)
        self._original_stream.send_nowait(item)

    def clone(self) -> "SpyMemoryObjectSendStream[T]":
        cloned: SpyMemoryObjectSendStream[T] = SpyMemoryObjectSendStream(
            self._original_stream.clone()
        )
        cloned.sent_messages = self.sent_messages  # Share message list
        return cloned

    def close(self) -> None:
        self._original_stream.close()

    async def aclose(self) -> None:
        await self._original_stream.aclose()

    def statistics(self) -> MemoryObjectStreamStatistics:
        return self._original_stream.statistics()

    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:
        return self._original_stream.extra_attributes

    def extra(self, attribute: Any, default: Any = None) -> Any:
        return self._original_stream.extra(attribute, default)

    async def __aenter__(self) -> "SpyMemoryObjectSendStream[T]":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()


class StreamSpyCollection:
    def __init__(
        self,
        client_spy: SpyMemoryObjectSendStream[SessionMessage],
        server_spy: SpyMemoryObjectSendStream[SessionMessage],
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
    from typing import cast

    client_spy: SpyMemoryObjectSendStream[SessionMessage] | None = None
    server_spy: SpyMemoryObjectSendStream[SessionMessage] | None = None

    # Store references to our spy objects
    def capture_spies(
        c_spy: SpyMemoryObjectSendStream[SessionMessage],
        s_spy: SpyMemoryObjectSendStream[SessionMessage],
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
        # Create streams using SessionMessage only
        # (Exception type in Transport protocol is for transport errors)
        server_to_client_send, server_to_client_receive = (
            anyio.create_memory_object_stream[SessionMessage](1)
        )
        client_to_server_send, client_to_server_receive = (
            anyio.create_memory_object_stream[SessionMessage](1)
        )

        # Create spy wrappers for the send streams
        spy_client_write: SpyMemoryObjectSendStream[SessionMessage] = (
            SpyMemoryObjectSendStream(client_to_server_send)
        )
        spy_server_write: SpyMemoryObjectSendStream[SessionMessage] = (
            SpyMemoryObjectSendStream(server_to_client_send)
        )

        # Capture references for the test to use
        capture_spies(spy_client_write, spy_server_write)

        mcp_server = self._get_mcp_server()

        async with anyio.create_task_group() as tg:
            # Use ExitStack pattern for proper cleanup of all streams
            async with server_to_client_receive:
                async with spy_client_write:
                    async with client_to_server_receive:
                        async with spy_server_write:
                            # Cast streams to match Server.run signature
                            tg.start_soon(
                                mcp_server.run,
                                cast(
                                    MemoryObjectReceiveStream[
                                        SessionMessage | Exception
                                    ],
                                    client_to_server_receive,
                                ),
                                cast(
                                    MemoryObjectSendStream[SessionMessage],
                                    spy_server_write,
                                ),
                                mcp_server.create_initialization_options(),
                            )

                            try:
                                # Client reads from server_to_client, writes via spy
                                # Cast receive stream to include Exception type
                                yield (
                                    cast(
                                        MemoryObjectReceiveStream[
                                            SessionMessage | Exception
                                        ],
                                        server_to_client_receive,
                                    ),
                                    cast(
                                        MemoryObjectSendStream[SessionMessage],
                                        spy_client_write,
                                    ),
                                )
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
