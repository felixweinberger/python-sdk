"""High-level MCP Client with transport abstraction."""

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import timedelta
from typing import Any

import mcp.types as types
from mcp.client.session import (
    ClientSession,
    ListRootsFnT,
    LoggingFnT,
    MessageHandlerFnT,
    SamplingFnT,
)
from mcp.client.transports import HttpTransport, InMemoryTransport, Transport
from mcp.server import FastMCP
from mcp.server.lowlevel.server import Server

# Type alias for valid Client targets
ClientTarget = Server[Any] | FastMCP | Transport | str


def _infer_transport(target: ClientTarget) -> Transport:
    """Infer the appropriate transport for a given target.

    Args:
        target: The target to connect to. Can be:
            - Server or FastMCP instance: Uses InMemoryTransport
            - Transport instance: Uses the transport directly
            - URL string: Uses HttpTransport

    Returns:
        A Transport instance for the given target.

    Raises:
        TypeError: If the target type is not supported.
    """
    # Check if it's already a Transport
    if isinstance(target, Transport):
        return target

    # Check for Server or FastMCP using proper isinstance checks
    if isinstance(target, Server | FastMCP):
        return InMemoryTransport(target)

    # Check for URL string (explicit check for clarity even though type narrowing
    # already excludes other types at this point)
    if isinstance(target, str):  # pyright: ignore[reportUnnecessaryIsInstance]
        return HttpTransport(url=target)

    raise TypeError(
        f"Cannot infer transport for target of type {type(target).__name__}. "
        "Expected Server, FastMCP, Transport, or URL string."
    )


class Client:
    """High-level MCP client with automatic transport inference.

    The Client provides a simplified interface for connecting to MCP servers
    with automatic transport selection based on the target type:

    - Server/FastMCP instance: Uses in-memory transport for direct communication
    - Transport instance: Uses the provided transport directly
    - URL string: Uses HTTP transport (StreamableHTTP)

    Example usage:
        # Connect to a FastMCP server directly
        async with Client(server) as client:
            tools = await client.list_tools()

        # Connect to an HTTP endpoint
        async with Client("http://localhost:8000/mcp") as client:
            tools = await client.list_tools()

        # Use a custom transport
        transport = SSETransport(url="http://localhost:8000/sse")
        async with Client(transport) as client:
            tools = await client.list_tools()
    """

    def __init__(
        self,
        target: ClientTarget,
        *,
        read_timeout_seconds: timedelta | None = None,
        sampling_callback: SamplingFnT | None = None,
        list_roots_callback: ListRootsFnT | None = None,
        logging_callback: LoggingFnT | None = None,
        message_handler: MessageHandlerFnT | None = None,
        client_info: types.Implementation | None = None,
    ) -> None:
        """Initialize the client.

        Args:
            target: The target to connect to. Can be:
                - Server or FastMCP instance for in-memory connection
                - Transport instance for custom transport
                - URL string for HTTP connection
            read_timeout_seconds: Timeout for read operations.
            sampling_callback: Callback for handling sampling requests.
            list_roots_callback: Callback for handling list roots requests.
            logging_callback: Callback for handling log messages.
            message_handler: Custom message handler.
            client_info: Client implementation info.
        """
        self._transport = _infer_transport(target)
        self._read_timeout_seconds = read_timeout_seconds
        self._sampling_callback = sampling_callback
        self._list_roots_callback = list_roots_callback
        self._logging_callback = logging_callback
        self._message_handler = message_handler
        self._client_info = client_info
        self._session: ClientSession | None = None
        self._context: AbstractAsyncContextManager[Client] | None = None

    @property
    def session(self) -> ClientSession:
        """Get the current session.

        Raises:
            RuntimeError: If called outside of an async context manager.
        """
        if self._session is None:
            raise RuntimeError(
                "Client is not connected. Use 'async with Client(...) as client:'"
            )
        return self._session

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["Client"]:
        """Connect to the server and return the client.

        This is the primary way to use the Client. It handles connection
        setup, initialization, and cleanup automatically.

        Yields:
            The connected Client instance with an active session.

        Example:
            async with client.connect() as connected_client:
                tools = await connected_client.list_tools()
        """
        async with self._transport.connect() as (read_stream, write_stream):
            async with ClientSession(
                read_stream=read_stream,
                write_stream=write_stream,
                read_timeout_seconds=self._read_timeout_seconds,
                sampling_callback=self._sampling_callback,
                list_roots_callback=self._list_roots_callback,
                logging_callback=self._logging_callback,
                message_handler=self._message_handler,
                client_info=self._client_info,
            ) as session:
                await session.initialize()
                self._session = session
                try:
                    yield self
                finally:
                    self._session = None

    async def __aenter__(self) -> "Client":
        """Enter the async context manager."""
        if self._context is not None:
            raise RuntimeError("Client context manager is not reentrant")
        self._context = self.connect()
        return await self._context.__aenter__()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the async context manager."""
        if self._context is None:
            raise RuntimeError("Client context manager was not entered")
        try:
            await self._context.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self._context = None

    # Delegate common methods to session for convenience

    async def list_tools(self, cursor: str | None = None) -> types.ListToolsResult:
        """List available tools on the server."""
        return await self.session.list_tools(cursor=cursor)

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        read_timeout_seconds: timedelta | None = None,
    ) -> types.CallToolResult:
        """Call a tool on the server."""
        return await self.session.call_tool(
            name=name,
            arguments=arguments,
            read_timeout_seconds=read_timeout_seconds,
        )

    async def list_resources(
        self, cursor: str | None = None
    ) -> types.ListResourcesResult:
        """List available resources on the server."""
        return await self.session.list_resources(cursor=cursor)

    async def read_resource(self, uri: str) -> types.ReadResourceResult:
        """Read a resource from the server."""
        from pydantic import AnyUrl

        return await self.session.read_resource(AnyUrl(uri))

    async def list_prompts(self, cursor: str | None = None) -> types.ListPromptsResult:
        """List available prompts on the server."""
        return await self.session.list_prompts(cursor=cursor)

    async def get_prompt(
        self, name: str, arguments: dict[str, str] | None = None
    ) -> types.GetPromptResult:
        """Get a prompt from the server."""
        return await self.session.get_prompt(name=name, arguments=arguments)

    async def send_ping(self) -> types.EmptyResult:
        """Send a ping to the server."""
        return await self.session.send_ping()
