"""Tests for the high-level Client class."""

import sys

import pytest

if sys.version_info >= (3, 11):
    from builtins import ExceptionGroup
else:
    from exceptiongroup import ExceptionGroup

from mcp.client import Client
from mcp.client.transports import HttpTransport, InMemoryTransport
from mcp.server import FastMCP


class TestClientInit:
    """Tests for Client initialization."""

    def test_client_with_fastmcp(self) -> None:
        """Client should accept FastMCP instance."""
        server = FastMCP(name="test")
        client = Client(server)
        assert isinstance(client._transport, InMemoryTransport)

    def test_client_with_url_string(self) -> None:
        """Client should accept URL string."""
        client = Client("http://localhost:8000/mcp")
        assert isinstance(client._transport, HttpTransport)

    def test_client_with_transport(self) -> None:
        """Client should accept Transport instance."""
        transport = HttpTransport(url="http://localhost:8000/mcp")
        client = Client(transport)
        assert client._transport is transport


class TestClientSessionAccess:
    """Tests for Client session access."""

    def test_session_raises_when_not_connected(self) -> None:
        """Accessing session before connection should raise."""
        client = Client("http://localhost:8000/mcp")
        with pytest.raises(RuntimeError, match="Client is not connected"):
            _ = client.session


class TestClientContextManagerEdgeCases:
    """Tests for Client context manager edge cases."""

    @pytest.mark.anyio
    async def test_double_entry_raises_error(self) -> None:
        """Entering context manager twice should raise RuntimeError."""
        server = FastMCP(name="test")
        client = Client(server)

        async with client:
            with pytest.raises(RuntimeError, match="not reentrant"):
                await client.__aenter__()

    @pytest.mark.anyio
    async def test_exit_without_entry_raises_error(self) -> None:
        """Exiting without entering should raise RuntimeError."""
        server = FastMCP(name="test")
        client = Client(server)

        with pytest.raises(RuntimeError, match="was not entered"):
            await client.__aexit__(None, None, None)

    @pytest.mark.anyio
    async def test_context_reusable_after_exit(self) -> None:
        """Client should be reusable after proper exit."""
        server = FastMCP(name="test")
        client = Client(server)

        # First use
        async with client:
            assert client._session is not None

        # Verify cleanup
        assert client._session is None
        assert client._context is None

        # Second use should work
        async with client:
            assert client._session is not None

    @pytest.mark.anyio
    async def test_context_cleanup_on_error(self) -> None:
        """Client context should be cleaned up even on error."""
        server = FastMCP(name="test")
        client = Client(server)

        # Exceptions get wrapped in ExceptionGroup by anyio's TaskGroup
        with pytest.raises(ExceptionGroup) as exc_info:
            async with client:
                assert client._session is not None
                raise ValueError("test error")

        # Verify the original error is in the group
        assert any(
            isinstance(e, ValueError) and "test error" in str(e)
            for e in exc_info.value.exceptions
            if isinstance(e, ValueError)
        ) or any(
            isinstance(e, ExceptionGroup)
            and any(isinstance(inner, ValueError) for inner in e.exceptions)
            for e in exc_info.value.exceptions
        )

        # Context should be cleaned up
        assert client._session is None
        assert client._context is None


class TestClientWithFastMCP:
    """Tests for Client with FastMCP server."""

    @pytest.mark.anyio
    async def test_client_connect_and_list_tools(self) -> None:
        """Client should be able to connect and list tools."""
        server = FastMCP(name="test")

        @server.tool()
        def greet(name: str) -> str:
            """Greet someone."""
            return f"Hello, {name}!"

        async with Client(server) as client:
            result = await client.list_tools()
            assert len(result.tools) == 1
            assert result.tools[0].name == "greet"

    @pytest.mark.anyio
    async def test_client_call_tool(self) -> None:
        """Client should be able to call tools."""
        server = FastMCP(name="test")

        @server.tool()
        def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        async with Client(server) as client:
            result = await client.call_tool("add", {"a": 2, "b": 3})
            assert len(result.content) == 1
            assert result.content[0].text == "5"  # type: ignore[union-attr]

    @pytest.mark.anyio
    async def test_client_list_resources(self) -> None:
        """Client should be able to list resources."""
        server = FastMCP(name="test")

        @server.resource("resource://greeting")
        def get_greeting() -> str:
            return "Hello, World!"

        async with Client(server) as client:
            result = await client.list_resources()
            assert len(result.resources) == 1
            assert str(result.resources[0].uri) == "resource://greeting"

    @pytest.mark.anyio
    async def test_client_read_resource(self) -> None:
        """Client should be able to read resources."""
        server = FastMCP(name="test")

        @server.resource("resource://greeting")
        def get_greeting() -> str:
            return "Hello, World!"

        async with Client(server) as client:
            result = await client.read_resource("resource://greeting")
            assert len(result.contents) == 1
            assert result.contents[0].text == "Hello, World!"  # type: ignore[union-attr]

    @pytest.mark.anyio
    async def test_client_list_prompts(self) -> None:
        """Client should be able to list prompts."""
        server = FastMCP(name="test")

        @server.prompt()
        def greeting_prompt(name: str) -> str:
            """Greeting prompt."""
            return f"Say hello to {name}"

        async with Client(server) as client:
            result = await client.list_prompts()
            assert len(result.prompts) == 1
            assert result.prompts[0].name == "greeting_prompt"

    @pytest.mark.anyio
    async def test_client_get_prompt(self) -> None:
        """Client should be able to get a prompt."""
        server = FastMCP(name="test")

        @server.prompt()
        def greeting_prompt(name: str) -> str:
            """Greeting prompt."""
            return f"Say hello to {name}"

        async with Client(server) as client:
            result = await client.get_prompt("greeting_prompt", {"name": "Alice"})
            assert len(result.messages) == 1

    @pytest.mark.anyio
    async def test_client_ping(self) -> None:
        """Client should be able to ping the server."""
        server = FastMCP(name="test")

        async with Client(server) as client:
            result = await client.send_ping()
            assert result is not None

    @pytest.mark.anyio
    async def test_client_context_manager_cleanup(self) -> None:
        """Client should clean up session after context exit."""
        server = FastMCP(name="test")
        client = Client(server)

        async with client:
            assert client._session is not None

        assert client._session is None

    @pytest.mark.anyio
    async def test_client_connect_method(self) -> None:
        """Client.connect() should work as async context manager."""
        server = FastMCP(name="test")
        client = Client(server)

        async with client.connect() as connected_client:
            assert connected_client is client
            assert client._session is not None

        assert client._session is None
