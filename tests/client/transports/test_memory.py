"""Tests for InMemoryTransport."""

import pytest

from mcp.client.transports import InMemoryTransport
from mcp.server import FastMCP, Server


class TestInMemoryTransport:
    """Tests for InMemoryTransport."""

    def test_init_with_server(self) -> None:
        """Should accept a Server instance."""
        server = Server(name="test")
        transport = InMemoryTransport(server)
        assert transport._server is server

    def test_init_with_fastmcp(self) -> None:
        """Should accept a FastMCP instance."""
        server = FastMCP(name="test")
        transport = InMemoryTransport(server)
        assert transport._server is server

    def test_get_mcp_server_from_server(self) -> None:
        """Should return the Server directly."""
        server = Server(name="test")
        transport = InMemoryTransport(server)
        assert transport._get_mcp_server() is server

    def test_get_mcp_server_from_fastmcp(self) -> None:
        """Should extract _mcp_server from FastMCP."""
        fastmcp = FastMCP(name="test")
        transport = InMemoryTransport(fastmcp)
        assert transport._get_mcp_server() is fastmcp._mcp_server

    @pytest.mark.anyio
    async def test_connect_yields_streams(self) -> None:
        """Should yield read and write streams."""
        server = FastMCP(name="test")

        @server.tool()
        def test_tool() -> str:
            return "hello"

        transport = InMemoryTransport(server)

        async with transport.connect() as (read_stream, write_stream):
            # Verify we got stream objects
            assert read_stream is not None
            assert write_stream is not None
            # Verify they have the expected methods
            assert hasattr(read_stream, "receive")
            assert hasattr(write_stream, "send")
