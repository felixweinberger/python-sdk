"""Tests for transport inference logic."""

import pytest

from mcp.client.client import _infer_transport
from mcp.client.transports import HttpTransport, InMemoryTransport, SSETransport
from mcp.server import FastMCP, Server


class TestTransportInference:
    """Tests for the _infer_transport function."""

    def test_infer_transport_from_url_string(self) -> None:
        """URL strings should create HttpTransport."""
        transport = _infer_transport("http://localhost:8000/mcp")
        assert isinstance(transport, HttpTransport)
        assert transport.url == "http://localhost:8000/mcp"

    def test_infer_transport_from_https_url(self) -> None:
        """HTTPS URLs should also create HttpTransport."""
        transport = _infer_transport("https://example.com/mcp")
        assert isinstance(transport, HttpTransport)
        assert transport.url == "https://example.com/mcp"

    def test_infer_transport_from_server(self) -> None:
        """Server instances should create InMemoryTransport."""
        server = Server(name="test")
        transport = _infer_transport(server)
        assert isinstance(transport, InMemoryTransport)

    def test_infer_transport_from_fastmcp(self) -> None:
        """FastMCP instances should create InMemoryTransport."""
        server = FastMCP(name="test")
        transport = _infer_transport(server)
        assert isinstance(transport, InMemoryTransport)

    def test_infer_transport_from_http_transport(self) -> None:
        """HttpTransport instances should be returned as-is."""
        original = HttpTransport(url="http://localhost:8000/mcp")
        transport = _infer_transport(original)
        assert transport is original

    def test_infer_transport_from_sse_transport(self) -> None:
        """SSETransport instances should be returned as-is."""
        original = SSETransport(url="http://localhost:8000/sse")
        transport = _infer_transport(original)
        assert transport is original

    def test_infer_transport_from_memory_transport(self) -> None:
        """InMemoryTransport instances should be returned as-is."""
        server = Server(name="test")
        original = InMemoryTransport(server)
        transport = _infer_transport(original)
        assert transport is original

    def test_infer_transport_raises_for_invalid_type(self) -> None:
        """Invalid types should raise TypeError."""
        with pytest.raises(TypeError, match="Cannot infer transport"):
            _infer_transport(12345)  # type: ignore[arg-type]

    def test_infer_transport_raises_for_none(self) -> None:
        """None should raise TypeError."""
        with pytest.raises(TypeError, match="Cannot infer transport"):
            _infer_transport(None)  # type: ignore[arg-type]

    def test_infer_transport_raises_for_dict(self) -> None:
        """Dicts should raise TypeError."""
        with pytest.raises(TypeError, match="Cannot infer transport"):
            _infer_transport({"url": "http://localhost"})  # type: ignore[arg-type]
