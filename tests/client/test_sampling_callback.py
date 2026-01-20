import pytest

from mcp.client import Client
from mcp.client.session import ClientSession
from mcp.server.fastmcp import FastMCP
from mcp.shared.context import RequestContext
from mcp.types import (
    CreateMessageRequestParams,
    CreateMessageResult,
    SamplingMessage,
    TextContent,
)


@pytest.mark.anyio
async def test_sampling_callback():
    server = FastMCP("test")

    callback_return = CreateMessageResult(
        role="assistant",
        content=TextContent(
            type="text", text="This is a response from the sampling callback"
        ),
        model="test-model",
        stopReason="endTurn",
    )

    async def sampling_callback(
        context: RequestContext[ClientSession, None],
        params: CreateMessageRequestParams,
    ) -> CreateMessageResult:
        return callback_return

    @server.tool("test_sampling")
    async def test_sampling_tool(message: str):
        value = await server.get_context().session.create_message(
            messages=[
                SamplingMessage(
                    role="user", content=TextContent(type="text", text=message)
                )
            ],
            max_tokens=100,
        )
        assert value == callback_return
        return True

    # Test with sampling callback
    async with Client(server, sampling_callback=sampling_callback) as client:
        result = await client.call_tool(
            "test_sampling", {"message": "Test message for sampling"}
        )
        assert result.isError is False
        assert isinstance(result.content[0], TextContent)
        assert result.content[0].text == "true"

    # Test without sampling callback
    async with Client(server) as client:
        result = await client.call_tool(
            "test_sampling", {"message": "Test message for sampling"}
        )
        assert result.isError is True
        assert isinstance(result.content[0], TextContent)
        assert (
            result.content[0].text
            == "Error executing tool test_sampling: Sampling not supported"
        )
