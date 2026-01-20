import anyio
import pytest
from pydantic import AnyUrl

from mcp.client import Client
from mcp.server.fastmcp import FastMCP

_sleep_time_seconds = 0.01
_resource_name = "slow://slow_resource"


@pytest.mark.anyio
async def test_messages_are_executed_concurrently():
    server = FastMCP("test")

    @server.tool("sleep")
    async def sleep_tool():
        await anyio.sleep(_sleep_time_seconds)
        return "done"

    @server.resource(_resource_name)
    async def slow_resource():
        await anyio.sleep(_sleep_time_seconds)
        return "slow"

    async with Client(server) as client:
        start_time = anyio.current_time()
        async with anyio.create_task_group() as tg:
            for _ in range(10):
                tg.start_soon(client.session.call_tool, "sleep")
                tg.start_soon(client.session.read_resource, AnyUrl(_resource_name))

        end_time = anyio.current_time()

        duration = end_time - start_time
        assert duration < 6 * _sleep_time_seconds
        print(duration)


def main():
    anyio.run(test_messages_are_executed_concurrently)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.DEBUG)

    main()
