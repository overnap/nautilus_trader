import asyncio

import pytest

from nautilus_trader.adapters.upbit.websocket.client import UpbitWebSocketClient
from nautilus_trader.common.component import LiveClock


@pytest.mark.asyncio()
async def test_upbit_websocket_client():
    clock = LiveClock()

    client = UpbitWebSocketClient(
        clock=clock,
        handler=print,
        handler_reconnect=None,
        url="wss://api.upbit.com/websocket/v1",
        loop=asyncio.get_event_loop(),
    )

    await client.connect()
    await client._subscribe("ticker", "KRW-BTC")

    await asyncio.sleep(4)
    await client.disconnect()
