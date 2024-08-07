# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2024 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio
import json
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any

from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.nautilus_pyo3 import WebSocketClient
from nautilus_trader.core.nautilus_pyo3 import WebSocketConfig
from nautilus_trader.core.nautilus_pyo3 import UUID4

# TODO: 함수 피쳐들 치기 (완)
# TODO: 테스트 코드 작성
# TODO: Upbit Symbol 작성
# TODO: doc 작성
# TODO: 코드 타입을 enum으로?
# TODO: 오더북 모아보기 피쳐


class UpbitWebSocketClient:
    """
    Provides a `Binance` streaming WebSocket client.

    Parameters
    ----------
    clock : LiveClock
        The clock for the client.
    url : str
        The base URL for the WebSocket connection.
    handler : Callable[[bytes], None]
        The callback handler for message events.
    handler_reconnect : Callable[..., Awaitable[None]], optional
        The callback handler to be called on reconnect.
    loop : asyncio.AbstractEventLoop
        The event loop for the client.

    References
    ----------
    https://docs.upbit.com/reference

    """

    def __init__(
        self,
        clock: LiveClock,
        url: str,
        handler: Callable[[bytes], None],
        handler_reconnect: Callable[..., Awaitable[None]] | None,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._clock = clock
        self._log: Logger = Logger(type(self).__name__)

        self._url: str = url
        self._handler: Callable[[bytes], None] = handler
        self._handler_reconnect: Callable[..., Awaitable[None]] | None = handler_reconnect
        self._loop = loop

        self._codes: dict[str, list[str]] = {"ticker": [], "trade": [], "orderbook": []}
        self._client: WebSocketClient | None = None
        self._is_connecting = False
        self._ticket: UUID4 | None = None

    @property
    def url(self) -> str:
        """
        Return the server URL being used by the client.

        Returns
        -------
        str

        """
        return self._url

    @property
    def subscriptions(self) -> dict[str, list[str]]:
        """
        Return the current active subscriptions for the client.

        Returns
        -------
        str

        """
        return self._codes.copy()

    @property
    def has_subscriptions(self) -> bool:
        """
        Return whether the client has subscriptions.

        Returns
        -------
        bool

        """
        return bool(self._codes["ticker"] or self._codes["trade"] or self._codes["orderbook"])

    async def connect(self) -> None:
        """
        Connect a websocket client to the server.
        """
        self._log.debug(f"Connecting to {self._url}...")
        self._is_connecting = True

        config = WebSocketConfig(
            url=self._url,
            handler=self._handler,
            heartbeat=60,  # TODO: 확인
            headers=[],  # TODO: private 연결 위한 JWT 헤더
            ping_handler=self._handle_ping,
        )

        self._ticket = UUID4()
        self._client = await WebSocketClient.connect(
            config=config,
            post_reconnection=self.reconnect,
        )
        self._is_connecting = False
        self._log.info(f"Connected to {self._url}", LogColor.BLUE)

    def _handle_ping(self, raw: bytes) -> None:
        self._loop.create_task(self.send_pong(raw))

    async def send_pong(self, raw: bytes) -> None:
        """
        Send the given raw payload to the server as a PONG message.
        """
        if self._client is None:
            return

        await self._client.send_pong(raw)

    # TODO: Temporarily sync
    def reconnect(self) -> None:
        """
        Reconnect the client to the server and resubscribe to all streams.
        """

        self._log.warning(f"Reconnected to {self._url}")

        # Re-subscribe to all streams
        self._loop.create_task(self._subscribe_all())

        if self._handler_reconnect:
            self._loop.create_task(self._handler_reconnect())  # type: ignore

    async def disconnect(self) -> None:
        """
        Disconnect the client from the server.
        """
        if self._client is None:
            self._log.warning("Cannot disconnect: not connected")
            return

        self._log.debug("Disconnecting...")
        await self._client.disconnect()
        self._client = None  # Dispose (will go out of scope)

        self._log.info(f"Disconnected from {self._url}", LogColor.BLUE)

    async def subscribe_listen_key(self, code_type: str, code: str) -> None:
        """
        Subscribe to user data stream.
        """
        await self._subscribe(code_type, code)

    async def unsubscribe_listen_key(self, code_type: str, code: str) -> None:
        """
        Unsubscribe from user data stream.
        """
        await self._unsubscribe(code_type, code)

    async def subscribe_trades(self, symbol: str) -> None:
        """
        Subscribe to trade stream.

        The Trade Streams push raw trade information; each trade has a unique buyer and seller.
        Stream Name: <symbol>@trade
        Update Speed: Real-time

        """
        await self._subscribe("trade", symbol)

    async def unsubscribe_trades(self, symbol: str) -> None:
        """
        Unsubscribe from trade stream.
        """
        await self._unsubscribe("trade", symbol)

    async def subscribe_ticker(self, symbol: str) -> None:
        """
        Subscribe to individual symbol or all symbols ticker stream.

        24hr rolling window ticker statistics for a single symbol.
        These are NOT the statistics of the UTC day, but a 24hr rolling window for the previous 24hrs.
        Stream Name: <symbol>@ticker or
        Stream Name: !ticker@arr
        Update Speed: 1000ms

        """
        await self._subscribe("ticker", symbol)

    async def unsubscribe_ticker(self, symbol: str) -> None:
        """
        Unsubscribe from individual symbol or all symbols ticker stream.
        """
        await self._unsubscribe("ticker", symbol)

    async def subscribe_orderbook(self, symbol: str) -> None:
        """
        Subscribe to individual symbol or all symbols ticker stream.

        24hr rolling window ticker statistics for a single symbol.
        These are NOT the statistics of the UTC day, but a 24hr rolling window for the previous 24hrs.
        Stream Name: <symbol>@ticker or
        Stream Name: !ticker@arr
        Update Speed: 1000ms

        """
        await self._subscribe("orderbook", symbol)

    async def unsubscribe_orderbook(self, symbol: str) -> None:
        """
        Unsubscribe from individual symbol or all symbols ticker stream.
        """
        await self._unsubscribe("orderbook", symbol)

    async def subscribe_orderbook_unit(self, symbol: str, unit: int) -> None:
        """
        Subscribe to individual symbol or all symbols ticker stream.

        24hr rolling window ticker statistics for a single symbol.
        These are NOT the statistics of the UTC day, but a 24hr rolling window for the previous 24hrs.
        Stream Name: <symbol>@ticker or
        Stream Name: !ticker@arr
        Update Speed: 1000ms

        """
        if not 1 <= unit <= 15:
            raise ValueError(f"`unit` must be between 1 and 15, was {unit}")
        await self.subscribe_orderbook(symbol + "." + str(unit))

    async def unsubscribe_orderbook_unit(self, symbol: str, unit: int) -> None:
        """
        Unsubscribe from individual symbol or all symbols ticker stream.
        """
        if not 1 <= unit <= 15:
            raise ValueError(f"`unit` must be between 1 and 15, was {unit}")
        await self.unsubscribe_orderbook(symbol + "." + str(unit))

    async def subscribe_orderbook_level(self, symbol: str, unit: int) -> None:
        """
        See https://docs.upbit.com/reference/websocket-orderbook#request

        """
        raise NotImplementedError

    async def unsubscribe_orderbook_level(self, symbol: str, unit: int) -> None:
        """
        Unsubscribe from individual symbol or all symbols ticker stream.
        """
        raise NotImplementedError

    async def _subscribe(self, code_type: str, code: str) -> None:
        if code_type not in ("ticker", "trade", "orderbook"):
            self._log.warning(f"Cannot subscribe to {code}@{code_type}: code_type is unknown")
            return  # Code type error

        if code in self._codes[code_type]:
            self._log.warning(f"Cannot subscribe to {code}@{code_type}: already subscribed")
            return  # Already subscribed

        self._codes[code_type].append(code)

        while self._is_connecting and not self._client:
            await asyncio.sleep(0.01)

        if self._client is None:
            # Make initial connection
            await self.connect()
            return

        await self._subscribe_all()

    async def _subscribe_all(self) -> None:
        if self._client is None:
            self._log.error("Cannot subscribe all: no connected")
            return

        message = self._create_subscribe_msg()
        self._log.debug(f"SENDING: {message}")

        await self._client.send_text(json.dumps(message))
        for code_type, codes in self._codes.items():
            for code in codes:
                self._log.debug(f"Subscribed to {code}@{code_type}")

    async def _unsubscribe(self, code_type: str, code: str) -> None:
        if code_type not in self._codes or code not in self._codes[code_type]:
            self._log.warning(f"Cannot unsubscribe from {code}@{code_type}: not subscribed")
            return  # Not subscribed

        self._codes[code_type].remove(code)

        if self._client is None:
            self._log.error(f"Cannot unsubscribe from {code}@{code_type}: not connected")
            return

        await self._subscribe_all()

    def _create_subscribe_msg(self) -> list[dict[str, Any]]:
        message = [
            {"ticket": self._ticket.value},
            # {"format": "SIMPLE"},  # TODO: 실제 받을땐 심플 포맷으로 변경
        ]
        if self._codes["ticker"]:
            message.append(
                {
                    "type": "ticker",
                    "codes": self._codes["ticker"],
                }
            )
        if self._codes["trade"]:
            message.append(
                {
                    "type": "trade",
                    "codes": self._codes["trade"],
                }
            )
        if self._codes["orderbook"]:
            message.append(
                {
                    "type": "orderbook",
                    "codes": self._codes["orderbook"],
                }
            )

        return message
