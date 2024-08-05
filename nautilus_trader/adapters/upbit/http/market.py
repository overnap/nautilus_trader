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

import sys
import time

import msgspec

from nautilus_trader.adapters.upbit.common.enums import UpbitCandleInterval
from nautilus_trader.adapters.upbit.common.enums import UpbitSecurityType
from nautilus_trader.adapters.upbit.common.enums import UpbitOrderbookLevel
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitOrderbook
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitCandle
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitTicker
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitTrade
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbol
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbols
from nautilus_trader.adapters.binance.common.types import BinanceBar
from nautilus_trader.adapters.upbit.http.client import UpbitHttpClient
from nautilus_trader.adapters.upbit.http.endpoint import UpbitHttpEndpoint
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.core.datetime import nanos_to_millis
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId


class UpbitTradesHttp(UpbitHttpEndpoint):
    """
    Endpoint of recent market trades.

    `GET /api/v3/trades`
    `GET /fapi/v1/trades`
    `GET /dapi/v1/trades`

    References
    ----------
    https://binance-docs.github.io/apidocs/spot/en/#recent-trades-list
    https://binance-docs.github.io/apidocs/futures/en/#recent-trades-list
    https://binance-docs.github.io/apidocs/delivery/en/#recent-trades-list

    """

    def __init__(
        self,
        client: UpbitHttpClient,
        base_endpoint: str,
    ):
        methods = {
            HttpMethod.GET: UpbitSecurityType.NONE,
        }
        url_path = base_endpoint + "trades/ticks"
        super().__init__(
            client,
            methods,
            url_path,
        )
        self._get_resp_decoder = msgspec.json.Decoder(list[UpbitTrade])

    class GetParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        """
        GET parameters for recent trades.

        Parameters
        ----------
        market: UpbitSymbol
        to: str | None = None
        count: int | None = None
        cursor: str | None = None
        daysAgo: int | None = None

        """

        market: UpbitSymbol
        to: str | None = None
        count: int | None = None
        cursor: str | None = None
        daysAgo: int | None = None

    async def get(self, params: GetParameters) -> list[UpbitTrade]:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, params)
        return self._get_resp_decoder.decode(raw)


class UpbitCandlesHttp(UpbitHttpEndpoint):
    """
    Endpoint of Kline/candlestick bars for a symbol.

    `GET /v1/candles/minutes`

    References
    ----------
    https://docs.upbit.com/reference/%EB%B6%84minute-%EC%BA%94%EB%93%A4-1

    """

    def __init__(self, client: UpbitHttpClient, base_endpoint: str):
        methods = {
            HttpMethod.GET: UpbitSecurityType.NONE,
        }
        url_path = base_endpoint + "candles"
        super().__init__(
            client,
            methods,
            url_path,
        )
        self._get_resp_decoder = msgspec.json.Decoder(list[UpbitCandle])

    class GetParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        """
        GET parameters for klines.

        Parameters
        ----------
        market: UpbitSymbol
        to: str | None = None
        count: int | None = None
        convertingPriceUnit: int | None = None  # Day candle only

        """

        market: UpbitSymbol
        to: str | None = None
        count: int | None = None
        convertingPriceUnit: int | None = None  # Day candle only

    async def get(self, params: GetParameters, interval: UpbitCandleInterval) -> list[UpbitCandle]:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, params, add_path=f"/{interval.value}")
        return self._get_resp_decoder.decode(raw)


class UpbitTickerHttp(UpbitHttpEndpoint):
    """
    Endpoint of latest price for a symbol or symbols.

    `GET /api/v3/ticker/price`
    `GET /fapi/v1/ticker/price`
    `GET /dapi/v1/ticker/price`

    References
    ----------
    https://binance-docs.github.io/apidocs/spot/en/#symbol-price-ticker
    https://binance-docs.github.io/apidocs/futures/en/#symbol-price-ticker
    https://binance-docs.github.io/apidocs/delivery/en/#symbol-price-ticker

    """

    def __init__(
        self,
        client: UpbitHttpClient,
        base_endpoint: str,
    ):
        methods = {
            HttpMethod.GET: UpbitSecurityType.NONE,
        }
        url_path = base_endpoint + "ticker"
        super().__init__(
            client,
            methods,
            url_path,
        )
        self._get_resp_decoder = msgspec.json.Decoder(list[UpbitTicker])

    class GetParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        """
        GET parameters for price ticker.

        Parameters
        ----------
        symbol : BinanceSymbol
            The trading pair. When given, endpoint will return a single BinanceTickerPrice.
            When omitted, endpoint will return a list of BinanceTickerPrice for all trading pairs.
        symbols : str
            SPOT/MARGIN only!
            List of trading pairs. When given, endpoint will return a list of BinanceTickerPrice.

        """

        markets: UpbitSymbols

    async def get(self, params: GetParameters) -> list[UpbitTicker]:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, params)
        return self._get_resp_decoder.decode(raw)


class UpbitOrderbookHttp(UpbitHttpEndpoint):
    """
    Endpoint of best price/qty on the order book for a symbol or symbols.

    `GET /api/v3/ticker/bookTicker`
    `GET /fapi/v1/ticker/bookTicker`
    `GET /dapi/v1/ticker/bookTicker`

    References
    ----------
    https://binance-docs.github.io/apidocs/spot/en/#symbol-order-book-ticker
    https://binance-docs.github.io/apidocs/futures/en/#symbol-order-book-ticker
    https://binance-docs.github.io/apidocs/delivery/en/#symbol-order-book-ticker

    """

    def __init__(
        self,
        client: UpbitHttpClient,
        base_endpoint: str,
    ):
        methods = {
            HttpMethod.GET: UpbitSecurityType.NONE,
        }
        url_path = base_endpoint + "orderbook"
        super().__init__(
            client,
            methods,
            url_path,
        )
        self._get_resp_decoder = msgspec.json.Decoder([UpbitOrderbook])

    class GetParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        """
        GET parameters for order book ticker.

        Parameters
        ----------
        markets: UpbitSymbols
        level: UpbitOrderbookLevel | None

        """

        markets: UpbitSymbols
        level: UpbitOrderbookLevel | None

    async def get(self, params: GetParameters) -> list[UpbitOrderbook]:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, params)
        return self._get_resp_decoder.decode(raw)


# TODO: 함수 입력에 UpbitSymbol을 두는게 맞는지? 안에서 형변환을 해줘야하는지?
# TODO: 지운 함수들이 중요한지?


class UpbitMarketHttpAPI:
    """
    Provides access to the Binance Market HTTP REST API.

    Parameters
    ----------
    client : BinanceHttpClient
        The Binance REST API client.
    account_type : BinanceAccountType
        The Binance account type, used to select the endpoint prefix.

    Warnings
    --------
    This class should not be used directly, but through a concrete subclass.

    """

    def __init__(self, client: UpbitHttpClient):
        PyCondition.not_none(client, "client")
        self.client = client

        self.base_endpoint = "/v1/"

        # Create Endpoints
        self._endpoint_trades = UpbitTradesHttp(client, self.base_endpoint)
        self._endpoint_candles = UpbitCandlesHttp(client, self.base_endpoint)
        self._endpoint_ticker = UpbitTickerHttp(client, self.base_endpoint)
        self._endpoint_orderbook = UpbitOrderbookHttp(client, self.base_endpoint)

    async def query_orderbook(
        self,
        symbols: list[str],
        level: UpbitOrderbookLevel | None = None,
    ) -> list[UpbitOrderbook]:
        """
        Query order book
        """
        return await self._endpoint_orderbook.get(
            params=self._endpoint_orderbook.GetParameters(
                markets=UpbitSymbols(symbols), level=level
            ),
        )

    async def request_order_book_snapshot(
        self,
        instrument_id: InstrumentId,
        ts_init: int,
        level: UpbitOrderbookLevel | None = None,
    ) -> OrderBookDeltas:
        """
        Request snapshot of order book depth.
        """

        # One `InstrumentId` guarantees one orderbook
        orderbook = (await self.query_orderbook([instrument_id.symbol.value], level))[0]
        return orderbook.parse_to_order_book_snapshot(
            instrument_id=instrument_id,
            ts_init=ts_init,
        )

    async def query_trades(
        self,
        symbol: str,
        to: str | None = None,
        count: int | None = None,
        cursor: str | None = None,
        daysAgo: int | None = None,
    ) -> list[UpbitTrade]:
        """
        Query trades for symbol.
        """
        return await self._endpoint_trades.get(
            params=self._endpoint_trades.GetParameters(
                market=UpbitSymbol(symbol),
                to=to,
                count=count,
                cursor=cursor,
                daysAgo=daysAgo,
            ),
        )

    async def request_trade_ticks(
        self,
        instrument_id: InstrumentId,
        ts_init: int,
        count: int | None = None,
    ) -> list[TradeTick]:
        """
        Request TradeTicks from Binance.
        """
        trades = await self.query_trades(
            symbol=instrument_id.symbol.value,
            count=count,
        )
        return [
            trade.parse_to_trade_tick(
                instrument_id=instrument_id,
                ts_init=ts_init,
            )
            for trade in trades
        ]

    async def request_historical_trade_ticks(
        self,
        instrument_id: InstrumentId,
        ts_init: int,
        count: int | None = None,
        to: str | None = None,
        cursor: str | None = None,
        daysAgo: int | None = None,
    ) -> list[TradeTick]:
        """
        Request historical TradeTicks from Binance.
        """
        historical_trades = await self.query_trades(
            symbol=instrument_id.symbol.value,
            to=to,
            count=count,
            cursor=cursor,
            daysAgo=daysAgo,
        )
        return [
            trade.parse_to_trade_tick(
                instrument_id=instrument_id,
                ts_init=ts_init,
            )
            for trade in historical_trades
        ]

    async def query_candles(
        self,
        symbol: str,
        interval: UpbitCandleInterval,
        to: str | None = None,
        count: int | None = None,
        converting_price_unit: int | None = None,
    ) -> list[UpbitCandle]:
        """
        Query klines for a symbol over an interval.
        """
        return await self._endpoint_candles.get(
            params=self._endpoint_candles.GetParameters(
                market=UpbitSymbol(symbol),
                to=to,
                count=count,
                convertingPriceUnit=converting_price_unit,
            ),
            interval=interval,
        )

    async def request_binance_bars(
        self,
        bar_type: BarType,
        ts_init: int,
        interval: UpbitCandleInterval,
        count: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[BinanceBar]:
        """
        Request Binance Bars from Klines.
        """
        end_time_ms = int(end_time) if end_time is not None else sys.maxsize
        all_bars: list[BinanceBar] = []
        while True:
            candles = await self.query_candles(
                symbol=bar_type.instrument_id.symbol.value,
                to=unix_nanos_to_dt(
                    millis_to_nanos(start_time)
                ).isoformat(),  # TODO: milli/nano 단위 체크!
                count=count,
            )
            bars: list[BinanceBar] = [
                candle.parse_to_binance_bar(bar_type, ts_init) for candle in candles
            ]
            all_bars.extend(bars)

            # Update the start_time to fetch the next set of bars
            if candles:
                next_start_time = candles[-1].timestamp + 1
            else:
                # Handle the case when klines is empty
                break

            # No more bars to fetch
            if (count and len(candles) < count) or next_start_time >= end_time_ms:
                break

            start_time = next_start_time

        return all_bars
