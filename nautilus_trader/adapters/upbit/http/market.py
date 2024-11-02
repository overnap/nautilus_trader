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
import sys
import time

import msgspec
from nautilus_trader.core.nautilus_pyo3.model import AggregationSource

from nautilus_trader.adapters.upbit.common.constants import UPBIT_MAX_CANDLE_COUNT
from nautilus_trader.adapters.upbit.common.credentials import get_api_key, get_api_secret
from nautilus_trader.adapters.upbit.common.enums import UpbitCandleInterval
from nautilus_trader.adapters.upbit.common.enums import UpbitSecurityType
from nautilus_trader.adapters.upbit.common.enums import UpbitOrderbookLevel
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitOrderbook, UpbitCodeInfo
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitCandle
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitTickerResponse
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitTrade
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbol
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbols
from nautilus_trader.adapters.binance.common.types import BinanceBar
from nautilus_trader.adapters.upbit.common.types import UpbitBar
from nautilus_trader.adapters.upbit.http.client import UpbitHttpClient
from nautilus_trader.adapters.upbit.http.endpoint import UpbitHttpEndpoint
from nautilus_trader.core.correctness import PyCondition

from nautilus_trader.common.component import LiveClock
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.core.datetime import nanos_to_millis
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.core.nautilus_pyo3 import HttpMethod, PriceType, unix_nanos_to_iso8601
from nautilus_trader.model.data import BarType, BarSpecification, BarAggregation
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue


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

        add_path = None
        match interval:
            case UpbitCandleInterval.SECOND_1:
                add_path = "seconds"
            case UpbitCandleInterval.MINUTE_1:
                add_path = "minutes/1"
            case UpbitCandleInterval.MINUTE_3:
                add_path = "minutes/3"
            case UpbitCandleInterval.MINUTE_5:
                add_path = "minutes/5"
            case UpbitCandleInterval.MINUTE_10:
                add_path = "minutes/10"
            case UpbitCandleInterval.MINUTE_15:
                add_path = "minutes/15"
            case UpbitCandleInterval.MINUTE_30:
                add_path = "minutes/30"
            case UpbitCandleInterval.MINUTE_60:
                add_path = "minutes/60"
            case UpbitCandleInterval.MINUTE_240:
                add_path = "minutes/240"
            case UpbitCandleInterval.DAY_1:
                add_path = "days"
            case UpbitCandleInterval.WEEK_1:
                add_path = "weeks"
            case UpbitCandleInterval.MONTH_1:
                add_path = "months"

        raw = await self._method(method_type, params, add_path=f"/{add_path}")
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
        self._get_resp_decoder = msgspec.json.Decoder(list[UpbitTickerResponse])

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

    async def get(self, params: GetParameters) -> list[UpbitTickerResponse]:
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
        self._get_resp_decoder = msgspec.json.Decoder(list[UpbitOrderbook])

    class GetParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        """
        GET parameters for order book ticker.

        Parameters
        ----------
        markets: UpbitSymbols
        level: UpbitOrderbookLevel

        """

        markets: UpbitSymbols
        level: UpbitOrderbookLevel

    async def get(self, params: GetParameters) -> list[UpbitOrderbook]:
        method_type = HttpMethod.GET

        raw = await self._method(method_type, params)
        return self._get_resp_decoder.decode(raw)


class UpbitCodeInfoHttp(UpbitHttpEndpoint):
    """
    Endpoint of SPOT/MARGIN exchange trading rules and symbol information.

    `GET /api/v3/exchangeInfo`

    References
    ----------
    https://binance-docs.github.io/apidocs/spot/en/#exchange-information

    """

    def __init__(
        self,
        client: UpbitHttpClient,
        base_endpoint: str,
    ):
        methods = {
            HttpMethod.GET: UpbitSecurityType.NONE,
        }
        url_path = base_endpoint + "market/all"
        super().__init__(
            client,
            methods,
            url_path,
        )
        self._get_resp_decoder = msgspec.json.Decoder(list[UpbitCodeInfo])

    async def get(self) -> list[UpbitCodeInfo]:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, params=None)
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
        self._endpoint_code_info = UpbitCodeInfoHttp(client, self.base_endpoint)

    async def query_ticker(
        self,
        symbols: list[str],
    ) -> list[UpbitTickerResponse]:
        """
        Query order book
        """
        return await self._endpoint_ticker.get(
            params=self._endpoint_ticker.GetParameters(markets=UpbitSymbols(symbols)),
        )

    async def query_orderbook(
        self,
        symbols: list[str],
        level: UpbitOrderbookLevel = 0,
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

    async def request_upbit_bars(
        self,
        bar_type: BarType,
        ts_init: int,
        interval: UpbitCandleInterval,
        limit: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[UpbitBar]:
        """
        Request Binance Bars from Klines.
        """
        start_time = int(start_time) if start_time is not None else sys.maxsize

        bars_list: list[list[UpbitBar]] = []
        while True:
            candles = await self.query_candles(
                symbol=bar_type.instrument_id.symbol.value,
                interval=interval,
                to=(
                    unix_nanos_to_iso8601(millis_to_nanos(end_time))
                    if end_time is not None
                    else None
                ),
                count=min(UPBIT_MAX_CANDLE_COUNT, limit) if limit else UPBIT_MAX_CANDLE_COUNT,
            )
            bars: list[UpbitBar] = [
                candle.parse_to_upbit_bar(bar_type, ts_init) for candle in reversed(candles)
            ]
            bars_list.append(bars)

            # Update the start_time to fetch the next set of bars
            if candles:
                next_end_time = candles[-1].timestamp - 1
            else:
                # Handle the case when klines is empty
                break

            # No more bars to fetch
            if (
                len(candles) < UPBIT_MAX_CANDLE_COUNT
                or (limit and len(bars_list) * UPBIT_MAX_CANDLE_COUNT >= limit)
                or next_end_time < start_time
            ):
                break

            end_time = next_end_time

        bars_list.reverse()
        all_bars: list[UpbitBar] = []
        for bars in bars_list:
            all_bars.extend(bars)

        if limit and len(all_bars) > limit:
            all_bars = all_bars[-limit:]
        if start_time:
            # TODO: 이분탐색으로 최적화 가능한데 200개짜리라 굳이? 싶긴하다. 여유나면 짜기
            for i, bar in enumerate(reversed(all_bars)):
                if bar.ts_event < millis_to_nanos(start_time):
                    all_bars = all_bars[-i:]
                    break

        return all_bars

    async def request_last_upbit_bar_for_subscribe(
        self,
        bar_type: BarType,
        ts_init: int,
        interval: UpbitCandleInterval,
    ) -> UpbitBar:
        """
        Request Binance Bars from Klines.
        """
        candles = await self.query_candles(
            symbol=bar_type.instrument_id.symbol.value,
            interval=interval,
            count=1,
        )
        bars: list[UpbitBar] = [
            candle.parse_to_upbit_bar(bar_type, ts_init) for candle in reversed(candles)
        ]
        assert len(bars) == 1

        return bars[0]

    async def query_code_info(self) -> list[UpbitCodeInfo]:
        """
        Check Binance Spot exchange information.
        """
        return await self._endpoint_code_info.get()


if __name__ == "__main__":
    clock = LiveClock()

    http_client = UpbitHttpClient(
        clock=clock,
        key=get_api_key(),
        secret=get_api_secret(),
        base_url="https://api.upbit.com/",
    )

    market_http = UpbitMarketHttpAPI(http_client)

    symbol = "KRW-BTC"

    candles = asyncio.run(market_http.query_candles(symbol, UpbitCandleInterval.MINUTE_30))
    print(candles)

    orderbook = asyncio.run(market_http.query_orderbook([symbol]))
    print(orderbook)

    trade = asyncio.run(market_http.query_trades(symbol))
    print(trade)

    bt = BarType(
        InstrumentId(Symbol(symbol), Venue("UPBIT")),
        BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
        AggregationSource.EXTERNAL,
    )
    start_time = clock.timestamp_ms() - 120000
    bars = asyncio.run(
        market_http.request_upbit_bars(
            bt,
            interval=UpbitCandleInterval.MINUTE_1,
            ts_init=clock.timestamp_ns(),
            start_time=start_time,
        )
    )
    assert nanos_to_millis(bars[0].ts_event) >= start_time
    assert nanos_to_millis(bars[0].ts_event) - 60000 < start_time
    for i in range(len(bars) - 1):
        assert bars[i].ts_event <= bars[i + 1].ts_event
    print("[candle] two minutes start time query:", len(bars))

    end_time = clock.timestamp_ms()
    bars = asyncio.run(
        market_http.request_upbit_bars(
            bt,
            interval=UpbitCandleInterval.MINUTE_1,
            ts_init=clock.timestamp_ns(),
            end_time=end_time,
        )
    )
    print("[candle] end time query:", len(bars))

    end_time = clock.timestamp_ms()
    bars = asyncio.run(
        market_http.request_upbit_bars(
            bt,
            interval=UpbitCandleInterval.MINUTE_1,
            ts_init=clock.timestamp_ns(),
            limit=5,
        )
    )
    print("[candle] five limit query:", len(bars))

    print("[ticker] ", len(asyncio.run(market_http.query_ticker(["KRW-BTC", "KRW-ETH"]))))
