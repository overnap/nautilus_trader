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
import decimal
from decimal import Decimal

import msgspec
import pandas as pd

from nautilus_trader.adapters.binance.common.constants import BINANCE_VENUE
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser
from nautilus_trader.adapters.binance.common.enums import BinanceErrorCode
from nautilus_trader.adapters.binance.common.enums import BinanceKlineInterval
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitWebSocketMsg
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitWebSocketOrderbook
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitWebSocketTicker
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitWebSocketTrade
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol
from nautilus_trader.adapters.binance.common.types import BinanceBar
from nautilus_trader.adapters.binance.common.types import BinanceTicker
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.futures.types import BinanceFuturesMarkPriceUpdate
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.adapters.binance.http.error import BinanceError
from nautilus_trader.adapters.upbit.common.enums import UpbitCandleInterval
from nautilus_trader.adapters.upbit.common.types import UpbitBar
from nautilus_trader.adapters.upbit.http.client import UpbitHttpClient
from nautilus_trader.adapters.upbit.http.market import UpbitMarketHttpAPI
from nautilus_trader.adapters.upbit.websocket.client import UpbitWebSocketClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import secs_to_millis
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.aggregation import BarAggregator
from nautilus_trader.data.aggregation import TickBarAggregator
from nautilus_trader.data.aggregation import ValueBarAggregator
from nautilus_trader.data.aggregation import VolumeBarAggregator
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Quantity


class UpbitDataClient(LiveMarketDataClient):
    """
    Provides a data client of common methods for the `Binance` exchange.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : BinanceHttpClient
        The Binance HTTP client.
    market : BinanceMarketHttpAPI
        The Binance Market HTTP API.
    enum_parser : BinanceEnumParser
        The parser for Binance enums.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : InstrumentProvider
        The instrument provider.
    account_type : BinanceAccountType
        The account type for the client.
    base_url_ws : str
        The base url for the WebSocket client.
    name : str, optional
        The custom client ID.
    config : BinanceDataClientConfig
        The configuration for the client.

    Warnings
    --------
    This class should not be used directly, but through a concrete subclass.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: UpbitHttpClient,
        market: UpbitMarketHttpAPI,
        enum_parser: BinanceEnumParser,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: InstrumentProvider,
        account_type: BinanceAccountType,
        url_ws: str,
        name: str | None,
        config: BinanceDataClientConfig,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or BINANCE_VENUE.value),  # TODO: 수정
            venue=Venue(name or BINANCE_VENUE.value),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        # Configuration

        self._update_instrument_interval: int = 60 * 60  # Once per hour (hardcode)
        self._update_instruments_task: asyncio.Task | None = None

        self._connect_websockets_delay: float = 0.0  # Delay for bulk subscriptions to come in
        self._connect_websockets_task: asyncio.Task | None = None

        # HTTP API
        self._http_client = client
        self._http_market = market

        # Enum parser
        self._enum_parser = enum_parser

        # WebSocket API
        self._ws_client = UpbitWebSocketClient(
            clock=clock,
            handler=self._handle_ws_message,
            handler_reconnect=self._reconnect,
            url=url_ws,
            loop=self._loop,
        )

        # Hot caches
        self._instrument_ids: dict[str, InstrumentId] = {}
        self._book_depths: dict[InstrumentId, int | None] = {}
        self._book_buffer: dict[
            InstrumentId,
            list[OrderBookDelta | OrderBookDeltas],
        ] = {}

        self._log.info(f"Base url HTTP {self._http_client.base_url}", LogColor.BLUE)
        self._log.info(f"Base url WebSocket {url_ws}", LogColor.BLUE)

        # Register common WebSocket message handlers
        self._ws_handlers = {
            "ticker": self._handle_ticker,
            "trade": self._handle_trade,
            "orderbook": self._handle_orderbook,
            "order": self._handle_trade,
            "asset": self._handle_asset,
        }

        # WebSocket msgspec decoders
        self._decoder_type_msg = msgspec.json.Decoder(UpbitWebsocketMsg)
        self._decoder_order_book_msg = msgspec.json.Decoder(UpbitWebSocketOrderbook)
        self._decoder_ticker_msg = msgspec.json.Decoder(UpbitWebSocketTicker)
        self._decoder_trade_msg = msgspec.json.Decoder(UpbitWebSocketTrade)

        # Retry logic (hard coded for now)
        self._max_retries: int = 3
        self._retry_delay: float = 1.0
        self._retry_errors: set[BinanceErrorCode] = {
            BinanceErrorCode.DISCONNECTED,
            BinanceErrorCode.TOO_MANY_REQUESTS,  # Short retry delays may result in bans
            BinanceErrorCode.TIMEOUT,
            BinanceErrorCode.INVALID_TIMESTAMP,
            BinanceErrorCode.ME_RECVWINDOW_REJECT,
        }

    async def _connect(self) -> None:
        self._log.info("Initializing instruments...")

        await self._instrument_provider.initialize()

        self._send_all_instruments_to_data_engine()
        self._update_instruments_task = self.create_task(self._update_instruments())

    async def _update_instruments(self) -> None:
        while True:
            retries = 0
            while True:
                try:
                    self._log.debug(
                        f"Scheduled `update_instruments` to run in "
                        f"{self._update_instrument_interval}s",
                    )
                    await asyncio.sleep(self._update_instrument_interval)
                    await self._instrument_provider.load_all_async()
                    self._send_all_instruments_to_data_engine()
                    break
                except BinanceError as e:
                    error_code = BinanceErrorCode(e.message["code"])
                    retries += 1

                    if not self._should_retry(error_code, retries):
                        self._log.error(f"Error updating instruments: {e}")
                        break

                    self._log.warning(
                        f"{error_code.name}: retrying update instruments "
                        f"{retries}/{self._max_retries} in {self._retry_delay}s",
                    )
                    await asyncio.sleep(self._retry_delay)
                except asyncio.CancelledError:
                    self._log.debug("Canceled `update_instruments` task")
                    return

    async def _reconnect(self) -> None:
        coros = []
        for instrument_id in self._book_depths:
            coros.append(self._order_book_snapshot_then_deltas(instrument_id))

        await asyncio.gather(*coros)

    async def _disconnect(self) -> None:
        # Cancel update instruments task
        if self._update_instruments_task:
            self._log.debug("Canceling `update_instruments` task")
            self._update_instruments_task.cancel()
            self._update_instruments_task = None

        await self._ws_client.disconnect()

    def _should_retry(self, error_code: BinanceErrorCode, retries: int) -> bool:
        if (
            error_code not in self._retry_errors
            or not self._max_retries
            or retries > self._max_retries
        ):
            return False
        return True

    # -- SUBSCRIPTIONS ----------------------------------------------------------------------------

    async def _subscribe(self, data_type: DataType) -> None:
        instrument_id: InstrumentId | None = data_type.metadata.get("instrument_id")
        if instrument_id is None:
            self._log.error(
                f"Cannot subscribe to `{data_type.type}` no instrument ID in `data_type` metadata",
            )
            return

        if data_type.type == BinanceTicker:
            await self._ws_client.subscribe_ticker(instrument_id.symbol.value)
        elif data_type.type == BinanceFuturesMarkPriceUpdate:
            if not self._binance_account_type.is_futures:
                self._log.error(
                    f"Cannot subscribe to `BinanceFuturesMarkPriceUpdate` "
                    f"for {self._binance_account_type.value} account types",
                )
                return
            await self._ws_client.subscribe_mark_price(instrument_id.symbol.value, speed=1000)
        else:
            self._log.error(
                f"Cannot subscribe to {data_type.type} (not implemented)",
            )

    async def _unsubscribe(self, data_type: DataType) -> None:
        instrument_id: InstrumentId | None = data_type.metadata.get("instrument_id")
        if instrument_id is None:
            self._log.error(
                "Cannot subscribe to `BinanceFuturesMarkPriceUpdate` no instrument ID in `data_type` metadata",
            )
            return

        if data_type.type == BinanceTicker:
            await self._ws_client.unsubscribe_ticker(instrument_id.symbol.value)
        elif data_type.type == BinanceFuturesMarkPriceUpdate:
            if not self._binance_account_type.is_futures:
                self._log.error(
                    "Cannot unsubscribe from `BinanceFuturesMarkPriceUpdate` "
                    f"for {self._binance_account_type.value} account types",
                )
                return
        else:
            self._log.error(
                f"Cannot unsubscribe from {data_type.type} (not implemented)",
            )

    async def _subscribe_instruments(self) -> None:
        pass  # Do nothing further

    async def _subscribe_instrument(self, instrument_id: InstrumentId) -> None:
        pass  # Do nothing further

    async def _subscribe_order_book_deltas(
        self,
        instrument_id: InstrumentId,
        book_type: BookType,
        depth: int | None = None,
        kwargs: dict | None = None,
    ) -> None:
        update_speed = None
        if kwargs is not None:
            update_speed = kwargs.get("update_speed")
        await self._subscribe_order_book(
            instrument_id=instrument_id,
            book_type=book_type,
            update_speed=update_speed,
            depth=depth,
        )

    async def _subscribe_order_book_snapshots(
        self,
        instrument_id: InstrumentId,
        book_type: BookType,
        depth: int | None = None,
        kwargs: dict | None = None,
    ) -> None:
        update_speed = None
        if kwargs is not None:
            update_speed = kwargs.get("update_speed")
        await self._subscribe_order_book(
            instrument_id=instrument_id,
            book_type=book_type,
            update_speed=update_speed,
            depth=depth,
        )

    async def _subscribe_order_book(  # (too complex)
        self,
        instrument_id: InstrumentId,
        book_type: BookType,
        update_speed: int | None = None,
        depth: int | None = None,
    ) -> None:
        if book_type == BookType.L3_MBO:
            self._log.error(
                "Cannot subscribe to order book deltas: "
                "L3_MBO data is not published by Binance. "
                "Valid book types are L1_MBP, L2_MBP",
            )
            return

        valid_speeds = [100, 1000]
        if self._binance_account_type.is_futures:
            if update_speed is None:
                update_speed = 0  # Default 0 ms for futures
            valid_speeds = [0, 100, 250, 500]  # 0ms option for futures exists but not documented?
        elif update_speed is None:
            update_speed = 100  # Default 100ms for spot
        if update_speed not in valid_speeds:
            self._log.error(
                "Cannot subscribe to order book:"
                f"invalid `update_speed`, was {update_speed}. "
                f"Valid update speeds are {valid_speeds} ms",
            )
            return

        if not depth:
            # Reasonable default for full book, which works for Spot and Futures
            depth = 1000

        if 0 < depth <= 20:
            if depth not in (5, 10, 20):
                self._log.error(
                    "Cannot subscribe to order book snapshots: "
                    f"invalid `depth`, was {depth}. "
                    "Valid depths are 5, 10 or 20",
                )
                return
            await self._ws_client.subscribe_partial_book_depth(
                symbol=instrument_id.symbol.value,
                depth=depth,
                speed=update_speed,
            )
        else:
            await self._ws_client.subscribe_diff_book_depth(
                symbol=instrument_id.symbol.value,
                speed=update_speed,
            )

        self._book_depths[instrument_id] = depth

        await self._order_book_snapshot_then_deltas(instrument_id)

    async def _order_book_snapshot_then_deltas(self, instrument_id: InstrumentId) -> None:
        # Add delta feed buffer
        self._book_buffer[instrument_id] = []

        depth = self._book_depths[instrument_id]

        self._log.info(
            f"OrderBook snapshot rebuild for {instrument_id} @ depth {depth} starting",
            LogColor.BLUE,
        )

        snapshot: OrderBookDeltas = await self._http_market.request_order_book_snapshot(
            instrument_id=instrument_id,
            limit=depth,
            ts_init=self._clock.timestamp_ns(),
        )
        self._handle_data(snapshot)

        book_buffer = self._book_buffer.pop(instrument_id, [])
        for deltas in book_buffer:
            if snapshot and deltas.sequence <= snapshot.sequence:
                continue
            self._handle_data(deltas)

        self._log.info(
            f"OrderBook snapshot rebuild for {instrument_id} completed",
            LogColor.BLUE,
        )

    async def _subscribe_quote_ticks(self, instrument_id: InstrumentId) -> None:
        await self._ws_client.subscribe_book_ticker(instrument_id.symbol.value)

    async def _subscribe_trade_ticks(self, instrument_id: InstrumentId) -> None:
        if self._use_agg_trade_ticks:
            await self._ws_client.subscribe_agg_trades(instrument_id.symbol.value)
        else:
            await self._ws_client.subscribe_trades(instrument_id.symbol.value)

    async def _subscribe_bars(self, bar_type: BarType) -> None:
        PyCondition.true(bar_type.is_externally_aggregated(), "aggregation_source is not EXTERNAL")

        if not bar_type.spec.is_time_aggregated():
            self._log.error(
                f"Cannot subscribe to {bar_type}: only time bars are aggregated by Binance",
            )
            return

        resolution = self._enum_parser.parse_nautilus_bar_aggregation(bar_type.spec.aggregation)
        if self._binance_account_type.is_futures and resolution == "s":
            self._log.error(
                f"Cannot subscribe to {bar_type}. "
                "Second interval bars are not aggregated by Binance Futures",
            )
        try:
            interval = BinanceKlineInterval(f"{bar_type.spec.step}{resolution}")
        except ValueError:
            self._log.error(
                f"Bar interval {bar_type.spec.step}{resolution} not supported by Binance",
            )
            return

        await self._ws_client.subscribe_bars(
            symbol=bar_type.instrument_id.symbol.value,
            interval=interval.value,
        )

    async def _unsubscribe_instruments(self) -> None:
        pass  # Do nothing further

    async def _unsubscribe_instrument(self, instrument_id: InstrumentId) -> None:
        pass  # Do nothing further

    async def _unsubscribe_order_book_deltas(self, instrument_id: InstrumentId) -> None:
        pass  # TODO: Unsubscribe from Binance if no other subscriptions

    async def _unsubscribe_order_book_snapshots(self, instrument_id: InstrumentId) -> None:
        pass  # TODO: Unsubscribe from Binance if no other subscriptions

    async def _unsubscribe_quote_ticks(self, instrument_id: InstrumentId) -> None:
        await self._ws_client.unsubscribe_book_ticker(instrument_id.symbol.value)

    async def _unsubscribe_trade_ticks(self, instrument_id: InstrumentId) -> None:
        await self._ws_client.unsubscribe_trades(instrument_id.symbol.value)

    async def _unsubscribe_bars(self, bar_type: BarType) -> None:
        if not bar_type.spec.is_time_aggregated():
            self._log.error(
                f"Cannot unsubscribe from {bar_type}: only time bars are aggregated by Binance",
            )
            return

        resolution = self._enum_parser.parse_nautilus_bar_aggregation(bar_type.spec.aggregation)
        if self._binance_account_type.is_futures and resolution == "s":
            self._log.error(
                f"Cannot unsubscribe from {bar_type}. "
                "Second interval bars are not aggregated by Binance Futures",
            )
        try:
            interval = BinanceKlineInterval(f"{bar_type.spec.step}{resolution}")
        except ValueError:
            self._log.error(
                f"Bar interval {bar_type.spec.step}{resolution} not supported by Binance",
            )
            return

        await self._ws_client.unsubscribe_bars(
            symbol=bar_type.instrument_id.symbol.value,
            interval=interval.value,
        )

    # -- REQUESTS ---------------------------------------------------------------------------------

    async def _request_instrument(
        self,
        instrument_id: InstrumentId,
        correlation_id: UUID4,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> None:
        if start is not None:
            self._log.warning(
                f"Requesting instrument {instrument_id} with specified `start` which has no effect",
            )

        if end is not None:
            self._log.warning(
                f"Requesting instrument {instrument_id} with specified `end` which has no effect",
            )

        instrument: Instrument | None = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find instrument for {instrument_id}")
            return

        data_type = DataType(
            type=Instrument,
            metadata={"instrument_id": instrument_id},
        )

        self._handle_data_response(
            data_type=data_type,
            data=[instrument],  # Data engine handles lists of instruments
            correlation_id=correlation_id,
        )

    async def _request_quote_ticks(
        self,
        instrument_id: InstrumentId,
        limit: int,
        correlation_id: UUID4,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> None:
        self._log.error(
            "Cannot request historical quote ticks: not published by Binance",
        )

    async def _request_trade_ticks(
        self,
        instrument_id: InstrumentId,
        limit: int,
        correlation_id: UUID4,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> None:
        if limit == 0 or limit > 1000:
            limit = 1000

        if not self._use_agg_trade_ticks:
            if start is not None or end is not None:
                self._log.warning(
                    "Trade ticks have been requested with a from/to time range, "
                    f"however the request will be for the most recent {limit}. "
                    "Consider using aggregated trade ticks (`use_agg_trade_ticks`)",
                )
            ticks = await self._http_market.request_trade_ticks(
                instrument_id=instrument_id,
                limit=limit,
                ts_init=self._clock.timestamp_ns(),
            )
        else:
            # Convert from timestamps to milliseconds
            start_time_ms = None
            end_time_ms = None
            if start:
                start_time_ms = int(start.timestamp() * 1000)
            if end:
                end_time_ms = int(end.timestamp() * 1000)
            ticks = await self._http_market.request_agg_trade_ticks(
                instrument_id=instrument_id,
                limit=limit,
                start_time=start_time_ms,
                end_time=end_time_ms,
                ts_init=self._clock.timestamp_ns(),
            )

        self._handle_trade_ticks(instrument_id, ticks, correlation_id)

    async def _request_bars(  # (too complex)
        self,
        bar_type: BarType,
        count: int,
        correlation_id: UUID4,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> None:
        if bar_type.spec.price_type != PriceType.LAST:
            self._log.error(
                f"Cannot request {bar_type}: "
                f"only historical bars for LAST price type available from Binance",
            )
            return

        if start is None and end is None and count is None:
            count = 200

        start_time_ms = None
        if start is not None:
            start_time_ms = secs_to_millis(start.timestamp())

        end_time_ms = None
        if end is not None:
            end_time_ms = secs_to_millis(end.timestamp())

        if bar_type.is_externally_aggregated() or bar_type.spec.is_time_aggregated():
            if not bar_type.spec.is_time_aggregated():
                self._log.error(
                    f"Cannot request {bar_type}: only time bars are aggregated by Binance",
                )
                return

            resolution = self._enum_parser.parse_nautilus_bar_aggregation(bar_type.spec.aggregation)
            try:
                interval = UpbitCandleInterval(f"{bar_type.spec.step}{resolution}")
            except ValueError:
                self._log.error(
                    f"Cannot create Upbit Candle interval. {bar_type.spec.step}{resolution}"
                    "not supported",
                )
                return

            bars = await self._http_market.request_upbit_bars(
                bar_type=bar_type,
                interval=interval,
                start_time=start_time_ms,
                end_time=end_time_ms,
                count=count if count > 0 else None,
                ts_init=self._clock.timestamp_ns(),
            )

            if bar_type.is_internally_aggregated():
                self._log.info(
                    "Inferred INTERNAL time bars from EXTERNAL time bars",
                    LogColor.BLUE,
                )

        elif start and start < self._clock.utc_now() - pd.Timedelta(days=1):
            bars = await self._aggregate_internal_from_minute_bars(
                bar_type=bar_type,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                count=count if count > 0 else None,
            )
        else:
            self._log.error(f"Cannot infer INTERNAL time bars without `start_time` in Upbit")
            return

        partial: Bar = bars.pop()
        self._handle_bars(bar_type, bars, partial, correlation_id)

    async def _request_order_book_snapshot(
        self,
        instrument_id: InstrumentId,
        limit: int,
        correlation_id: UUID4,
    ) -> None:
        if limit not in [5, 10, 20, 50, 100, 500, 1000]:
            self._log.error(
                "Cannot get order book snapshots: "
                f"invalid `limit`, was {limit}. "
                "Valid limits are 5, 10, 20, 50, 100, 500 or 1000",
            )
            return
        else:
            snapshot: OrderBookDeltas = await self._http_market.request_order_book_snapshot(
                instrument_id=instrument_id,
                limit=limit,
                ts_init=self._clock.timestamp_ns(),
            )

            data_type = DataType(
                OrderBookDeltas,
                metadata={
                    "instrument_id": instrument_id,
                    "limit": limit,
                },
            )
            self._handle_data_response(
                data_type=data_type,
                data=snapshot,
                correlation_id=correlation_id,
            )

    async def _aggregate_internal_from_minute_bars(
        self,
        bar_type: BarType,
        start_time_ms: int | None,
        end_time_ms: int | None,
        count: int | None,
    ) -> list[Bar]:
        instrument = self._instrument_provider.find(bar_type.instrument_id)
        if instrument is None:
            self._log.error(
                f"Cannot aggregate internal bars: instrument {bar_type.instrument_id} not found",
            )
            return []

        self._log.info("Requesting 1-MINUTE Upbit bars to infer INTERNAL bars...", LogColor.BLUE)

        upbit_bars = await self._http_market.request_upbit_bars(
            bar_type=BarType(
                bar_type.instrument_id,
                BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                AggregationSource.EXTERNAL,
            ),
            interval=UpbitCandleInterval.MINUTE_1,
            start_time=start_time_ms,
            end_time=end_time_ms,
            ts_init=self._clock.timestamp_ns(),
            count=count,
        )

        quantize_value = Decimal(f"1e-{instrument.size_precision}")

        bars: list[Bar] = []
        if bar_type.spec.aggregation == BarAggregation.TICK:
            aggregator = TickBarAggregator(
                instrument=instrument,
                bar_type=bar_type,
                handler=bars.append,
            )
        elif bar_type.spec.aggregation == BarAggregation.VOLUME:
            aggregator = VolumeBarAggregator(
                instrument=instrument,
                bar_type=bar_type,
                handler=bars.append,
            )
        elif bar_type.spec.aggregation == BarAggregation.VALUE:
            aggregator = ValueBarAggregator(
                instrument=instrument,
                bar_type=bar_type,
                handler=bars.append,
            )
        else:
            raise RuntimeError(  # pragma: no cover (design-time error)
                f"Cannot start aggregator: "  # pragma: no cover (design-time error)
                f"BarAggregation.{bar_type.spec.aggregation_string_c()} "  # pragma: no cover (design-time error)
                f"not supported in open-source",  # pragma: no cover (design-time error)
            )

        for upbit_bar in upbit_bars:
            if upbit_bar.count == 0:
                continue
            self._aggregate_bar_to_trade_ticks(
                instrument=instrument,
                aggregator=aggregator,
                upbit_bar=upbit_bar,
                quantize_value=quantize_value,
            )

        self._log.info(
            f"Inferred {len(bars)} {bar_type} bars aggregated from {len(binance_bars)} 1-MINUTE Binance bars",
            LogColor.BLUE,
        )

        if count:
            bars = bars[:count]
        return bars

    def _aggregate_bar_to_trade_ticks(
        self,
        instrument: Instrument,
        aggregator: BarAggregator,
        upbit_bar: UpbitBar,
        quantize_value: Decimal,
    ) -> None:
        volume = upbit_bar.volume.as_decimal()
        size_part: Decimal = (volume / 4).quantize(
            quantize_value,
            rounding=decimal.ROUND_DOWN,
        )
        remainder: Decimal = volume - (size_part * 4)

        size = Quantity(size_part, instrument.size_precision)

        open = TradeTick(
            instrument_id=instrument.id,
            price=upbit_bar.open,
            size=size,
            aggressor_side=AggressorSide.NO_AGGRESSOR,
            trade_id=TradeId("NULL"),  # N/A
            ts_event=upbit_bar.ts_event,
            ts_init=upbit_bar.ts_event,
        )

        high = TradeTick(
            instrument_id=instrument.id,
            price=upbit_bar.high,
            size=size,
            aggressor_side=AggressorSide.NO_AGGRESSOR,
            trade_id=TradeId("NULL"),  # N/A
            ts_event=upbit_bar.ts_event,
            ts_init=upbit_bar.ts_event,
        )

        low = TradeTick(
            instrument_id=instrument.id,
            price=upbit_bar.low,
            size=size,
            aggressor_side=AggressorSide.NO_AGGRESSOR,
            trade_id=TradeId("NULL"),  # N/A
            ts_event=upbit_bar.ts_event,
            ts_init=upbit_bar.ts_event,
        )

        close = TradeTick(
            instrument_id=instrument.id,
            price=upbit_bar.close,
            size=Quantity(size_part + remainder, instrument.size_precision),
            aggressor_side=AggressorSide.NO_AGGRESSOR,
            trade_id=TradeId("NULL"),  # N/A
            ts_event=upbit_bar.ts_event,
            ts_init=upbit_bar.ts_event,
        )

        aggregator.handle_trade_tick(open)
        aggregator.handle_trade_tick(high)
        aggregator.handle_trade_tick(low)
        aggregator.handle_trade_tick(close)

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)

        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

    def _get_cached_instrument_id(self, symbol: str) -> InstrumentId:
        # Parse instrument ID
        binance_symbol = BinanceSymbol(symbol)
        nautilus_symbol: str = binance_symbol.parse_as_nautilus(
            self._binance_account_type,
        )
        instrument_id: InstrumentId | None = self._instrument_ids.get(nautilus_symbol)
        if not instrument_id:
            instrument_id = InstrumentId(Symbol(nautilus_symbol), self.venue)
            self._instrument_ids[nautilus_symbol] = instrument_id
        return instrument_id

    # -- WEBSOCKET HANDLERS ---------------------------------------------------------------------------------

    def _handle_ws_message(self, raw: bytes) -> None:
        # TODO: Uncomment for development
        # self._log.info(str(raw), LogColor.CYAN)
        wrapper = self._decoder_type_msg.decode(raw)
        if not wrapper.type:
            # Control message response
            return
        try:
            handled = False
            for handler in self._ws_handlers:
                if handler in wrapper.type:
                    self._ws_handlers[handler](raw)
                    handled = True
            if not handled:
                self._log.error(
                    f"Unrecognized websocket message type: {wrapper.stream}",
                )
        except Exception as e:
            self._log.error(f"Error handling websocket message, {e}")

    def _handle_ticker(self, raw: bytes) -> None:
        msg = self._decoder_ticker_msg.decode(raw)
        instrument_id: InstrumentId = self._get_cached_instrument_id(msg.data.s)
        ticker: BinanceTicker = msg.data.parse_to_binance_ticker(
            instrument_id=instrument_id,
            ts_init=self._clock.timestamp_ns(),
        )
        data_type = DataType(
            BinanceTicker,
            metadata={"instrument_id": instrument_id},
        )
        custom = CustomData(data_type=data_type, data=ticker)
        self._handle_data(custom)

    def _handle_trade(self, raw: bytes) -> None:
        msg = self._decoder_trade_msg.decode(raw)
        instrument_id: InstrumentId = self._get_cached_instrument_id(msg.code)
        trade_tick: TradeTick = msg.parse_to_trade_tick(
            instrument_id=instrument_id,
            ts_init=self._clock.timestamp_ns(),
        )
        self._handle_data(trade_tick)

    def _handle_book_partial_update(self, raw: bytes) -> None:
        msg = self._decoder_order_book_msg.decode(raw)
        instrument_id: InstrumentId = self._get_cached_instrument_id(msg.code)
        book_snapshot: OrderBookDeltas = msg.parse_to_order_book_snapshot(
            instrument_id=instrument_id,
            ts_init=self._clock.timestamp_ns(),
        )
        # Check if book buffer active
        book_buffer: list[OrderBookDelta | OrderBookDeltas] | None = self._book_buffer.get(
            instrument_id,
        )
        if book_buffer is not None:
            book_buffer.append(book_snapshot)
        else:
            self._handle_data(book_snapshot)
