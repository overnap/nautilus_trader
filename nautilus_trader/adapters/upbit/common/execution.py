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
from decimal import Decimal

import msgspec
import pandas as pd
from nautilus_trader.core.nautilus_pyo3 import millis_to_nanos
from nautilus_trader.core.nautilus_pyo3.model import LiquiditySide

from nautilus_trader.adapters.binance.common.constants import BINANCE_MAX_CALLBACK_RATE
from nautilus_trader.adapters.binance.common.constants import BINANCE_MIN_CALLBACK_RATE
from nautilus_trader.adapters.binance.common.constants import BINANCE_VENUE
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser
from nautilus_trader.adapters.binance.common.enums import BinanceErrorCode
from nautilus_trader.adapters.binance.common.enums import BinanceTimeInForce
from nautilus_trader.adapters.binance.common.schemas.account import BinanceOrder
from nautilus_trader.adapters.binance.common.schemas.account import BinanceUserTrade
from nautilus_trader.adapters.binance.common.schemas.user import BinanceListenKey
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol
from nautilus_trader.adapters.binance.config import BinanceExecClientConfig
from nautilus_trader.adapters.binance.http.account import BinanceAccountHttpAPI
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.adapters.binance.http.error import BinanceClientError
from nautilus_trader.adapters.binance.http.error import BinanceError
from nautilus_trader.adapters.binance.http.market import BinanceMarketHttpAPI
from nautilus_trader.adapters.binance.http.user import BinanceUserDataHttpAPI
from nautilus_trader.adapters.binance.websocket.client import BinanceWebSocketClient
from nautilus_trader.adapters.upbit.common.constants import UPBIT_VENUE
from nautilus_trader.adapters.upbit.common.credentials import get_api_key, get_api_secret
from nautilus_trader.adapters.upbit.common.enums import (
    UpbitEnumParser,
    UpbitTimeInForce,
    UpbitWebSocketType,
    UpbitOrderStatus,
)
from nautilus_trader.adapters.upbit.common.schemas.exchange import (
    UpbitOrder,
    UpbitWebSocketOrder,
    UpbitWebSocketAsset,
)
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitWebSocketMsg
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbol
from nautilus_trader.adapters.upbit.http.client import UpbitHttpClient
from nautilus_trader.adapters.upbit.http.market import UpbitMarketHttpAPI
from nautilus_trader.adapters.upbit.http.exchange import UpbitExchangeHttpAPI
from nautilus_trader.adapters.upbit.spot.providers import UpbitInstrumentProvider
from nautilus_trader.adapters.upbit.websocket.client import UpbitWebSocketClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.config import InstrumentProviderConfig
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import nanos_to_millis
from nautilus_trader.core.datetime import secs_to_millis
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.enums import trailing_offset_type_to_str
from nautilus_trader.model.enums import trigger_type_to_str
from nautilus_trader.model.identifiers import AccountId, TradeId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Price, Quantity

from nautilus_trader.model.functions import order_type_to_str, time_in_force_to_str
from nautilus_trader.model.orders import LimitOrder, MarketToLimitOrder
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.orders import Order
from nautilus_trader.model.orders import StopLimitOrder
from nautilus_trader.model.orders import StopMarketOrder
from nautilus_trader.model.orders import TrailingStopMarketOrder
from nautilus_trader.model.position import Position
from nautilus_trader.test_kit.mocks.cache_database import MockCacheDatabase
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


class UpbitExecutionClient(LiveExecutionClient):
    """
    Execution client providing common functionality for the `Binance` exchanges.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : BinanceHttpClient
        The binance HTTP client.
    account : BinanceAccountHttpAPI
        The binance Account HTTP API.
    market : BinanceMarketHttpAPI
        The binance Market HTTP API.
    user : BinanceUserHttpAPI
        The binance User HTTP API.
    enum_parser : BinanceEnumParser
        The parser for Binance enums.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : BinanceSpotInstrumentProvider
        The instrument provider.
    account_type : BinanceAccountType
        The account type for the client.
    base_url_ws : str
        The base URL for the WebSocket client.
    name : str, optional
        The custom client ID.
    config : BinanceExecClientConfig
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
        exchange: UpbitExchangeHttpAPI,
        enum_parser: UpbitEnumParser,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: InstrumentProvider,
        base_url_ws: str,
        name: str | None,
        config: BinanceExecClientConfig,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or BINANCE_VENUE.value),
            venue=Venue(name or BINANCE_VENUE.value),
            oms_type=OmsType.NETTING,
            instrument_provider=instrument_provider,
            account_type=AccountType.CASH,
            base_currency=None,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

        # Configuration
        self._use_gtd: bool = config.use_gtd
        self._use_reduce_only: bool = config.use_reduce_only
        self._use_position_ids: bool = config.use_position_ids
        self._max_retries: int = config.max_retries or 0
        self._retry_delay: float = config.retry_delay or 1.0
        self._log.info(f"{config.use_gtd=}", LogColor.BLUE)
        self._log.info(f"{config.use_reduce_only=}", LogColor.BLUE)
        self._log.info(f"{config.use_position_ids=}", LogColor.BLUE)
        self._log.info(f"{config.treat_expired_as_canceled=}", LogColor.BLUE)
        self._log.info(f"{config.max_retries=}", LogColor.BLUE)
        self._log.info(f"{config.retry_delay=}", LogColor.BLUE)

        self._set_account_id(
            AccountId(f"{name or UPBIT_VENUE.value}-master"),
        )

        # Enum parser
        self._enum_parser = enum_parser

        # Http API
        self._http_client = client
        self._http_market = market
        self._http_exchange = exchange

        # WebSocket API
        self._ws_client = UpbitWebSocketClient(
            clock=clock,
            handler=self._handle_ws_message,
            handler_reconnect=None,
            url=base_url_ws,
            loop=self._loop,
            header=[("authorization", client.get_auth_without_data())],
        )

        # Register spot websocket user data event handlers
        self._ws_handlers = {
            UpbitWebSocketType.ORDER: self._handle_asset_update,
            UpbitWebSocketType.ASSET: self._handle_order_update,
        }

        # Websocket schema decoders
        self._decoder_ws_message = msgspec.json.Decoder(UpbitWebSocketMsg)
        self._decoder_asset_update = msgspec.json.Decoder(UpbitWebSocketAsset)
        self._decoder_order_update = msgspec.json.Decoder(UpbitWebSocketOrder)

        # Order submission method hashmap
        self._submit_order_method = {
            OrderType.MARKET: self._submit_market_order,
            OrderType.LIMIT: self._submit_limit_order,
        }

        # Retry logic (hard coded for now)
        self._retry_errors: set[BinanceErrorCode] = {
            BinanceErrorCode.DISCONNECTED,
            BinanceErrorCode.TOO_MANY_REQUESTS,  # Short retry delays may result in bans
            BinanceErrorCode.TIMEOUT,
            BinanceErrorCode.SERVER_BUSY,
            BinanceErrorCode.INVALID_TIMESTAMP,
            BinanceErrorCode.CANCEL_REJECTED,
            BinanceErrorCode.ME_RECVWINDOW_REJECT,
        }

        # Hot caches
        self._instrument_ids: dict[str, InstrumentId] = {}
        self._generate_order_status_retries: dict[ClientOrderId, int] = {}
        self._modifying_orders: dict[ClientOrderId, VenueOrderId] = {}
        self._order_retries: dict[ClientOrderId, int] = {}

        self._log.info(f"Base url HTTP {self._http_client.base_url}", LogColor.BLUE)
        self._log.info(f"Base url WebSocket {base_url_ws}", LogColor.BLUE)

    @property
    def use_position_ids(self) -> bool:
        """
        Whether a `position_id` will be assigned to order events generated by the
        client.

        Returns
        -------
        bool

        """
        return self._use_position_ids

    async def _connect(self) -> None:
        try:
            # Initialize instrument provider
            await self._instrument_provider.initialize()

            # TODO: 주문 가능 정보 보면서 각 마켓에 대한 제한 저장
        except BinanceError as e:
            self._log.exception(f"Error on connect: {e.message}", e)
            return

        # Check Binance-Nautilus clock sync TODO: 이거 대체해서 구현할 수단 있을까?
        # server_time: int = await self._http_market.request_server_time()
        # self._log.info(f"Binance server time {server_time} UNIX (ms)")

        nautilus_time: int = self._clock.timestamp_ms()
        self._log.info(f"Nautilus clock time {nautilus_time} UNIX (ms)")

        # Connect WebSocket client
        await self._ws_client.subscribe_assets()
        await self._ws_client.subscribe_orders()

    async def _disconnect(self) -> None:
        await self._ws_client.disconnect()

    # -- EXECUTION REPORTS ------------------------------------------------------------------------

    async def generate_order_status_report(
        self,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId | None = None,
        venue_order_id: VenueOrderId | None = None,
    ) -> OrderStatusReport | None:
        PyCondition.false(
            client_order_id is None and venue_order_id is None,
            "both `client_order_id` and `venue_order_id` were `None`",
        )

        retries = self._generate_order_status_retries.get(client_order_id, 0)
        if retries > 3:
            self._log.error(
                f"Reached maximum retries 3/3 for generating OrderStatusReport for "
                f"{repr(client_order_id) if client_order_id else ''} "
                f"{repr(venue_order_id) if venue_order_id else ''}",
            )
            return None

        self._log.info(
            f"Generating OrderStatusReport for "
            f"{repr(client_order_id) if client_order_id else ''} "
            f"{repr(venue_order_id) if venue_order_id else ''}",
        )

        upbit_order: UpbitOrder | None = None
        try:
            if venue_order_id:
                upbit_order = await self._http_exchange.query_order(venue_order_id=venue_order_id)
            else:
                upbit_order = await self._http_exchange.query_order(client_order_id=client_order_id)
        except BinanceError as e:
            retries += 1
            self._log.error(
                f"Cannot generate order status report for {client_order_id!r}: {e.message}. Retry {retries}/3",
            )
            self._generate_order_status_retries[client_order_id] = retries
            if not client_order_id:
                self._log.warning("Cannot retry without a client order ID")
            else:
                order: Order | None = self._cache.order(client_order_id)
                if order is None:
                    self._log.warning("Order not found in cache")
                    return None
                elif order.is_closed:
                    return None  # Nothing else to do

                if retries >= 3:
                    # Order will no longer be considered in-flight once this event is applied.
                    # We could pop the value out of the hashmap here, but better to leave it in
                    # so that there are no longer subsequent retries (we don't expect many of these).
                    self.generate_order_rejected(
                        strategy_id=order.strategy_id,
                        instrument_id=instrument_id,
                        client_order_id=client_order_id,
                        reason=str(e.message),
                        ts_event=self._clock.timestamp_ns(),
                    )
            return None  # Error now handled

        if not upbit_order:
            # Cannot proceed to generating report
            self._log.error(
                f"Cannot generate `OrderStatusReport` for {client_order_id=!r}, {venue_order_id=!r}: "
                "order not found",
            )
            return None

        report: OrderStatusReport = upbit_order.parse_to_order_status_report(
            account_id=self.account_id,
            instrument_id=self._get_cached_instrument_id(upbit_order.market),
            report_id=UUID4(),
            enum_parser=self._enum_parser,
            ts_init=self._clock.timestamp_ns(),
            identifier=client_order_id,
        )

        self._log.debug(f"Received {report}")
        return report

    def _get_cache_active_symbols(self) -> set[str]:
        # Check cache for all active symbols
        open_orders: list[Order] = self._cache.orders_open(venue=self.venue)
        open_positions: list[Position] = self._cache.positions_open(venue=self.venue)
        active_symbols: set[str] = set()
        for o in open_orders:
            active_symbols.add(o.instrument_id.symbol.value)
        for p in open_positions:
            active_symbols.add(p.instrument_id.symbol.value)
        return active_symbols

    async def generate_order_status_reports(
        self,
        instrument_id: InstrumentId | None = None,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        open_only: bool = False,
    ) -> list[OrderStatusReport]:
        self._log.info("Requesting OrderStatusReports...")
        raise NotImplementedError  # TODO: HTTP API부터 짜야함
        # try:
        #     # Check Binance for all order active symbols
        #     symbol = instrument_id.symbol.value if instrument_id is not None else None
        #     active_symbols = self._get_cache_active_symbols()
        #     active_symbols.update(await self._get_binance_active_position_symbols(symbol))
        #     binance_open_orders = await self._http_account.query_open_orders(symbol)
        #     for order in binance_open_orders:
        #         active_symbols.add(order.symbol)
        #     # Get all orders for those active symbols
        #     binance_orders: list[BinanceOrder] = []
        #     for symbol in active_symbols:
        #         # Here we don't pass a `start_time` or `end_time` as order reports appear to go
        #         # randomly missing when these are specified. We filter on the Nautilus side below.
        #         # Explicitly setting limit to the max lookback of 1000, in the future we should
        #         # add pagination.
        #         response = await self._http_account.query_all_orders(symbol=symbol, limit=1_000)
        #         binance_orders.extend(response)
        # except BinanceError as e:
        #     self._log.exception(f"Cannot generate OrderStatusReport: {e.message}", e)
        #     return []
        #
        # start_ms = secs_to_millis(start.timestamp()) if start is not None else None
        # end_ms = secs_to_millis(end.timestamp()) if end is not None else None
        #
        # reports: list[OrderStatusReport] = []
        # for order in binance_orders:
        #     if start_ms is not None and order.time < start_ms:
        #         continue  # Filter start on the Nautilus side
        #     if end_ms is not None and order.time > end_ms:
        #         continue  # Filter end on the Nautilus side
        #     if order.origQty and Decimal(order.origQty) == 0:
        #         continue  # Cannot parse zero quantity order (filter for Binance)
        #     report = order.parse_to_order_status_report(
        #         account_id=self.account_id,
        #         instrument_id=self._get_cached_instrument_id(order.symbol),
        #         report_id=UUID4(),
        #         enum_parser=self._enum_parser,
        #         treat_expired_as_canceled=self._treat_expired_as_canceled,
        #         ts_init=self._clock.timestamp_ns(),
        #     )
        #     self._log.debug(f"Received {reports}")
        #     reports.append(report)
        #
        # len_reports = len(reports)
        # plural = "" if len_reports == 1 else "s"
        # self._log.info(f"Received {len(reports)} OrderStatusReport{plural}")
        #
        # return reports

    async def generate_fill_reports(
        self,
        instrument_id: InstrumentId | None = None,
        venue_order_id: VenueOrderId | None = None,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> list[FillReport]:
        self._log.info("Requesting FillReports...")
        raise NotImplementedError
        # TODO: order_id가 정해지는 경우 주문 쿼리에서 긁어오기
        # TODO: start/end만 주어지거나 필터가 아예 없으면 그냥 다봐야함

        # try:
        #     # Check Binance for all trades on active symbols
        #     symbol = instrument_id.symbol.value if instrument_id is not None else None
        #     active_symbols = self._get_cache_active_symbols()
        #     active_symbols.update(await self._get_binance_active_position_symbols(symbol))
        #     binance_trades: list[BinanceUserTrade] = []
        #     for symbol in active_symbols:
        #         response = await self._http_exchange.query_order(venue_order_id=venue_order_id)
        #         binance_trades.extend(response)
        # except BinanceError as e:
        #     self._log.exception(f"Cannot generate FillReport: {e.message}", e)
        #     return []
        #
        # # Parse all Binance trades
        # reports: list[FillReport] = []
        # for trade in binance_trades:
        #     if trade.symbol is None:
        #         self._log.warning(f"No symbol for trade {trade}")
        #         continue
        #     report = trade.parse_to_fill_report(
        #         account_id=self.account_id,
        #         instrument_id=self._get_cached_instrument_id(trade.symbol),
        #         report_id=UUID4(),
        #         ts_init=self._clock.timestamp_ns(),
        #         use_position_ids=self._use_position_ids,
        #     )
        #     self._log.debug(f"Received {report}")
        #     reports.append(report)
        #
        # # Confirm sorting in ascending order
        # reports = sorted(reports, key=lambda x: x.trade_id)
        #
        # len_reports = len(reports)
        # plural = "" if len_reports == 1 else "s"
        # self._log.info(f"Received {len(reports)} FillReport{plural}")
        #
        # return reports

    # -- COMMAND HANDLERS -------------------------------------------------------------------------

    def _check_order_validity(self, order: Order) -> None:
        # Check order type valid
        if order.order_type not in self._enum_parser.valid_order_types:
            self._log.error(
                f"Cannot submit order: {order_type_to_str(order.order_type)} "
                f"orders not supported by the Upbit. "
                f"Use any of {[order_type_to_str(t) for t in self._enum_parser.valid_order_types]}",
            )
            return
        # Check time in force valid
        if order.time_in_force not in self._enum_parser.valid_time_in_force:
            self._log.error(
                f"Cannot submit order: "
                f"{time_in_force_to_str(order.time_in_force)} "
                f"not supported by the Upbit. "
                f"Use any of {[time_in_force_to_str(t) for t in self._enum_parser.valid_time_in_force]}",
            )
            return
        # Check time in force with order type valid
        if order.time_in_force != TimeInForce.GTC and order.order_type == OrderType.MARKET:
            self._log.error(
                f"Cannot submit order: "
                f"{time_in_force_to_str(order.time_in_force)} "
                f"with {order_type_to_str(order.order_type)} orders not supported by the Upbit. "
                f"See https://docs.upbit.com/reference/%EC%A3%BC%EB%AC%B8%ED%95%98%EA%B8%B0",
            )
            return
        if order.time_in_force == TimeInForce.GTC and order.order_type == OrderType.MARKET_TO_LIMIT:
            self._log.error(
                f"Cannot submit order: "
                f"{time_in_force_to_str(order.time_in_force)} "
                f"with {order_type_to_str(order.order_type)} orders not supported by the Upbit. "
                f"See https://docs.upbit.com/reference/%EC%A3%BC%EB%AC%B8%ED%95%98%EA%B8%B0",
            )
            return
        if (
            order.order_type == OrderType.MARKET
            and order.side == OrderSide.BUY
            and not order.is_quote_quantity
        ):
            self._log.error(
                f"Cannot submit order: "
                f"{order_type_to_str(OrderType.MARKET)} BUYING orders that is not `is_quote_quantity` "
                f"not supported by the Upbit. "
                f"See https://docs.upbit.com/reference/%EC%A3%BC%EB%AC%B8%ED%95%98%EA%B8%B0",
            )
            return

    def _should_retry(self, error_code: BinanceErrorCode, retries: int) -> bool:
        if (
            error_code not in self._retry_errors
            or not self._max_retries
            or retries > self._max_retries
        ):
            return False
        return True

    def _determine_time_in_force(self, order: Order) -> UpbitTimeInForce:
        time_in_force: UpbitTimeInForce
        if order.time_in_force == TimeInForce.GTD:
            time_in_force = UpbitTimeInForce.GTC
            self._log.info(
                f"Converted GTD `time_in_force` to GTC for {order.client_order_id}",
                LogColor.BLUE,
            )
        else:
            time_in_force = self._enum_parser.parse_internal_time_in_force(order.time_in_force)
        return time_in_force

    async def _submit_order(self, command: SubmitOrder) -> None:
        await self._submit_order_inner(command.order)

    async def _submit_order_inner(self, order: Order) -> None:
        if order.is_closed:
            self._log.warning(f"Cannot submit already closed order {order}")
            return

        # Check validity
        self._check_order_validity(order)
        self._log.debug(f"Submitting {order}")

        # Generate event here to ensure correct ordering of events
        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        while True:
            try:
                await self._submit_order_method[order.order_type](order)
                self._order_retries.pop(order.client_order_id, None)
                break  # Successful request
            except KeyError:
                raise RuntimeError(f"unsupported order type, was {order.order_type}")
            except BinanceError as e:
                error_code = BinanceErrorCode(e.message["code"])

                retries = self._order_retries.get(order.client_order_id, 0) + 1
                self._order_retries[order.client_order_id] = retries

                if not self._should_retry(error_code, retries):
                    self.generate_order_rejected(
                        strategy_id=order.strategy_id,
                        instrument_id=order.instrument_id,
                        client_order_id=order.client_order_id,
                        reason=str(e.message),
                        ts_event=self._clock.timestamp_ns(),
                    )
                    break

                self._log.warning(
                    f"{error_code.name}: retrying {order.client_order_id!r} "
                    f"{retries}/{self._max_retries} in {self._retry_delay}s",
                )
                await asyncio.sleep(self._retry_delay)

    async def _submit_market_order(self, order: MarketOrder) -> None:
        await self._http_exchange.new_order(
            market=order.instrument_id.symbol,
            side=self._enum_parser.parse_internal_order_side(order.side),
            order_type=self._enum_parser.parse_internal_order_type(order.order_type, order.side),
            time_in_force=UpbitTimeInForce.GTC,
            volume=(order.quantity if order.side == OrderSide.SELL else None),
            price=(order.quantity if order.side == OrderSide.BUY else None),
            client_order_id=order.client_order_id,
        )

    async def _submit_limit_order(self, order: LimitOrder) -> None:
        await self._http_exchange.new_order(
            market=order.instrument_id.symbol,
            side=self._enum_parser.parse_internal_order_side(order.side),
            order_type=self._enum_parser.parse_internal_order_type(order.order_type),
            time_in_force=self._determine_time_in_force(order),
            volume=order.quantity,
            price=order.price,
            client_order_id=order.client_order_id,
        )

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        for order in command.order_list.orders:
            self.generate_order_submitted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                ts_event=self._clock.timestamp_ns(),
            )

        for order in command.order_list.orders:
            if order.linked_order_ids:  # TODO: Implement
                self._log.warning(f"Cannot yet handle OCO conditional orders, {order}")
            await self._submit_order_inner(order)

    def _get_cached_instrument_id(self, symbol: str) -> InstrumentId:
        # Parse instrument ID
        nautilus_symbol: str = UpbitSymbol(symbol).parse_as_nautilus()
        instrument_id: InstrumentId | None = self._instrument_ids.get(nautilus_symbol)
        if not instrument_id:
            instrument_id = InstrumentId(Symbol(nautilus_symbol), self.venue)
            self._instrument_ids[nautilus_symbol] = instrument_id
        return instrument_id

    async def _cancel_order(self, command: CancelOrder) -> None:
        while True:
            try:
                await self._cancel_order_single(
                    client_order_id=command.client_order_id,
                    venue_order_id=command.venue_order_id,
                )
                self._order_retries.pop(command.client_order_id, None)
                break  # Successful request
            except BinanceError as e:
                error_code = BinanceErrorCode(e.message["code"])

                retries = self._order_retries.get(command.client_order_id, 0) + 1
                self._order_retries[command.client_order_id] = retries

                if not self._should_retry(error_code, retries):
                    break

                self._log.warning(
                    f"{error_code.name}: retrying {command.client_order_id!r} "
                    f"{retries}/{self._max_retries} in {self._retry_delay}s",
                )
                await asyncio.sleep(self._retry_delay)

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        open_orders_strategy: list[Order] = self._cache.orders_open(
            instrument_id=command.instrument_id,
            strategy_id=command.strategy_id,
        )

        try:
            for order in open_orders_strategy:
                await self._cancel_order_single(
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                )
        except BinanceError as e:
            if "Unknown order sent" in e.message:
                self._log.info(
                    "No open orders to cancel according to Binance",
                    LogColor.GREEN,
                )
            else:
                self._log.exception(f"Cannot cancel open orders: {e.message}", e)

    async def _cancel_order_single(
        self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId | None,
    ) -> None:
        order: Order | None = self._cache.order(client_order_id)
        if order is None:
            self._log.error(f"{client_order_id!r} not found to cancel")
            return

        if order.is_closed:
            self._log.warning(
                f"CancelOrder command for {client_order_id!r} when order already {order.status_string()} "
                "(will not send to exchange)",
            )
            return

        try:
            await self._http_exchange.cancel_order(
                venue_order_id=venue_order_id if venue_order_id else None,
                client_order_id=client_order_id if client_order_id else None,
            )
        except BinanceError as e:
            error_code = BinanceErrorCode(e.message["code"])
            if error_code == BinanceErrorCode.CANCEL_REJECTED:
                self._log.warning(f"Cancel rejected: {e.message}")
            else:
                self._log.exception(
                    f"Cannot cancel order "
                    f"{client_order_id!r}, "
                    f"{venue_order_id!r}: "
                    f"{e.message}",
                    e,
                )

    # -- WEBSOCKET EVENT HANDLERS --------------------------------------------------------------------

    def _handle_ws_message(self, raw: bytes) -> None:
        # TODO: Uncomment for development
        # self._log.info(str(json.dumps(msgspec.json.decode(raw), indent=4)), color=LogColor.MAGENTA)
        msg = self._decoder_ws_message.decode(raw)
        try:
            self._ws_handlers[msg.type](raw)
        except Exception as e:
            self._log.exception(f"Error on handling {raw!r}", e)

    def _handle_order_update(self, raw: bytes) -> None:
        order_msg = self._decoder_order_update.decode(raw)

        venue_order_id = VenueOrderId(order_msg.uuid)
        client_order_id_str = self._cache.client_order_id(venue_order_id)
        if not client_order_id_str:
            raise AssertionError(
                "Client order id should be set, but not found by venue order id in cache."
            )
        client_order_id = ClientOrderId(client_order_id_str)
        ts_event = millis_to_nanos(order_msg.timestamp)
        instrument_id = self._get_cached_instrument_id(order_msg.code)
        strategy_id = self._cache.strategy_id_for_order(client_order_id)
        if strategy_id is None:
            report = order_msg.parse_to_order_status_report(
                account_id=self.account_id,
                instrument_id=instrument_id,
                report_id=UUID4(),
                enum_parser=self._enum_parser,
                ts_init=self._clock.timestamp_ns(),
                identifier=client_order_id.value,
            )
            self._send_order_status_report(report)
            strategy_id = self._cache.strategy_id_for_order(
                client_order_id
            )  # TODO: 이러면 잘 작동하나?

        if order_msg.state == UpbitOrderStatus.WAIT or order_msg.state == UpbitOrderStatus.WATCH:
            self.generate_order_accepted(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=millis_to_nanos(order_msg.order_timestamp),
            )
        elif order_msg.state == UpbitOrderStatus.TRADE:
            self.generate_order_filled(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,
                trade_id=TradeId(order_msg.trade_uuid),
                order_side=self._enum_parser.parse_upbit_order_side(order_msg.ask_bid),
                order_type=self._enum_parser.parse_upbit_order_type(order_msg.order_type),
                last_qty=Quantity(order_msg.volume),
                last_px=Price(order_msg.price),
                quote_currency=self._instrument_provider.find(instrument_id).quote_currency,
                commission=order_msg.calculate_commission(),
                liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                ts_event=millis_to_nanos(order_msg.trade_timestamp),
            )
        elif order_msg.state == UpbitOrderStatus.DONE:
            pass  # TODO: 따로 필요 없나?
        elif order_msg.state == UpbitOrderStatus.CANCEL:
            self.generate_order_canceled(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )

    def _handle_asset_update(self, raw: bytes) -> None:
        asset_msg = self._decoder_asset_update.decode(raw)
        self.generate_account_state(
            balances=[asset.parse_to_account_balance() for asset in asset_msg.assets],
            margins=[],
            reported=True,
            ts_event=millis_to_nanos(asset_msg.asset_timestamp),
        )


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    clock = LiveClock()
    trader_id = TestIdStubs.trader_id()

    msgbus = MessageBus(
        trader_id=trader_id,
        clock=clock,
    )

    cache_db = MockCacheDatabase()

    cache = Cache(
        database=cache_db,
    )

    http_client = UpbitHttpClient(
        clock=clock,
        key=get_api_key(),
        secret=get_api_secret(),
        base_url="https://api.upbit.com/",
    )

    client = UpbitExecutionClient(
        loop=loop,
        client=http_client,
        market=UpbitMarketHttpAPI(http_client),
        exchange=UpbitExchangeHttpAPI(http_client),
        enum_parser=UpbitEnumParser(),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=UpbitInstrumentProvider(
            http_client,
            clock,
            config=InstrumentProviderConfig(load_all=True),
        ),
        base_url_ws="wss://api.upbit.com/websocket/v1",
        name=None,
        config=BinanceExecClientConfig(),
    )

    print("Tasks created!")

    loop.run_forever()
