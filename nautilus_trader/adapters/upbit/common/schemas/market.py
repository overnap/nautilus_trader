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

from decimal import Decimal

import msgspec

from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser
from nautilus_trader.adapters.binance.common.enums import BinanceExchangeFilterType
from nautilus_trader.adapters.binance.common.enums import BinanceKlineInterval
from nautilus_trader.adapters.binance.common.enums import BinanceRateLimitInterval
from nautilus_trader.adapters.binance.common.enums import BinanceRateLimitType
from nautilus_trader.adapters.binance.common.enums import BinanceSymbolFilterType
from nautilus_trader.adapters.binance.common.types import BinanceBar
from nautilus_trader.adapters.binance.common.types import BinanceTicker
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


################################################################################
# HTTP responses
################################################################################


class BinanceTime(msgspec.Struct, frozen=True):
    """
    Schema of current server time GET response of `time`
    """

    serverTime: int


class BinanceExchangeFilter(msgspec.Struct):
    """
    Schema of an exchange filter, within response of GET `exchangeInfo.`
    """

    filterType: BinanceExchangeFilterType
    maxNumOrders: int | None = None
    maxNumAlgoOrders: int | None = None


class BinanceRateLimit(msgspec.Struct):
    """
    Schema of rate limit info, within response of GET `exchangeInfo.`
    """

    rateLimitType: BinanceRateLimitType
    interval: BinanceRateLimitInterval
    intervalNum: int
    limit: int
    count: int | None = None  # SPOT/MARGIN rateLimit/order response only


class BinanceSymbolFilter(msgspec.Struct):
    """
    Schema of a symbol filter, within response of GET `exchangeInfo.`
    """

    filterType: BinanceSymbolFilterType
    minPrice: str | None = None
    maxPrice: str | None = None
    tickSize: str | None = None
    multiplierUp: str | None = None
    multiplierDown: str | None = None
    multiplierDecimal: str | None = None
    avgPriceMins: int | None = None
    minQty: str | None = None
    maxQty: str | None = None
    stepSize: str | None = None
    limit: int | None = None
    maxNumOrders: int | None = None

    notional: str | None = None  # SPOT/MARGIN & USD-M FUTURES only
    minNotional: str | None = None  # SPOT/MARGIN & USD-M FUTURES only
    maxNumAlgoOrders: int | None = None  # SPOT/MARGIN & USD-M FUTURES only

    bidMultiplierUp: str | None = None  # SPOT/MARGIN only
    bidMultiplierDown: str | None = None  # SPOT/MARGIN only
    askMultiplierUp: str | None = None  # SPOT/MARGIN only
    askMultiplierDown: str | None = None  # SPOT/MARGIN only
    applyMinToMarket: bool | None = None  # SPOT/MARGIN only
    maxNotional: str | None = None  # SPOT/MARGIN only
    applyMaxToMarket: bool | None = None  # SPOT/MARGIN only
    maxNumIcebergOrders: int | None = None  # SPOT/MARGIN only
    maxPosition: str | None = None  # SPOT/MARGIN only
    minTrailingAboveDelta: int | None = None  # SPOT/MARGIN only
    maxTrailingAboveDelta: int | None = None  # SPOT/MARGIN only
    minTrailingBelowDelta: int | None = None  # SPOT/MARGIN only
    maxTrailingBelowDelta: int | None = None  # SPOT/MARGIN only


class UpbitTrade(msgspec.Struct, frozen=True):
    """
    Schema of a single trade.
    """

    market: str
    trade_date_utc: str
    trade_time_utc: str
    timestamp: int
    trade_price: str
    trade_volume: str
    prev_closing_price: str
    change_price: str
    ask_bid: str
    sequential_id: int

    def parse_to_trade_tick(
            self,
            instrument_id: InstrumentId,
            ts_init: int,
    ) -> TradeTick:
        """
        Parse Binance trade to internal TradeTick.
        """
        # TODO: market이랑 instrument 비교?
        return TradeTick(
            instrument_id=instrument_id,
            price=Price.from_str(self.trade_price),
            size=Quantity.from_str(self.trade_volume),
            aggressor_side=AggressorSide.BUYER if self.ask_bid == "ASK" else AggressorSide.SELLER,
            trade_id=TradeId(str(self.sequential_id)),
            ts_event=millis_to_nanos(self.timestamp),
            ts_init=ts_init,
        )


class UpbitCandle(msgspec.Struct, frozen=True):
    """
    Schema of single Upbit kline.
    """

    market: str
    candle_date_time_utc: str
    candle_date_time_kst: str
    opening_price: str
    high_price: str
    low_price: str
    trade_price: str  # closing price
    timestamp: int
    candle_acc_trade_price: str
    candle_acc_trade_volume: str
    unit: int | None = None  # Minute candle only
    prev_closing_price: str | None = None  # Day candle only
    change_price: str | None = None  # Day candle only
    change_rate: str | None = None  # Day candle only
    converted_trade_price: str | None = (
        None  # Day candle only, request with `convertingPriceUnit` exclusively
    )
    first_day_of_period: str | None = None  # Week and Month candle only

    def parse_to_binance_bar(
            self,
            bar_type: BarType,
            ts_init: int,
    ) -> BinanceBar:
        """
        Parse kline to BinanceBar.
        """
        return BinanceBar(
            bar_type=bar_type,
            open=Price.from_str(self.opening_price),
            high=Price.from_str(self.high_price),
            low=Price.from_str(self.low_price),
            close=Price.from_str(self.trade_price),
            volume=Quantity.from_str(self.candle_acc_trade_volume),
            quote_volume=Decimal(self.candle_acc_trade_price),
            count=1,  # FIXME: 임시로 거래가 1번만 일어난 캔들로 취급! 수정 필요
            taker_buy_base_volume=Decimal(self.candle_acc_trade_volume),
            taker_buy_quote_volume=Decimal(
                self.candle_acc_trade_price
            ),  # FIXME: 임시로 taker만 있는 거래로 취급! 수정 필요
            ts_event=millis_to_nanos(
                self.candle_date_time_utc
            ),  # TODO: 봉 완성 시간인거 확인 했으나 한번더 확인 필요
            ts_init=ts_init,
        )  # TODO: BinanceBar가 별로 중요한 타입이 아니면 직접 만들기.


class UpbitTicker(msgspec.Struct, frozen=True):
    """
    Schema of single Upbit ticker.
    `trade_*` means most recent data.
    """

    market: str
    trade_date: str
    trade_time: str
    trade_date_kst: str
    trade_time_kst: str
    trade_timestamp: int
    opening_price: str
    high_price: str
    low_price: str
    trade_price: str  # closing price
    prev_closing_price: str  # Criteria for `*change*` fields
    change: str  # EVEN | RISE | FALL
    change_price: str
    change_rate: str
    signed_change_price: str
    signed_change_rate: str
    trade_volume: str
    acc_trade_price: str  # From UTC 0h
    acc_trade_price_24h: str
    acc_trade_volume: str  # From UTC 0h
    acc_trade_volume_24h: str
    highest_52_week_price: str
    highest_52_week_date: str
    lowest_52_week_price: str
    lowest_52_week_date: str
    timestamp: int


class UpbitOrderbookUnit(msgspec.Struct, frozen=True):
    """
    Schema for individual unit of Upbit orderbook. (HTTP)
    """

    ask_price: str
    bid_price: str
    ask_size: str
    bid_size: str


class UpbitOrderbook(msgspec.Struct, frozen=True):
    """
    Schema for Upbit orderbook. (HTTP)
    """

    market: str
    timestamp: int
    total_ask_size: str
    total_bid_size: str
    orderbook_units: list[UpbitOrderbookUnit]
    level: str

    def parse_to_order_book_snapshot(
            self,
            instrument_id: InstrumentId,
            ts_init: int,
    ) -> OrderBookDeltas:
        ts_event: int = millis_to_nanos(self.timestamp)
        bids: list[BookOrder] = [
            BookOrder(OrderSide.BUY, Price.from_str(o.bid_price), Quantity.from_str(o.bid_size), 0)
            for o in self.orderbook_units
        ]
        asks: list[BookOrder] = [
            BookOrder(OrderSide.SELL, Price.from_str(o.ask_price), Quantity.from_str(o.ask_size), 0)
            for o in self.orderbook_units
        ]

        deltas = [OrderBookDelta.clear(instrument_id, ts_init, ts_event)]
        deltas += [
            OrderBookDelta(instrument_id, BookAction.ADD, o, ts_event, ts_init) for o in bids + asks
        ]
        return OrderBookDeltas(instrument_id=instrument_id, deltas=deltas)


################################################################################
# WebSocket messages
################################################################################

# TODO: 축약형으로 쓰고싶은데 as (ask size) 예약어걸림. rename 어케쓰는건지 확인

class UpbitWebSocketMsg(msgspec.Struct):
    """
    Provides a wrapper for data WebSocket messages from `Binance`.
    """

    type: str


class UpbitWebSocketTicker(msgspec.Struct):
    """
    Provides a wrapper for data WebSocket messages from `Binance`.
    """

    type: str  # 타입 (ticker : 현재가)
    code: str  # 마켓 코드 (ex. KRW-BTC)
    opening_price: str  # 시가
    high_price: str  # 고가
    low_price: str  # 저가
    trade_price: str  # 현재가
    prev_closing_price: str  # 전일 종가
    change: str  # 전일 대비 (RISE : 상승, EVEN : 보합, FALL : 하락)
    change_price: str  # 부호 없는 전일 대비 값
    signed_change_price: str  # 전일 대비 값
    change_rate: str  # 부호 없는 전일 대비 등락율
    signed_change_rate: str  # 전일 대비 등락율
    trade_volume: str  # 가장 최근 거래량
    acc_trade_volume: str  # 누적 거래량 (UTC 0시 기준)
    acc_trade_volume_24h: str  # 24시간 누적 거래량
    acc_trade_price: str  # 누적 거래대금 (UTC 0시 기준)
    acc_trade_price_24h: str  # 24시간 누적 거래대금
    trade_date: str  # 최근 거래 일자 (UTC) (yyyyMMdd)
    trade_time: str  # 최근 거래 시각 (UTC) (HHmmss)
    trade_timestamp: int  # 체결 타임스탬프 (milliseconds)
    ask_bid: str  # 매수/매도 구분 (ASK : 매도, BID : 매수)
    acc_ask_volume: str  # 누적 매도량
    acc_bid_volume: str  # 누적 매수량
    highest_52_week_price: str  # 52주 최고가
    highest_52_week_date: str  # 52주 최고가 달성일 (yyyy-MM-dd)
    lowest_52_week_price: str  # 52주 최저가
    lowest_52_week_date: str  # 52주 최저가 달성일 (yyyy-MM-dd)
    market_state: str  # 거래상태 (PREVIEW : 입금지원, ACTIVE : 거래지원가능, DELISTED : 거래지원종료)
    delisting_date: str  # 거래지원 종료일
    market_warning: str  # 유의 종목 여부 (NONE : 해당없음, CAUTION : 투자유의)
    timestamp: int  # 타임스탬프 (millisecond)
    stream_type: str  # 스트림 타입 (SNAPSHOT : 스냅샷, REALTIME : 실시간)


class UpbitWebSocketTrade(msgspec.Struct):
    """
    Provides a wrapper for data WebSocket messages from `Binance`.
    """

    type: str  # 타입
    code: str  # 마켓 코드 (ex. KRW-BTC)
    trade_price: str  # 체결 가격
    trade_volume: str  # 체결량
    ask_bid: str  # 매수/매도 구분
    prev_closing_price: str  # 전일 종가
    change: str  # 전일 대비
    change_price: str  # 부호 없는 전일 대비 값
    trade_date: str  # 체결 일자 (UTC 기준)
    trade_time: str  # 체결 시각 (UTC 기준)
    trade_timestamp: int  # 체결 타임스탬프 (millisecond)
    timestamp: int  # 타임스탬프 (millisecond)
    sequential_id: int  # 체결 번호 (Unique)
    stream_type: str  # 스트림 타입


class UpbitWebSocketOrderbook(msgspec.Struct, frozen=True):
    """
    Schema for Upbit orderbook. (HTTP)
    """

    type: str  # 타입 (orderbook : 호가)
    code: str  # 마켓 코드 (ex. KRW-BTC)
    total_ask_size: str  # 호가 매도 총 잔량
    total_bid_size: str  # 호가 매수 총 잔량
    orderbook_units: list[UpbitOrderbookUnit]  # 호가 (List of Objects)
    timestamp: int  # 타임스탬프 (millisecond)
    level: str  # 호가 모아보기 단위 (default: 0)

    def parse_to_order_book_snapshot(
            self,
            instrument_id: InstrumentId,
            ts_init: int,
    ) -> OrderBookDeltas:
        ts_event: int = millis_to_nanos(self.timestamp)
        bids: list[BookOrder] = [
            BookOrder(OrderSide.BUY, Price.from_str(o.bid_price), Quantity.from_str(o.bid_size), 0)
            for o in self.orderbook_units
        ]
        asks: list[BookOrder] = [
            BookOrder(OrderSide.SELL, Price.from_str(o.ask_price), Quantity.from_str(o.ask_size), 0)
            for o in self.orderbook_units
        ]

        deltas = [OrderBookDelta.clear(instrument_id, ts_init, ts_event)]
        deltas += [
            OrderBookDelta(instrument_id, BookAction.ADD, o, ts_event, ts_init) for o in bids + asks
        ]
        return OrderBookDeltas(instrument_id=instrument_id, deltas=deltas)


class UpbitWebSocketOrder(msgspec.Struct):
    """
    Provides a wrapper for data WebSocket messages from `Binance`.
    """

    type: str  # 타입 (myOrder : 내 주문)
    code: str  # 마켓 코드 (ex. KRW-BTC)
    uuid: str  # 주문 고유 아이디
    ask_bid: str  # 매수/매도 구분 (ASK : 매도, BID : 매수)
    order_type: str  # 주문 타입 (limit: 지정가 주문, price: 시장가 매수, market: 시장가 매도, best: 최유리 지정가)
    state: str  # 주문 상태 (wait: 체결 대기, watch: 예약 대기, trade: 체결 발생, done: 완료, cancel: 취소)
    trade_uuid: str  # 체결의 고유 아이디
    price: str  # 주문 가격 또는 체결 가격 (state가 trade일 때)
    avg_price: str  # 평균 체결 가격
    volume: str  # 주문량 또는 체결량 (state가 trade일 때)
    remaining_volume: str  # 체결 후 남은 주문 양
    executed_volume: str  # 체결된 양
    trades_count: int  # 해당 주문에 걸린 체결 수
    reserved_fee: str  # 수수료로 예약된 비용
    remaining_fee: str  # 남은 수수료
    paid_fee: str  # 사용된 수수료
    locked: str  # 거래에 사용 중인 비용
    executed_funds: str  # 체결된 금액
    time_in_force: str  # IOC, FOK 설정 (ioc, fok)
    trade_timestamp: int  # 체결 타임스탬프 (millisecond)
    order_timestamp: int  # 주문 타임스탬프 (millisecond)
    timestamp: int  # 타임스탬프 (millisecond)
    stream_type: str  # 스트림 타입 (REALTIME : 실시간)


class UpbitWebSocketAssetUnit:
    currency: str  # 화폐를 의미하는 영문 대문자 코드
    balance: str  # 주문가능 수량
    locked: str  # 주문 중 묶여있는 수량


class UpbitWebSocketAsset(msgspec.Struct):
    """
    Provides a wrapper for data WebSocket messages from `Binance`.
    """

    type: str  # 타입 (myAsset : 내 자산)
    asset_uuid: str  # 자산 고유 아이디
    timestamp: int  # 타임스탬프 (millisecond)
    asset_timestamp: int  # 자산 타임스탬프 (millisecond)
    stream_type: str  # 스트림 타입 (REALTIME : 실시간)
    assets: list[UpbitWebSocketAssetUnit]


class BinanceOrderBookDelta(msgspec.Struct, array_like=True):
    """
    Schema of single ask/bid delta.
    """

    price: str
    size: str

    def parse_to_order_book_delta(
            self,
            instrument_id: InstrumentId,
            side: OrderSide,
            ts_event: int,
            ts_init: int,
            update_id: int,
    ) -> OrderBookDelta:
        size = Quantity.from_str(self.size)
        order = BookOrder(
            side=side,
            price=Price.from_str(self.price),
            size=Quantity.from_str(self.size),
            order_id=0,
        )

        return OrderBookDelta(
            instrument_id=instrument_id,
            action=BookAction.UPDATE if size > 0 else BookAction.DELETE,
            order=order,
            ts_event=ts_event,
            ts_init=ts_init,
            flags=0,
            sequence=update_id,
        )


class BinanceOrderBookData(msgspec.Struct, frozen=True):
    """
    WebSocket message 'inner struct' for `Binance` Partial & Diff.

    Book Depth Streams.

    """

    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    U: int  # First update ID in event
    u: int  # Final update ID in event
    b: list[BinanceOrderBookDelta]  # Bids to be updated
    a: list[BinanceOrderBookDelta]  # Asks to be updated

    T: int | None = None  # FUTURES only, transaction time
    pu: int | None = None  # FUTURES only, previous final update ID
    ps: str | None = None  # COIN-M FUTURES only, pair

    def parse_to_order_book_deltas(
            self,
            instrument_id: InstrumentId,
            ts_init: int,
    ) -> OrderBookDeltas:
        ts_event: int = millis_to_nanos(self.T) if self.T is not None else millis_to_nanos(self.E)

        bid_deltas: list[OrderBookDelta] = [
            delta.parse_to_order_book_delta(
                instrument_id,
                OrderSide.BUY,
                ts_event,
                ts_init,
                self.u,
            )
            for delta in self.b
        ]
        ask_deltas: list[OrderBookDelta] = [
            delta.parse_to_order_book_delta(
                instrument_id,
                OrderSide.SELL,
                ts_event,
                ts_init,
                self.u,
            )
            for delta in self.a
        ]

        return OrderBookDeltas(instrument_id=instrument_id, deltas=bid_deltas + ask_deltas)

    def parse_to_order_book_snapshot(
            self,
            instrument_id: InstrumentId,
            ts_init: int,
    ) -> OrderBookDeltas:
        ts_event: int = millis_to_nanos(self.T)
        bids: list[BookOrder] = [
            BookOrder(OrderSide.BUY, Price.from_str(o.price), Quantity.from_str(o.size), 0)
            for o in self.b
        ]
        asks: list[BookOrder] = [
            BookOrder(OrderSide.SELL, Price.from_str(o.price), Quantity.from_str(o.size), 0)
            for o in self.a
        ]

        deltas = [OrderBookDelta.clear(instrument_id, ts_init, ts_event)]
        deltas += [
            OrderBookDelta(instrument_id, BookAction.ADD, o, ts_event, ts_init) for o in bids + asks
        ]
        return OrderBookDeltas(instrument_id=instrument_id, deltas=deltas)


class BinanceOrderBookMsg(msgspec.Struct, frozen=True):
    """
    WebSocket message from `Binance` Partial & Diff.

    Book Depth Streams.

    """

    stream: str
    data: BinanceOrderBookData


class BinanceQuoteData(msgspec.Struct, frozen=True):
    """
    WebSocket message from `Binance` Individual Symbol Book Ticker Streams.
    """

    s: str  # symbol
    u: int  # order book updateId
    b: str  # best bid price
    B: str  # best bid qty
    a: str  # best ask price
    A: str  # best ask qty
    T: int | None = None  # event time

    def parse_to_quote_tick(
            self,
            instrument_id: InstrumentId,
            ts_init: int,
    ) -> QuoteTick:
        return QuoteTick(
            instrument_id=instrument_id,
            bid_price=Price.from_str(self.b),
            ask_price=Price.from_str(self.a),
            bid_size=Quantity.from_str(self.B),
            ask_size=Quantity.from_str(self.A),
            ts_event=millis_to_nanos(self.T) if self.T else ts_init,
            ts_init=ts_init,
        )


class BinanceQuoteMsg(msgspec.Struct, frozen=True):
    """
    WebSocket message from `Binance` Individual Symbol Book Ticker Streams.
    """

    stream: str
    data: BinanceQuoteData


class BinanceAggregatedTradeData(msgspec.Struct, frozen=True):
    """
    WebSocket message from `Binance` Aggregate Trade Streams.
    """

    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    a: int  # Aggregate trade ID
    p: str  # Price
    q: str  # Quantity
    f: int  # First trade ID
    l: int  # Last trade ID
    T: int  # Trade time
    m: bool  # Is the buyer the market maker?

    def parse_to_trade_tick(
            self,
            instrument_id: InstrumentId,
            ts_init: int,
    ) -> TradeTick:
        return TradeTick(
            instrument_id=instrument_id,
            price=Price.from_str(self.p),
            size=Quantity.from_str(self.q),
            aggressor_side=AggressorSide.SELLER if self.m else AggressorSide.BUYER,
            trade_id=TradeId(str(self.a)),
            ts_event=millis_to_nanos(self.T),
            ts_init=ts_init,
        )


class BinanceAggregatedTradeMsg(msgspec.Struct, frozen=True):
    """
    WebSocket message.
    """

    stream: str
    data: BinanceAggregatedTradeData


class BinanceTickerData(msgspec.Struct, kw_only=True, frozen=True):
    """
    WebSocket message from `Binance` 24hr Ticker.

    Fields
    ------
    - e: Event type
    - E: Event time
    - s: Symbol
    - p: Price change
    - P: Price change percent
    - w: Weighted average price
    - x: Previous close price
    - c: Last price
    - Q: Last quantity
    - b: Best bid price
    - B: Best bid quantity
    - a: Best ask price
    - A: Best ask quantity
    - o: Open price
    - h: High price
    - l: Low price
    - v: Total traded base asset volume
    - q: Total traded quote asset volume
    - O: Statistics open time
    - C: Statistics close time
    - F: First trade ID
    - L: Last trade ID
    - n: Total number of trades

    """

    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    p: str  # Price change
    P: str  # Price change percent
    w: str  # Weighted average price
    x: str | None = None  # First trade(F)-1 price (first trade before the 24hr rolling window)
    c: str  # Last price
    Q: str  # Last quantity
    b: str | None = None  # Best bid price
    B: str | None = None  # Best bid quantity
    a: str | None = None  # Best ask price
    A: str | None = None  # Best ask quantity
    o: str  # Open price
    h: str  # High price
    l: str  # Low price
    v: str  # Total traded base asset volume
    q: str  # Total traded quote asset volume
    O: int  # Statistics open time
    C: int  # Statistics close time
    F: int  # First trade ID
    L: int  # Last trade ID
    n: int  # Total number of trades

    def parse_to_binance_ticker(
            self,
            instrument_id: InstrumentId,
            ts_init: int,
    ) -> BinanceTicker:
        return BinanceTicker(
            instrument_id=instrument_id,
            price_change=Decimal(self.p),
            price_change_percent=Decimal(self.P),
            weighted_avg_price=Decimal(self.w),
            prev_close_price=Decimal(self.x) if self.x is not None else None,
            last_price=Decimal(self.c),
            last_qty=Decimal(self.Q),
            bid_price=Decimal(self.b) if self.b is not None else None,
            bid_qty=Decimal(self.B) if self.B is not None else None,
            ask_price=Decimal(self.a) if self.a is not None else None,
            ask_qty=Decimal(self.A) if self.A is not None else None,
            open_price=Decimal(self.o),
            high_price=Decimal(self.h),
            low_price=Decimal(self.l),
            volume=Decimal(self.v),
            quote_volume=Decimal(self.q),
            open_time_ms=self.O,
            close_time_ms=self.C,
            first_id=self.F,
            last_id=self.L,
            count=self.n,
            ts_event=millis_to_nanos(self.E),
            ts_init=ts_init,
        )


class BinanceTickerMsg(msgspec.Struct, frozen=True):
    """
    WebSocket message.
    """

    stream: str
    data: BinanceTickerData


class BinanceCandlestick(msgspec.Struct, frozen=True):
    """
    WebSocket message 'inner struct' for `Binance` Kline/Candlestick Streams.

    Fields
    ------
    - t: Kline start time
    - T: Kline close time
    - s: Symbol
    - i: Interval
    - f: First trade ID
    - L: Last trade ID
    - o: Open price
    - c: Close price
    - h: High price
    - l: Low price
    - v: Base asset volume
    - n: Number of trades
    - x: Is this kline closed?
    - q: Quote asset volume
    - V: Taker buy base asset volume
    - Q: Taker buy quote asset volume
    - B: Ignore

    """

    t: int  # Kline start time
    T: int  # Kline close time
    s: str  # Symbol
    i: BinanceKlineInterval  # Interval
    f: int  # First trade ID
    L: int  # Last trade ID
    o: str  # Open price
    c: str  # Close price
    h: str  # High price
    l: str  # Low price
    v: str  # Base asset volume
    n: int  # Number of trades
    x: bool  # Is this kline closed?
    q: str  # Quote asset volume
    V: str  # Taker buy base asset volume
    Q: str  # Taker buy quote asset volume
    B: str  # Ignore

    def parse_to_binance_bar(
            self,
            instrument_id: InstrumentId,
            enum_parser: BinanceEnumParser,
            ts_init: int,
    ) -> BinanceBar:
        bar_type = BarType(
            instrument_id=instrument_id,
            bar_spec=enum_parser.parse_binance_kline_interval_to_bar_spec(self.i),
            aggregation_source=AggregationSource.EXTERNAL,
        )
        return BinanceBar(
            bar_type=bar_type,
            open=Price.from_str(self.o),
            high=Price.from_str(self.h),
            low=Price.from_str(self.l),
            close=Price.from_str(self.c),
            volume=Quantity.from_str(self.v),
            quote_volume=Decimal(self.q),
            count=self.n,
            taker_buy_base_volume=Decimal(self.V),
            taker_buy_quote_volume=Decimal(self.Q),
            ts_event=millis_to_nanos(self.T),
            ts_init=ts_init,
        )


class BinanceCandlestickData(msgspec.Struct, frozen=True):
    """
    WebSocket message 'inner struct'.
    """

    e: str
    E: int
    s: str
    k: BinanceCandlestick


class BinanceCandlestickMsg(msgspec.Struct, frozen=True):
    """
    WebSocket message for `Binance` Kline/Candlestick Streams.
    """

    stream: str
    data: BinanceCandlestickData
