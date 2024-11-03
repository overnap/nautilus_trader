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
import pandas as pd
from attr.setters import frozen
from nautilus_trader.core.nautilus_pyo3.model import CurrencyType
from nautilus_trader.core.rust.model import RecordFlag

from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser
from nautilus_trader.adapters.binance.common.enums import BinanceExchangeFilterType
from nautilus_trader.adapters.binance.common.enums import BinanceKlineInterval
from nautilus_trader.adapters.binance.common.enums import BinanceRateLimitInterval
from nautilus_trader.adapters.binance.common.enums import BinanceRateLimitType
from nautilus_trader.adapters.binance.common.enums import BinanceSymbolFilterType
from nautilus_trader.adapters.binance.common.types import BinanceBar
from nautilus_trader.adapters.binance.common.types import BinanceTicker
from nautilus_trader.adapters.upbit.common.enums import UpbitOrderSideHttp, UpbitOrderSideWebSocket
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbol
from nautilus_trader.adapters.upbit.common.types import UpbitBar, UpbitTicker
from nautilus_trader.core.datetime import millis_to_nanos, dt_to_unix_nanos
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick

from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Currency
from nautilus_trader.model.objects import Quantity


################################################################################
# HTTP responses
################################################################################


class BinanceRateLimit(msgspec.Struct):
    """
    Schema of rate limit info, within response of GET `exchangeInfo.`
    """

    rateLimitType: BinanceRateLimitType
    interval: BinanceRateLimitInterval
    intervalNum: int
    limit: int
    count: int | None = None  # SPOT/MARGIN rateLimit/order response only


class UpbitTrade(msgspec.Struct, frozen=True):
    """
    Schema of a single trade.
    """

    market: str
    trade_date_utc: str
    trade_time_utc: str
    timestamp: int
    trade_price: float
    trade_volume: float
    prev_closing_price: float
    change_price: float
    ask_bid: UpbitOrderSideHttp
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
            price=Price.from_str(str(self.trade_price)),
            size=Quantity.from_str(str(self.trade_volume)),
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
    opening_price: float
    high_price: float
    low_price: float
    trade_price: float  # closing price
    timestamp: int
    candle_acc_trade_price: float
    candle_acc_trade_volume: float
    unit: int | None = None  # Minute candle only
    prev_closing_price: float | None = None  # Day candle only
    change_price: float | None = None  # Day candle only
    change_rate: str | None = None  # Day candle only
    converted_trade_price: float | None = (
        None  # Day candle only, request with `convertingPriceUnit` exclusively
    )
    first_day_of_period: str | None = None  # Week and Month candle only

    def parse_to_upbit_bar(
        self,
        bar_type: BarType,
        ts_init: int,
    ) -> UpbitBar:
        """
        Parse kline to BinanceBar.
        """
        return UpbitBar(
            bar_type=bar_type,
            open=Price.from_str(str(self.opening_price)),
            high=Price.from_str(str(self.high_price)),
            low=Price.from_str(str(self.low_price)),
            close=Price.from_str(str(self.trade_price)),
            volume=Quantity.from_str(str(self.candle_acc_trade_volume)),
            quote_volume=Decimal(str(self.candle_acc_trade_price)),
            ts_event=dt_to_unix_nanos(pd.to_datetime(self.candle_date_time_utc, format="ISO8601")),
            ts_init=ts_init,
        )


class UpbitTickerResponse(msgspec.Struct, frozen=True):
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
    opening_price: float
    high_price: float
    low_price: float
    trade_price: float  # closing price
    prev_closing_price: float  # Criteria for `*change*` fields
    change: str  # EVEN | RISE | FALL
    change_price: float
    change_rate: float
    signed_change_price: float
    signed_change_rate: float
    trade_volume: float
    acc_trade_price: float  # From UTC 0h
    acc_trade_price_24h: float
    acc_trade_volume: float  # From UTC 0h
    acc_trade_volume_24h: float
    highest_52_week_price: float
    highest_52_week_date: str
    lowest_52_week_price: float
    lowest_52_week_date: str
    timestamp: int


class UpbitOrderbookUnit(msgspec.Struct, frozen=True):
    """
    Schema for individual unit of Upbit orderbook. (HTTP)
    """

    ask_price: float
    bid_price: float
    ask_size: float
    bid_size: float


class UpbitOrderbook(msgspec.Struct, frozen=True):
    """
    Schema for Upbit orderbook. (HTTP)
    """

    market: str
    timestamp: int
    total_ask_size: float
    total_bid_size: float
    orderbook_units: list[UpbitOrderbookUnit]
    level: int

    def parse_to_order_book_snapshot(
        self,
        instrument_id: InstrumentId,
        ts_init: int,
    ) -> OrderBookDeltas:
        ts_event: int = millis_to_nanos(self.timestamp)
        bids: list[BookOrder] = [
            BookOrder(
                OrderSide.BUY,
                Price.from_str(str(o.bid_price)),
                Quantity.from_str(str(o.bid_size)),
                0,
            )
            for o in self.orderbook_units
        ]
        asks: list[BookOrder] = [
            BookOrder(
                OrderSide.SELL,
                Price.from_str(str(o.ask_price)),
                Quantity.from_str(str(o.ask_size)),
                0,
            )
            for o in self.orderbook_units
        ]

        deltas = [
            OrderBookDelta.clear(
                instrument_id=instrument_id,
                sequence=0,
                ts_init=ts_init,
                ts_event=ts_event,
            )
        ]
        for idx, bid in enumerate(bids):
            deltas.append(
                OrderBookDelta(
                    instrument_id=instrument_id,
                    action=BookAction.ADD,
                    order=bid,
                    flags=0 if idx < len(bids) else RecordFlag.F_LAST,
                    sequence=0,
                    ts_event=ts_event,
                    ts_init=ts_init,
                )
            )
        for idx, ask in enumerate(asks):
            deltas.append(
                OrderBookDelta(
                    instrument_id=instrument_id,
                    action=BookAction.ADD,
                    order=ask,
                    flags=0 if idx < len(asks) else RecordFlag.F_LAST,
                    sequence=0,
                    ts_event=ts_event,
                    ts_init=ts_init,
                )
            )
        return OrderBookDeltas(instrument_id=instrument_id, deltas=deltas)


class UpbitCodeInfo(msgspec.Struct, frozen=True):

    market: str
    korean_name: str
    english_name: str

    # def parse_to_base_asset(self):
    #     return Currency(
    #         code=self.baseAsset,
    #         precision=self.baseAssetPrecision,
    #         iso4217=0,  # Currently unspecified for crypto assets
    #         name=self.baseAsset,
    #         currency_type=CurrencyType.CRYPTO,
    #     )

    # def parse_to_quote_asset(self):
    #     return Currency(
    #         code=self.quoteAsset,
    #         precision=self.quoteAssetPrecision,
    #         iso4217=0,  # Currently unspecified for crypto assets
    #         name=self.quoteAsset,
    #         currency_type=CurrencyType.CRYPTO,
    #     )


################################################################################
# WebSocket messages
################################################################################

# TODO: 축약형으로 쓰고싶은데 as (ask size) 예약어걸림. rename 어케쓰는건지 확인


class UpbitWebSocketMsg(msgspec.Struct, frozen=True):
    type: str


class UpbitWebSocketTicker(msgspec.Struct):
    """
    Provides a wrapper for data WebSocket messages from `Binance`.
    """

    type: str  # 타입 (ticker : 현재가)
    code: str  # 마켓 코드 (ex. KRW-BTC)
    opening_price: float  # 시가
    high_price: float  # 고가
    low_price: float  # 저가
    trade_price: float  # 현재가
    prev_closing_price: float  # 전일 종가
    change: str  # 전일 대비 (RISE : 상승, EVEN : 보합, FALL : 하락)
    change_price: float  # 부호 없는 전일 대비 값
    signed_change_price: float  # 전일 대비 값
    change_rate: str  # 부호 없는 전일 대비 등락율
    signed_change_rate: str  # 전일 대비 등락율
    trade_volume: float  # 가장 최근 거래량
    acc_trade_volume: float  # 누적 거래량 (UTC 0시 기준)
    acc_trade_volume_24h: str  # 24시간 누적 거래량
    acc_trade_price: float  # 누적 거래대금 (UTC 0시 기준)
    acc_trade_price_24h: str  # 24시간 누적 거래대금
    trade_date: str  # 최근 거래 일자 (UTC) (yyyyMMdd)
    trade_time: str  # 최근 거래 시각 (UTC) (HHmmss)
    trade_timestamp: int  # 체결 타임스탬프 (milliseconds)
    ask_bid: UpbitOrderSideWebSocket  # 매수/매도 구분 (ASK : 매도, BID : 매수)
    acc_ask_volume: float  # 누적 매도량
    acc_bid_volume: float  # 누적 매수량
    highest_52_week_price: float  # 52주 최고가
    highest_52_week_date: str  # 52주 최고가 달성일 (yyyy-MM-dd)
    lowest_52_week_price: float  # 52주 최저가
    lowest_52_week_date: str  # 52주 최저가 달성일 (yyyy-MM-dd)
    market_state: (
        str  # 거래상태 (PREVIEW : 입금지원, ACTIVE : 거래지원가능, DELISTED : 거래지원종료)
    )
    delisting_date: str  # 거래지원 종료일
    market_warning: str  # 유의 종목 여부 (NONE : 해당없음, CAUTION : 투자유의)
    timestamp: int  # 타임스탬프 (millisecond)
    stream_type: str  # 스트림 타입 (SNAPSHOT : 스냅샷, REALTIME : 실시간)

    def parse_to_upbit_ticker(
        self,
        instrument_id: InstrumentId,
        ts_init: int,
    ) -> UpbitTicker:
        return UpbitTicker(
            instrument_id=instrument_id,
            price_change=Decimal(self.signed_change_price),
            price_change_percent=Decimal(self.signed_change_rate),
            prev_close_price=Decimal(self.prev_closing_price),
            last_price=Decimal(self.trade_price),
            last_qty=Decimal(self.trade_volume),
            open_price=Decimal(self.opening_price),
            high_price=Decimal(self.high_price),
            low_price=Decimal(self.low_price),
            volume=Decimal(self.acc_trade_volume),
            quote_volume=Decimal(self.acc_trade_price),
            volume_24h=Decimal(self.acc_trade_volume_24h),
            quote_volume_24h=Decimal(self.acc_trade_price_24h),
            ts_event=millis_to_nanos(self.timestamp),
            ts_init=ts_init,
        )


class UpbitWebSocketTrade(msgspec.Struct):
    """
    Provides a wrapper for data WebSocket messages from `Binance`.
    """

    type: str  # 타입
    code: str  # 마켓 코드 (ex. KRW-BTC)
    trade_price: float  # 체결 가격
    trade_volume: float  # 체결량
    ask_bid: UpbitOrderSideWebSocket  # 매수/매도 구분
    prev_closing_price: float  # 전일 종가
    change: str  # 전일 대비
    change_price: float  # 부호 없는 전일 대비 값
    trade_date: str  # 체결 일자 (UTC 기준)
    trade_time: str  # 체결 시각 (UTC 기준)
    trade_timestamp: int  # 체결 타임스탬프 (millisecond)
    timestamp: int  # 타임스탬프 (millisecond)
    sequential_id: int  # 체결 번호 (Unique)
    stream_type: str  # 스트림 타입

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
            price=Price.from_str(str(self.trade_price)),
            size=Quantity.from_str(str(self.trade_volume)),
            aggressor_side=(
                AggressorSide.BUYER
                if self.ask_bid == UpbitOrderSideWebSocket.BID
                else AggressorSide.SELLER
            ),
            trade_id=TradeId(str(self.sequential_id)),
            ts_event=millis_to_nanos(self.timestamp),
            ts_init=ts_init,
        )


class UpbitWebSocketOrderbook(msgspec.Struct, frozen=True):
    """
    Schema for Upbit orderbook. (HTTP)
    """

    type: str  # 타입 (orderbook : 호가)
    code: str  # 마켓 코드 (ex. KRW-BTC)
    total_ask_size: float  # 호가 매도 총 잔량
    total_bid_size: float  # 호가 매수 총 잔량
    orderbook_units: list[UpbitOrderbookUnit]  # 호가 (List of Objects)
    timestamp: int  # 타임스탬프 (millisecond)
    level: int  # 호가 모아보기 단위 (default: 0)

    def parse_to_order_book_snapshot(
        self,
        instrument_id: InstrumentId,
        ts_init: int,
    ) -> OrderBookDeltas:
        ts_event: int = millis_to_nanos(self.timestamp)
        bids: list[BookOrder] = [
            BookOrder(
                OrderSide.BUY,
                Price.from_str(str(o.bid_price)),
                Quantity.from_str(str(o.bid_size)),
                0,
            )
            for o in self.orderbook_units
        ]
        asks: list[BookOrder] = [
            BookOrder(
                OrderSide.SELL,
                Price.from_str(str(o.ask_price)),
                Quantity.from_str(str(o.ask_size)),
                0,
            )
            for o in self.orderbook_units
        ]

        deltas = [
            OrderBookDelta.clear(
                instrument_id=instrument_id,
                sequence=0,
                ts_init=ts_init,
                ts_event=ts_event,
            )
        ]
        for idx, bid in enumerate(bids):
            deltas.append(
                OrderBookDelta(
                    instrument_id=instrument_id,
                    action=BookAction.ADD,
                    order=bid,
                    flags=0 if idx < len(bids) else RecordFlag.F_LAST,
                    sequence=0,
                    ts_event=ts_event,
                    ts_init=ts_init,
                )
            )
        for idx, ask in enumerate(asks):
            deltas.append(
                OrderBookDelta(
                    instrument_id=instrument_id,
                    action=BookAction.ADD,
                    order=ask,
                    flags=0 if idx < len(asks) else RecordFlag.F_LAST,
                    sequence=0,
                    ts_event=ts_event,
                    ts_init=ts_init,
                )
            )
        return OrderBookDeltas(instrument_id=instrument_id, deltas=deltas)
