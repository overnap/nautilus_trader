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

from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser
from nautilus_trader.adapters.binance.common.enums import BinanceOrderSide
from nautilus_trader.adapters.binance.common.enums import BinanceOrderStatus
from nautilus_trader.adapters.binance.common.enums import BinanceOrderType
from nautilus_trader.adapters.binance.common.enums import BinanceTimeInForce
from nautilus_trader.adapters.upbit.common.enums import (
    UpbitEnumParser,
    UpbitOrderType,
    UpbitOrderStatus,
    UpbitOrderSideHttp,
    UpbitOrderSideWebSocket,
    UpbitTimeInForce,
    UpbitTradeFee,
)
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbol
from nautilus_trader.core.datetime import millis_to_nanos, dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import OrderListId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Currency, AccountBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


################################################################################
# HTTP responses
################################################################################


class BinanceStatusCode(msgspec.Struct, frozen=True):
    """
    HTTP response status code.
    """

    code: int
    msg: str


class UpbitAsset(msgspec.Struct, frozen=True):
    currency: str
    balance: str
    locked: str
    avg_buy_price: str
    avg_buy_price_modified: bool
    unit_currency: str

    def parse_to_account_balance(self) -> AccountBalance:
        currency = Currency.from_str(self.currency)
        free = Decimal(self.balance)
        locked = Decimal(self.locked)
        total: Decimal = free + locked
        return AccountBalance(
            total=Money(total, currency),
            locked=Money(locked, currency),
            free=Money(free, currency),
        )


class UpbitTrade(msgspec.Struct, frozen=True):
    market: str
    uuid: str
    price: str
    volume: str
    funds: str
    side: UpbitOrderSideHttp
    created_at: str

    def parse_to_fill_report(
        self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        report_id: UUID4,
        enum_parser: UpbitEnumParser,
        ts_init: int,
        order_id: str | VenueOrderId,
        use_position_ids: bool = True,
    ) -> FillReport:
        venue_position_id: PositionId | None
        if use_position_ids:
            venue_position_id = PositionId(f"{instrument_id}-{self.side.value}")
        else:
            venue_position_id = None

        # Upbit doesn't provide LiquiditySide; and there is no impact on fee in the KRW or BTC market
        liquidity_side = LiquiditySide.NO_LIQUIDITY_SIDE

        comimssion: Money | None
        # Hard coded market quotient extraction
        if self.market.split("-")[0] == "KRW":
            commission = UpbitTradeFee.KRW_COMMON * Money.from_str(self.funds)
        elif self.market.split("-")[0] == "BTC":
            commission = UpbitTradeFee.BTC * Money.from_str(self.funds)
        else:
            commission = None

        return FillReport(
            account_id=account_id,
            instrument_id=instrument_id,
            venue_order_id=VenueOrderId(str(order_id)),
            venue_position_id=venue_position_id,
            trade_id=TradeId(str(self.uuid)),
            order_side=enum_parser.parse_upbit_order_side(self.side),
            last_qty=Quantity.from_str(self.volume),
            last_px=Price.from_str(self.price),
            liquidity_side=liquidity_side,
            ts_event=dt_to_unix_nanos(pd.to_datetime(self.created_at, format="ISO8601")),
            commission=commission,
            report_id=report_id,
            ts_init=ts_init,
        )


class UpbitOrder(msgspec.Struct, frozen=True, omit_defaults=True):
    uuid: str
    side: UpbitOrderSideHttp
    ord_type: UpbitOrderType
    state: UpbitOrderStatus
    market: str
    created_at: str
    reserved_fee: str
    remaining_fee: str
    paid_fee: str
    locked: str
    trades_count: int
    price: str | None = None
    volume: str | None = None
    executed_volume: str | None = None
    remaining_volume: str | None = None
    time_in_force: UpbitTimeInForce | None = None
    trades: list[UpbitTrade] | None = None  # 주문 조회할 때만 들어온다

    def parse_to_order_status_report(
        self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        report_id: UUID4,
        enum_parser: UpbitEnumParser,
        ts_init: int,
        identifier: str | None,
    ) -> OrderStatusReport:
        if self.side is None:
            raise ValueError("`side` was `None` when a value was expected")
        if self.ord_type is None:
            raise ValueError("`ord_type` was `None` when a value was expected")
        if self.state is None:
            raise ValueError("`state` was `None` when a value was expected")

        client_order_id = ClientOrderId(identifier) if identifier else None
        order_list_id = None
        contingency_type = ContingencyType.NO_CONTINGENCY
        trigger_type = TriggerType.NO_TRIGGER

        trailing_offset = None
        trailing_offset_type = TrailingOffsetType.NO_TRAILING_OFFSET

        avg_px: Decimal | None
        updated_at = dt_to_unix_nanos(pd.to_datetime(self.created_at, format="ISO8601"))
        if self.trades:
            avg_px = Decimal(0)
            for trade in self.trades:
                avg_px += Decimal(trade.funds)
                updated_at = max(
                    updated_at, dt_to_unix_nanos(pd.to_datetime(trade.created_at, format="ISO8601"))
                )
            avg_px /= Decimal(self.executed_volume)
        else:
            avg_px = None

        post_only = False
        reduce_only = False

        return OrderStatusReport(
            account_id=account_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            order_list_id=order_list_id,
            venue_order_id=VenueOrderId(str(self.uuid)),
            order_side=enum_parser.parse_upbit_order_side(self.side),
            order_type=enum_parser.parse_upbit_order_type(self.ord_type),
            contingency_type=contingency_type,
            time_in_force=enum_parser.parse_upbit_time_in_force(self.time_in_force),
            order_status=enum_parser.parse_upbit_order_status(self.state),
            price=Price.from_str(self.price) if self.price else None,
            trigger_price=None,  # `decimal.Decimal`
            trigger_type=trigger_type,
            trailing_offset=trailing_offset,
            trailing_offset_type=trailing_offset_type,
            quantity=Quantity.from_str(self.volume if self.volume else self.price),
            filled_qty=Quantity.from_str(self.executed_volume if self.executed_volume else "0"),
            avg_px=avg_px,
            post_only=post_only,
            reduce_only=reduce_only,
            ts_accepted=dt_to_unix_nanos(pd.to_datetime(self.created_at, format="ISO8601")),
            ts_last=updated_at,
            report_id=report_id,
            ts_init=ts_init,
        )


################################################################################
# WebSocket responses
################################################################################


class UpbitWebSocketOrder(msgspec.Struct, frozen=True):
    type: str
    code: str
    uuid: str
    ask_bid: UpbitOrderSideWebSocket
    order_type: UpbitOrderType
    state: UpbitOrderStatus
    trades_count: int
    reserved_fee: float
    remaining_fee: float
    paid_fee: float
    locked: float
    executed_funds: float
    order_timestamp: int
    timestamp: int
    stream_type: str
    price: float | None = None
    avg_price: float | None = None
    volume: float | None = None
    remaining_volume: float | None = None
    executed_volume: float | None = None
    time_in_force: UpbitTimeInForce | None = None
    trade_uuid: str | None = None
    trade_timestamp: int | None = None

    def parse_to_order_status_report(
        self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        report_id: UUID4,
        enum_parser: UpbitEnumParser,
        ts_init: int,
        identifier: str | None,
    ) -> OrderStatusReport:
        print(self)
        if self.ask_bid is None:
            raise ValueError("`ask_bid` was `None` when a value was expected")
        if self.order_type is None:
            raise ValueError("`ord_type` was `None` when a value was expected")
        if self.state is None:
            raise ValueError("`state` was `None` when a value was expected")

        print("Valid Order!")
        client_order_id = ClientOrderId(identifier) if identifier else None
        order_list_id = None
        contingency_type = ContingencyType.NO_CONTINGENCY
        trigger_type = TriggerType.NO_TRIGGER

        trailing_offset = None
        trailing_offset_type = TrailingOffsetType.NO_TRAILING_OFFSET

        post_only = False
        reduce_only = False

        return OrderStatusReport(
            account_id=account_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            order_list_id=order_list_id,
            venue_order_id=VenueOrderId(str(self.uuid)),
            order_side=enum_parser.parse_upbit_order_side(self.ask_bid),
            order_type=enum_parser.parse_upbit_order_type(self.order_type),
            contingency_type=contingency_type,
            time_in_force=enum_parser.parse_upbit_time_in_force(self.time_in_force),
            order_status=enum_parser.parse_upbit_order_status(self.state),
            price=Price.from_str(self.price) if self.price else None,
            trigger_price=None,  # `decimal.Decimal`
            trigger_type=trigger_type,
            trailing_offset=trailing_offset,
            trailing_offset_type=trailing_offset_type,
            quantity=Quantity.from_str(self.volume if self.volume else self.price),
            filled_qty=Quantity.from_str(self.executed_volume if self.executed_volume else "0"),
            avg_px=Decimal(str(self.avg_price)),
            post_only=post_only,
            reduce_only=reduce_only,
            ts_accepted=millis_to_nanos(self.order_timestamp),
            ts_last=millis_to_nanos(self.timestamp),
            report_id=report_id,
            ts_init=ts_init,
        )

    def calculate_commission(self) -> Money:
        commission: Money | None
        # Hard coded market quotient extraction
        quote = self.code.split("-")[0]
        if quote == "KRW":
            commission = Money(
                UpbitTradeFee.KRW_COMMON * Decimal(str(self.price)) * Decimal(str(self.volume)),
                Currency.from_str(quote),
            )
        elif quote == "BTC":
            commission = Money(
                UpbitTradeFee.BTC * Decimal(str(self.price)) * Decimal(str(self.volume)),
                Currency.from_str(quote),
            )
        else:
            commission = None
        return commission

    def parse_to_fill_report(
        self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        report_id: UUID4,
        enum_parser: UpbitEnumParser,
        ts_init: int,
        use_position_ids: bool = True,
    ) -> FillReport:
        if self.state != UpbitOrderStatus.TRADE:
            raise ValueError(
                f'Only `state == "trade"` order can be parsed into trade, but "{self.state}"'
            )

        venue_position_id: PositionId | None
        if use_position_ids:
            venue_position_id = PositionId(f"{instrument_id}-{self.ask_bid.value}")
        else:
            venue_position_id = None

        # Upbit doesn't provide LiquiditySide; and there is no impact on fee in the KRW or BTC market
        liquidity_side = LiquiditySide.NO_LIQUIDITY_SIDE

        return FillReport(
            account_id=account_id,
            instrument_id=instrument_id,
            venue_order_id=VenueOrderId(self.uuid),
            venue_position_id=venue_position_id,
            trade_id=TradeId(self.trade_uuid),
            order_side=enum_parser.parse_upbit_order_side(self.ask_bid),
            last_qty=Quantity.from_str(self.volume),
            last_px=Price.from_str(self.price),
            liquidity_side=liquidity_side,
            ts_event=millis_to_nanos(self.trade_timestamp),
            commission=self.calculate_commission(),
            report_id=report_id,
            ts_init=ts_init,
        )


class UpbitWebSocketAssetUnit(msgspec.Struct, frozen=True):
    currency: str
    balance: float
    locked: float

    def parse_to_account_balance(self) -> AccountBalance:
        currency = Currency.from_str(self.currency)
        free = Decimal(self.balance)
        locked = Decimal(self.locked)
        total: Decimal = free + locked
        return AccountBalance(
            total=Money(total, currency),
            locked=Money(locked, currency),
            free=Money(free, currency),
        )


class UpbitWebSocketAsset(msgspec.Struct, frozen=True):
    type: str
    asset_uuid: str
    timestamp: int
    asset_timestamp: int
    stream_type: str
    assets: list[UpbitWebSocketAssetUnit]
