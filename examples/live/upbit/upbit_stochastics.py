#!/usr/bin/env python3
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

from nautilus_trader.core.rust.model import PriceType, TimeInForce

from nautilus_trader.adapters.upbit.common.types import UpbitBar
from nautilus_trader.adapters.upbit.config import UpbitDataClientConfig, UpbitExecClientConfig
from nautilus_trader.adapters.upbit.factories import (
    UpbitLiveDataClientFactory,
    UpbitLiveExecClientFactory,
)
from nautilus_trader.cache.config import CacheConfig
from nautilus_trader.common.config import DatabaseConfig, MessageBusConfig
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.examples.strategies.ema_cross import EMACross
from nautilus_trader.examples.strategies.ema_cross import EMACrossConfig
from nautilus_trader.indicators.average.sma import SimpleMovingAverage

from nautilus_trader.indicators.average.moving_average import MovingAverageType, MovingAverage

from nautilus_trader.indicators.average.ma_factory import MovingAverageFactory

from nautilus_trader.indicators.atr import AverageTrueRange

from nautilus_trader.indicators.stochastics import Stochastics
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId


# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***

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

import pandas as pd

from nautilus_trader.adapters.upbit.common.constants import UPBIT_VENUE
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbol
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.data import Data
from nautilus_trader.core.message import Event
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency, Money, AccountBalance, Quantity, Price
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.orders.limit import LimitOrder
from nautilus_trader.portfolio import PortfolioFacade
from nautilus_trader.trading.strategy import Strategy
from tests.integration_tests.adapters.conftest import portfolio


# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***


class UpbitStochasticsConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType

    index_instrument_id: InstrumentId
    index_bar_type: BarType

    period_k: PositiveInt
    period_d: PositiveInt
    ma_type: MovingAverageType
    ma_period_k: PositiveInt
    ma_period_d: PositiveInt

    trade_size: Decimal = Decimal("10")
    close_positions_on_stop: bool = True


class UpbitStochastics(Strategy):
    """
    A simple moving average cross example strategy.

    When the fast EMA crosses the slow EMA then enter a position at the market
    in that direction.

    Parameters
    ----------
    config : PGOConfig
        The configuration for the instance.

    Raises
    ------
    ValueError
        If `config.fast_ema_period` is not less than `config.slow_ema_period`.

    """

    def __init__(self, config: UpbitStochasticsConfig) -> None:
        super().__init__(config)

        # Configuration
        self.instrument_id = config.instrument_id
        self.bar_type = config.bar_type
        self.index_instrument_id = config.index_instrument_id
        self.index_bar_type = config.index_bar_type

        # Create the indicators for the strategy
        self.stochastics = Stochastics(config.period_k, config.period_d)
        self.slow_k = MovingAverageFactory.create(config.ma_period_k, config.ma_type)
        self.slow_d = MovingAverageFactory.create(config.ma_period_d, config.ma_type)

        self.index_stochastics = Stochastics(config.period_k, config.period_d)
        self.index_slow_k = MovingAverageFactory.create(config.ma_period_k, config.ma_type)
        self.index_slow_d = MovingAverageFactory.create(config.ma_period_d, config.ma_type)

        self.close_positions_on_stop = config.close_positions_on_stop
        self.instrument: Instrument = None
        self.trade_size = Decimal(config.trade_size)

        self.ts_trade = 0
        self.ts_log = 0

    def on_start(self) -> None:
        """
        Actions to be performed on strategy start.
        """
        self.instrument = self.cache.instrument(self.instrument_id)
        self.index_instrument = self.cache.instrument(self.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.instrument_id}")
            self.stop()
            return

        # Register the indicators for updating      w
        self.register_indicator_for_bars(self.bar_type, self.stochastics)
        self.register_indicator_for_bars(self.index_bar_type, self.index_stochastics)

        self.log.info(
            f"Start with {self.portfolio.account(UPBIT_VENUE).balance(self.instrument.quote_currency)}",
            LogColor.MAGENTA,
        )

        # Early subscription
        self.subscribe_quote_ticks(self.instrument_id)

        # Get historical data
        self.request_bars(
            self.bar_type,
            start=self._clock.utc_now() - pd.Timedelta(minutes=30),
        )
        self.request_bars(
            self.index_bar_type,
            start=self._clock.utc_now() - pd.Timedelta(minutes=30),
        )

        async def lazy_subscription():
            while True:
                # Wait until historical data is fetched completely
                if self.cache.has_bars(self.bar_type):
                    # Subscribe to live data
                    self.subscribe_bars(self.bar_type)
                    break
                else:
                    await asyncio.sleep(0.1)
            while True:
                # Wait until historical data is fetched completely
                if self.cache.has_bars(self.index_bar_type):
                    # Subscribe to live data
                    self.subscribe_bars(self.index_bar_type)
                    break
                else:
                    await asyncio.sleep(0.1)

        asyncio.get_event_loop().create_task(lazy_subscription())

    def on_instrument(self, instrument: Instrument) -> None:
        """
        Actions to be performed when the strategy is running and receives an instrument.

        Parameters
        ----------
        instrument : Instrument
            The instrument received.

        """
        # For debugging (must add a subscription)
        # self.log.info(repr(instrument), LogColor.CYAN)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """
        Actions to be performed when the strategy is running and receives order book
        deltas.

        Parameters
        ----------
        deltas : OrderBookDeltas
            The order book deltas received.

        """
        # For debugging (must add a subscription)
        # self.log.info(repr(deltas), LogColor.CYAN)

    def on_order_book(self, order_book: OrderBook) -> None:
        """
        Actions to be performed when the strategy is running and receives an order book.

        Parameters
        ----------
        order_book : OrderBook
            The order book received.

        """
        # For debugging (must add a subscription)
        # self.log.info(repr(order_book), LogColor.CYAN)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        Actions to be performed when the strategy is running and receives a quote tick.

        Parameters
        ----------
        tick : QuoteTick
            The tick received.

        """
        # For debugging (must add a subscription)
        # self.log.info(repr(tick), LogColor.CYAN)

    def on_trade_tick(self, tick: TradeTick) -> None:
        """
        Actions to be performed when the strategy is running and receives a trade tick.

        Parameters
        ----------
        tick : TradeTick
            The tick received.

        """
        # For debugging (must add a subscription)
        # self.log.info(repr(tick), LogColor.CYAN)

    def on_bar(self, bar: Bar) -> None:
        """
        Actions to be performed when the strategy is running and receives a bar.

        Parameters
        ----------
        bar : Bar
            The bar received.

        """
        # self.log.info(repr(bar), LogColor.CYAN)

        # Check if indicators ready
        if not self.indicators_initialized():
            self.log.info(
                f"Waiting for indicators to warm up [{self.cache.bar_count(self.bar_type)}]",
                color=LogColor.BLUE,
            )
            return  # Wait for indicators to warm up...

        # if bar.is_single_price():
        #     # Implies no market information for this bar
        #     return

        if bar.bar_type == self.bar_type:
            self.slow_k.update_raw(self.stochastics.value_d)
            if not self.slow_k.initialized:
                self.log.info(
                    f"Waiting for indicators to warm up [{self.cache.bar_count(self.bar_type)}]",
                    color=LogColor.BLUE,
                )
                return

            self.slow_d.update_raw(self.slow_k.value)
            if not self.slow_d.initialized:
                self.log.info(
                    f"Waiting for indicators to warm up [{self.cache.bar_count(self.bar_type)}]",
                    color=LogColor.BLUE,
                )
                return

            if self.portfolio.is_flat(self.instrument_id):
                # Long signal
                if self.slow_d.value + 1 < self.slow_k.value:
                    self.buy()
                    self.log.info(
                        f"Buy at K={self.slow_k.value} D={self.slow_d.value} "
                        f"price={self.cache.price(self.instrument_id, PriceType.ASK)}",
                        LogColor.MAGENTA,
                    )
                    self.ts_trade = self.clock.timestamp_ns()

            elif self.portfolio.is_net_long(self.instrument_id):
                # Exit long signal
                if self.slow_d.value >= self.slow_k.value:
                    self.sell()
                    self.log.info(
                        f"Sell at K={self.slow_k.value} D={self.slow_d.value} "
                        f"price={self.cache.price(self.instrument_id, PriceType.BID)}",
                        LogColor.MAGENTA,
                    )
                    self.ts_trade = self.clock.timestamp_ns()

            delay = int(1e9) * 5
            if self.ts_log + delay <= self.clock.timestamp_ns():
                self.log.info(
                    f"K: {self.slow_k.value}, D: {self.slow_d.value}, "
                    f"Price: {self.cache.price(self.instrument_id, PriceType.ASK)}",
                    LogColor.CYAN,
                )
                self.ts_log = self.clock.timestamp_ns()
        else:
            self.index_slow_k.update_raw(self.index_stochastics.value_d)
            if not self.index_slow_k.initialized:
                self.log.info(
                    f"Waiting for indicators to warm up [{self.cache.bar_count(self.bar_type)}]",
                    color=LogColor.BLUE,
                )
                return

            self.index_slow_d.update_raw(self.index_slow_k.value)
            if not self.index_slow_d.initialized:
                self.log.info(
                    f"Waiting for indicators to warm up [{self.cache.bar_count(self.bar_type)}]",
                    color=LogColor.BLUE,
                )
                return

    def buy(self) -> None:
        """
        Users simple buy method (example).
        """
        # balance: AccountBalance = self.portfolio.account(UPBIT_VENUE).balance(
        #     self.instrument.quote_currency
        # )
        # if balance is not None and balance.free.as_double() > 1:
        # order: MarketOrder = self.order_factory.market(
        #     instrument_id=self.instrument_id,
        #     order_side=OrderSide.BUY,
        #     quantity=Quantity.from_str(
        #         str(
        #             self.instrument.make_price(
        #                 (balance.free.as_decimal() - 1)
        #                 * (
        #                     1
        #                     - UpbitSymbol(self.instrument.symbol.value)
        #                     .calculate_upbit_fee()
        #                     .value
        #                 )
        #             ).as_decimal()
        #         )
        #     ),
        #     quote_quantity=True,
        #     # time_in_force=TimeInForce.FOK,
        # )
        order: LimitOrder = self.order_factory.limit(
            instrument_id=self.instrument_id,
            order_side=OrderSide.BUY,
            quantity=self.instrument.make_qty(self.trade_size),
            price=self.instrument.make_price(
                self.cache.price(self.instrument_id, PriceType.BID).as_double()
            ),
            time_in_force=TimeInForce.FOK,
        )

        self.submit_order(order)

    def sell(self) -> None:
        """
        Users simple sell method (example).
        """
        balance: AccountBalance = self.portfolio.account(UPBIT_VENUE).balance(
            self.instrument.get_base_currency()
        )
        if balance is not None and balance.free.as_double() > 0:
            order: MarketOrder = self.order_factory.market(
                instrument_id=self.instrument_id,
                order_side=OrderSide.SELL,
                quantity=self.instrument.make_qty(balance.free.as_double()),
                # time_in_force=TimeInForce.FOK,
            )

            self.submit_order(order)

    def on_data(self, data: Data) -> None:
        """
        Actions to be performed when the strategy is running and receives data.

        Parameters
        ----------
        data : Data
            The data received.

        """

    def on_event(self, event: Event) -> None:
        """
        Actions to be performed when the strategy is running and receives an event.

        Parameters
        ----------
        event : Event
            The event received.

        """
        self._log.debug(f"Event: {event!r}", LogColor.MAGENTA)

    def on_stop(self) -> None:
        """
        Actions to be performed when the strategy is stopped.
        """
        self.cancel_all_orders(self.instrument_id)
        if self.close_positions_on_stop:
            self.close_all_positions(self.instrument_id)

        # Unsubscribe from data
        self.unsubscribe_bars(self.bar_type)
        self.unsubscribe_bars(self.index_bar_type)
        self.unsubscribe_quote_ticks(self.instrument_id)
        # self.unsubscribe_trade_ticks(self.instrument_id)
        # self.unsubscribe_ticker(self.instrument_id)
        # self.unsubscribe_order_book_deltas(self.instrument_id)
        # self.unsubscribe_order_book_at_interval(self.instrument_id)

    def on_reset(self) -> None:
        """
        Actions to be performed when the strategy is reset.
        """
        # Reset indicators here
        self.stochastics.reset()
        self.slow_d.reset()
        self.slow_k.reset()
        self.index_stochastics.reset()
        self.index_slow_k.reset()
        self.index_slow_d.reset()

    def on_save(self) -> dict[str, bytes]:
        """
        Actions to be performed when the strategy is saved.

        Create and return a state dictionary of values to be saved.

        Returns
        -------
        dict[str, bytes]
            The strategy state dictionary.

        """
        return {}

    def on_load(self, state: dict[str, bytes]) -> None:
        """
        Actions to be performed when the strategy is loaded.

        Saved state values will be contained in the give state dictionary.

        Parameters
        ----------
        state : dict[str, bytes]
            The strategy state dictionary.

        """

    def on_dispose(self) -> None:
        """
        Actions to be performed when the strategy is disposed.

        Cleanup any resources used by the strategy here.

        """


# Configure the trading node
config_node = TradingNodeConfig(
    trader_id=TraderId("TESTER-001"),
    logging=LoggingConfig(log_level="INFO"),
    exec_engine=LiveExecEngineConfig(
        reconciliation=True,
        reconciliation_lookback_mins=1440,
        # snapshot_orders=True,
        # snapshot_positions=True,
        # snapshot_positions_interval_secs=5.0,
    ),
    cache=CacheConfig(
        database=DatabaseConfig(),
        buffer_interval_ms=100,
        flush_on_start=True,
    ),
    message_bus=MessageBusConfig(
        database=DatabaseConfig(),
        # encoding="json",
        streams_prefix="quoters",
        use_instance_id=False,
        # types_filter=[QuoteTick],
        autotrim_mins=1,
        heartbeat_interval_secs=1,
    ),
    data_clients={
        "UPBIT": UpbitDataClientConfig(
            instrument_provider=InstrumentProviderConfig(load_all=True),
        ),
    },
    exec_clients={
        "UPBIT": UpbitExecClientConfig(
            instrument_provider=InstrumentProviderConfig(load_all=True),
            max_retries=3,
            retry_delay=1.0,
        ),
    },
    timeout_connection=30.0,
    timeout_reconciliation=10.0,
    timeout_portfolio=10.0,
    timeout_disconnection=10.0,
    timeout_post_stop=5.0,
)

# Instantiate the node with a configuration
node = TradingNode(config=config_node)

# Configure your strategy
strat_config = UpbitStochasticsConfig(
    instrument_id=InstrumentId.from_str("KRW-DOGE.UPBIT"),
    external_order_claims=[InstrumentId.from_str("KRW-DOGE.UPBIT")],
    bar_type=BarType.from_str("KRW-DOGE.UPBIT-1-SECOND-LAST-EXTERNAL"),
    index_instrument_id=InstrumentId.from_str("KRW-BTC.UPBIT"),
    index_bar_type=BarType.from_str("KRW-BTC.UPBIT-1-SECOND-LAST-EXTERNAL"),
    period_k=600,
    period_d=10,
    ma_type=MovingAverageType.EXPONENTIAL,
    ma_period_k=10,
    ma_period_d=5,
    trade_size=Decimal("10"),
    close_positions_on_stop=True,
    order_id_tag="001",
)
# Instantiate your strategy
strategy = UpbitStochastics(config=strat_config)

# Add your strategies and modules
node.trader.add_strategy(strategy)

# Register your client factories with the node (can take user-defined factories)
node.add_data_client_factory("UPBIT", UpbitLiveDataClientFactory)
node.add_exec_client_factory("UPBIT", UpbitLiveExecClientFactory)
node.build()


# Stop and dispose of the node with SIGINT/CTRL+C
if __name__ == "__main__":
    try:
        node.run()
    finally:
        node.dispose()
