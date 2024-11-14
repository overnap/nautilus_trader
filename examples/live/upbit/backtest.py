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

import time
from decimal import Decimal

import pandas as pd

from nautilus_trader.adapters.binance.common.constants import BINANCE_VENUE
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.examples.algorithms.twap import TWAPExecAlgorithm
from nautilus_trader.examples.strategies.ema_cross_twap import EMACrossTWAP
from nautilus_trader.examples.strategies.ema_cross_twap import EMACrossTWAPConfig
from nautilus_trader.model.currencies import ETH
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.wranglers import TradeTickDataWrangler
from nautilus_trader.test_kit.providers import TestDataProvider
from nautilus_trader.test_kit.providers import TestInstrumentProvider

import asyncio

from nautilus_trader.core.rust.model import PriceType

from nautilus_trader.adapters.upbit.common.types import UpbitBar
from nautilus_trader.adapters.upbit.config import UpbitDataClientConfig, UpbitExecClientConfig
from nautilus_trader.adapters.upbit.factories import (
    UpbitLiveDataClientFactory,
    UpbitLiveExecClientFactory,
)
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
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId


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
from nautilus_trader.portfolio import PortfolioFacade
from nautilus_trader.trading.strategy import Strategy


import asyncio

from nautilus_trader.core.rust.model import PriceType

from nautilus_trader.adapters.upbit.common.types import UpbitBar
from nautilus_trader.adapters.upbit.config import UpbitDataClientConfig, UpbitExecClientConfig
from nautilus_trader.adapters.upbit.factories import (
    UpbitLiveDataClientFactory,
    UpbitLiveExecClientFactory,
)
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
from nautilus_trader.portfolio import PortfolioFacade
from nautilus_trader.trading.strategy import Strategy


# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***


class UpbitStochasticsConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType

    trade_size: Decimal
    period_k: PositiveInt = 14
    period_d: PositiveInt = 3
    ma_type: MovingAverageType = MovingAverageType.EXPONENTIAL
    ma_period: PositiveInt = 5

    signals_long: tuple[float] = (-80,)
    signals_exit_long: tuple[float] = (80,)

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

        self.trade_size = Decimal(config.trade_size)

        # Create the indicators for the strategy
        self.stochastics_raw = Stochastics(config.period_k, config.period_d)
        self.stochastics = MovingAverageFactory.create(config.ma_period, config.ma_type)

        self.signals_long = config.signals_long
        self.signals_exit_long = config.signals_exit_long

        self.close_positions_on_stop = config.close_positions_on_stop
        self.instrument: Instrument = None

        self.max_value_after_buy = None
        self.ts_trade = 0
        self.ts_log = 0

    def on_start(self) -> None:
        """
        Actions to be performed on strategy start.
        """
        self.instrument = self.cache.instrument(self.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.instrument_id}")
            self.stop()
            return

        # Register the indicators for updating
        self.register_indicator_for_bars(self.bar_type, self.stochastics_raw)

        # Early subscription
        self.subscribe_quote_ticks(self.instrument_id)

        # Get historical data
        self.request_bars(
            self.bar_type,
            start=self._clock.utc_now() - pd.Timedelta(minutes=30),
        )

        self.subscribe_bars(self.bar_type)

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

        last_value = self.stochastics.value
        self.stochastics.update_raw(self.stochastics_raw.value_d)
        value = self.stochastics.value

        # Check if indicators ready
        if not self.stochastics.initialized:
            self.log.info(
                f"Waiting for indicators to warm up [{self.cache.bar_count(self.bar_type)}]",
                color=LogColor.BLUE,
            )
            return  # Wait for indicators to warm up...

        if self.portfolio.is_flat(self.instrument_id):
            # Long signal
            for signal in self.signals_long:
                if last_value <= signal < value:
                    self.buy()
                    self.log.info(f"Buy at {value}", LogColor.MAGENTA)
                    self.ts_trade = self.clock.timestamp_ns()
                    self.max_value_after_buy = value

        elif self.portfolio.is_net_long(self.instrument_id):
            # Record the max value after buying
            self.max_value_after_buy = max(self.max_value_after_buy, value)

            # Exit long signal
            for signal in self.signals_exit_long:
                if last_value >= signal > value:
                    self.sell()
                    self.log.info(
                        f"Sell at {value}, max {self.max_value_after_buy}", LogColor.MAGENTA
                    )

        delay = int(1e9) * 5
        if self.ts_log + delay <= self.clock.timestamp_ns():
            self.log.info(
                f"Value: {value}, Price: {self.cache.price(self.instrument_id, PriceType.ASK)}",
                LogColor.CYAN,
            )
            self.ts_log = self.clock.timestamp_ns()

    def buy(self) -> None:
        """
        Users simple buy method (example).
        """
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.instrument_id,
            order_side=OrderSide.BUY,
            quantity=self.instrument.make_qty(self.trade_size),
            quote_quantity=True,
            # time_in_force=TimeInForce.FOK,
        )

        self.submit_order(order)

    def sell(self) -> None:
        """
        Users simple sell method (example).
        """
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.instrument_id,
            order_side=OrderSide.SELL,
            quantity=self.instrument.make_qty(self.trade_size),
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
        self.stochastics_raw.reset()

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


if __name__ == "__main__":
    # Configure backtest engine
    config = BacktestEngineConfig(
        trader_id=TraderId("BACKTESTER-001"),
        logging=LoggingConfig(
            log_level="INFO",
            log_colors=True,
            use_pyo3=False,
        ),
    )

    # Build the backtest engine
    engine = BacktestEngine(config=config)

    # Add a trading venue (multiple venues possible)
    BINANCE = Venue("BINANCE")
    engine.add_venue(
        venue=BINANCE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,  # Spot CASH account (not for perpetuals or futures)
        base_currency=None,  # Multi-currency account
        starting_balances=[Money(1_000_000.0, USDT), Money(0.0, ETH)],
    )

    # Add instruments
    ETHUSDT_BINANCE = TestInstrumentProvider.ethusdt_binance()
    engine.add_instrument(ETHUSDT_BINANCE)

    # Add data
    provider = TestDataProvider()
    wrangler = TradeTickDataWrangler(instrument=ETHUSDT_BINANCE)
    ticks = wrangler.process(provider.read_csv_ticks("binance/ethusdt-trades.csv"))
    engine.add_data(ticks)

    # Configure your strategy
    config = UpbitStochasticsConfig(
        instrument_id=ETHUSDT_BINANCE.id,
        bar_type=BarType.from_str("ETHUSDT.BINANCE-1-SECOND-LAST-INTERNAL"),
        trade_size="10",
        period_k=100,
        period_d=10,
        ma_type=MovingAverageType.EXPONENTIAL,
        ma_period=5,
        signals_long=(20,),
        signals_exit_long=(80,),
        close_positions_on_stop=True,
        order_id_tag="001",
    )

    # Instantiate and add your strategy
    strategy = UpbitStochastics(config=config)
    engine.add_strategy(strategy=strategy)

    time.sleep(0.1)
    input("Press Enter to continue...")

    # Run the engine (from start to end of data)
    engine.run()

    # Optionally view reports
    with pd.option_context(
        "display.max_rows",
        100,
        "display.max_columns",
        None,
        "display.width",
        300,
    ):
        print(engine.trader.generate_account_report(BINANCE))
        print(engine.trader.generate_order_fills_report())
        print(engine.trader.generate_positions_report())

    # For repeated backtest runs make sure to reset the engine
    engine.reset()

    # Good practice to dispose of the object
    engine.dispose()
