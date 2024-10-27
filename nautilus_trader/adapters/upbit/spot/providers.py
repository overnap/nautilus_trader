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
from nautilus_trader.core.rust.model import CurrencyType

from nautilus_trader.adapters.upbit.common.constants import UPBIT_VENUE
from nautilus_trader.adapters.upbit.common.credentials import get_api_key, get_api_secret
from nautilus_trader.adapters.upbit.common.enums import UpbitTradeFee
from nautilus_trader.adapters.upbit.common.schemas.market import UpbitCodeInfo
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbol
from nautilus_trader.adapters.upbit.http.client import UpbitHttpClient
from nautilus_trader.adapters.upbit.http.market import UpbitMarketHttpAPI
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue

from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.instruments.currency_pair import CurrencyPair
from nautilus_trader.model.objects import PRICE_MAX, Currency
from nautilus_trader.model.objects import MONEY_MAX
from nautilus_trader.model.objects import PRICE_MIN
from nautilus_trader.model.objects import QUANTITY_MAX
from nautilus_trader.model.objects import QUANTITY_MIN
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class UpbitInstrumentProvider(InstrumentProvider):
    """
    Provides a means of loading instruments from the `Binance Spot/Margin` exchange.

    Parameters
    ----------
    client : APIClient
        The client for the provider.
    clock : LiveClock
        The clock for the provider.
    account_type : BinanceAccountType, default SPOT
        The Binance account type for the provider.
    is_testnet : bool, default False
        If the provider is for the Spot testnet.
    config : InstrumentProviderConfig, optional
        The configuration for the provider.

    """

    def __init__(
        self,
        client: UpbitHttpClient,
        clock: LiveClock,
        is_testnet: bool = False,
        config: InstrumentProviderConfig | None = None,
        venue: Venue = UPBIT_VENUE,
    ):
        super().__init__(config=config)

        self._clock = clock
        self._client = client
        self._is_testnet = is_testnet
        self._venue = venue

        self._http_market = UpbitMarketHttpAPI(self._client)

        self._log_warnings = config.log_warnings if config else True

        self._decoder = msgspec.json.Decoder()
        self._encoder = msgspec.json.Encoder()

    async def load_all_async(self, filters: dict | None = None) -> None:
        filters_str = "..." if not filters else f" with filters {filters}..."
        self._log.info(f"Loading all instruments{filters_str}")

        # Get exchange info for all assets
        code_info_list = await self._http_market.query_code_info()
        code_info_dict = {code_info.market: code_info for code_info in code_info_list}

        # Extract all symbol strings
        symbols = [code_info.market for code_info in code_info_list]
        prices = [ticker.trade_price for ticker in await self._http_market.query_ticker(symbols)]

        for symbol, price in zip(symbols, prices):
            await self._parse_instrument(
                code_info=code_info_dict[symbol],
                ts_event=self._clock.timestamp_ns(),  # TODO: server time 없어서 그냥 현재 시각?
                current_price=price,
            )

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        if not instrument_ids:
            self._log.info("No instrument IDs given for loading.")
            return

        # Check all instrument IDs
        for instrument_id in instrument_ids:
            PyCondition.equal(instrument_id.venue, self._venue, "instrument_id.venue", "UPBIT")

        filters_str = "..." if not filters else f" with filters {filters}..."
        self._log.info(f"Loading instruments {instrument_ids}{filters_str}.")

        # Extract all symbol strings
        symbols = [str(UpbitSymbol(instrument_id.symbol.value)) for instrument_id in instrument_ids]
        prices = [ticker.trade_price for ticker in await self._http_market.query_ticker(symbols)]

        # Get exchange info for all assets
        code_info_list = await self._http_market.query_code_info()
        code_info_dict = {code_info.market: code_info for code_info in code_info_list}

        for symbol, price in zip(symbols, prices):
            if symbol not in code_info_dict:
                self._log.error(
                    f"Cannot load instrument {instrument_id} because it is not provided by Upbit"
                )
                continue

            await self._parse_instrument(
                code_info=code_info_dict[symbol],
                ts_event=self._clock.timestamp_ns(),  # TODO: server time 없어서 그냥 현재 시각?
                current_price=price,
            )

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        PyCondition.not_none(instrument_id, "instrument_id")
        PyCondition.equal(instrument_id.venue, self._venue, "instrument_id.venue", "UPBIT")

        filters_str = "..." if not filters else f" with filters {filters}..."
        self._log.debug(f"Loading instrument {instrument_id}{filters_str}.")

        symbol = str(UpbitSymbol(instrument_id.symbol.value))
        # Get exchange info for asset
        code_info_list = await self._http_market.query_code_info()
        for code_info in code_info_list:
            if code_info.market == symbol:
                await self._parse_instrument(
                    code_info=code_info,
                    ts_event=self._clock.timestamp_ns(),  # TODO: server time 없어서 그냥 현재 시각?
                )
                return
        self._log.error(
            f"Cannot load instrument {instrument_id} because it is not provided by Upbit"
        )

    async def _parse_instrument(  # TODO: 현재가 필요해서 async로 바꿔버림. 괜찮은지 확인
        self,
        code_info: UpbitCodeInfo,
        ts_event: int,
        current_price: float | Price | None = None,
    ) -> None:
        ts_init = self._clock.timestamp_ns()
        try:
            [quote_asset, base_asset] = code_info.market.split("-")
            base_currency = Currency(
                code=base_asset,
                precision=8,
                iso4217=0,
                name=base_asset,
                currency_type=CurrencyType.CRYPTO,
            )
            quote_currency = Currency(
                code=quote_asset,
                precision=8,
                iso4217=0,
                name=quote_asset,
                currency_type=CurrencyType.CRYPTO,
            )

            # TODO: 틱사이즈 = base의 단위, 스텝사이즈 = quote의 단위
            raw_symbol = Symbol(code_info.market)
            instrument_id = InstrumentId(symbol=raw_symbol, venue=self._venue)

            instrument: Instrument
            if quote_asset == "KRW":
                if current_price is None:
                    current_price = (
                        await self._http_market.request_trade_ticks(
                            instrument_id=instrument_id, ts_init=self._clock.timestamp_ns()
                        )
                    )[0].price

                # TODO: Price랑 숫자랑 비교가 제대로 동작하는지 확인.
                order_precision: str
                if current_price >= 2_000_000:
                    order_precision = "1000"
                elif current_price >= 1_000_000:
                    order_precision = "500"
                elif current_price >= 500_000:
                    order_precision = "100"
                elif current_price >= 100_000:
                    order_precision = "50"
                elif current_price >= 10_000:
                    order_precision = "10"
                elif current_price >= 1_000:
                    order_precision = "1"
                elif current_price >= 100:
                    if base_asset in [
                        "ADA",
                        "ALGO",
                        "BLUR",
                        "CELO",
                        "ELF",
                        "EOS",
                        "GRS",
                        "GRT",
                        "ICX",
                        "MANA",
                        "MINA",
                        "POL",
                        "SAND",
                        "SEI",
                        "STG",
                        "TRX",
                    ]:  # https://upbit.com/service_center/notice?id=4487
                        order_precision = "1"
                    else:
                        order_precision = "0.1"
                elif current_price >= 10:
                    order_precision = "0.01"
                elif current_price >= 1:
                    order_precision = "0.001"
                elif current_price >= 0.1:
                    order_precision = "0.0001"
                elif current_price >= 0.01:
                    order_precision = "0.00001"
                elif current_price >= 0.001:
                    order_precision = "0.000001"
                elif current_price >= 0.0001:
                    order_precision = "0.0000001"
                else:  # current_price < 0.0001
                    order_precision = "0.00000001"

                instrument = CurrencyPair(
                    instrument_id=instrument_id,
                    raw_symbol=raw_symbol,
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                    price_precision=abs(int(Decimal(order_precision).as_tuple().exponent)),
                    size_precision=8,
                    price_increment=Price.from_str(order_precision),
                    size_increment=Quantity.from_str("0.00000001"),
                    lot_size=Quantity.from_str("0.00000001"),
                    max_quantity=Quantity.from_str("4294967296"),  # TODO: 너무 작을 수 있음.
                    min_quantity=Quantity.from_str("0.00000001"),
                    max_notional=Money(
                        "4294967296", currency=quote_currency
                    ),  # TODO: 너무 작을 수 있음.
                    min_notional=Money("5000", currency=quote_currency),
                    max_price=Price.from_str("4294967296"),
                    min_price=Price.from_str(order_precision),
                    margin_init=Decimal(0),
                    margin_maint=Decimal(0),
                    maker_fee=UpbitTradeFee.KRW_COMMON.value,
                    taker_fee=UpbitTradeFee.KRW_COMMON.value,
                    ts_event=min(ts_event, ts_init),
                    ts_init=ts_init,
                    info=msgspec.structs.asdict(code_info),
                )
            elif quote_asset == "BTC":
                instrument = CurrencyPair(
                    instrument_id=instrument_id,
                    raw_symbol=raw_symbol,
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                    price_precision=8,
                    size_precision=8,
                    price_increment=Price.from_str("0.00000001"),
                    size_increment=Quantity.from_str("0.00000001"),
                    lot_size=Quantity.from_str("0.00000001"),
                    max_quantity=Quantity.from_str("4294967296"),  # TODO: 너무 작을 수 있음.
                    min_quantity=Quantity.from_str("0.00000001"),
                    max_notional=Money(
                        "4294967296", currency=quote_currency
                    ),  # TODO: 너무 작을 수 있음.
                    min_notional=Money("0.00005", currency=quote_currency),
                    max_price=Price.from_str("4294967296"),
                    min_price=Price.from_str("0.00000001"),
                    margin_init=Decimal(0),
                    margin_maint=Decimal(0),
                    maker_fee=UpbitTradeFee.BTC.value,
                    taker_fee=UpbitTradeFee.BTC.value,
                    ts_event=min(ts_event, ts_init),
                    ts_init=ts_init,
                    info=msgspec.structs.asdict(code_info),
                )
            elif quote_asset == "USDT":
                if current_price is None:
                    current_price = (
                        await self._http_market.request_trade_ticks(
                            instrument_id=instrument_id, ts_init=self._clock.timestamp_ns()
                        )
                    )[0].price

                order_precision: str
                if current_price >= 10:
                    order_precision = "0.01"
                elif current_price >= 1:
                    order_precision = "0.001"
                elif current_price >= 0.1:
                    order_precision = "0.0001"
                elif current_price >= 0.01:
                    order_precision = "0.00001"
                elif current_price >= 0.001:
                    order_precision = "0.000001"
                elif current_price >= 0.0001:
                    order_precision = "0.0000001"
                else:  # current_price < 0.001
                    order_precision = "0.00000001"

                instrument = CurrencyPair(
                    instrument_id=instrument_id,
                    raw_symbol=raw_symbol,
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                    price_precision=abs(int(Decimal(order_precision).as_tuple().exponent)),
                    size_precision=8,
                    price_increment=Price.from_str(order_precision),
                    size_increment=Quantity.from_str("0.00000001"),
                    lot_size=Quantity.from_str("0.00000001"),
                    max_quantity=Quantity.from_str("4294967296"),
                    min_quantity=Quantity.from_str("0.00000001"),
                    max_notional=Money("4294967296", currency=quote_currency),
                    min_notional=Money("0.5", currency=quote_currency),
                    max_price=Price.from_str("4294967296"),
                    min_price=Price.from_str(order_precision),
                    margin_init=Decimal(0),
                    margin_maint=Decimal(0),
                    maker_fee=UpbitTradeFee.USDT_MAKER.value,
                    taker_fee=UpbitTradeFee.USDT_TAKER.value,
                    ts_event=min(ts_event, ts_init),
                    ts_init=ts_init,
                    info=msgspec.structs.asdict(code_info),
                )
            else:
                raise ValueError(f"quote `{quote_asset}` is not in {{KRW, BTC, USDT}}")

            self.add_currency(currency=instrument.base_currency)
            self.add_currency(currency=instrument.quote_currency)
            self.add(instrument=instrument)

            self._log.debug(f"Added instrument {instrument.id}.")
        except ValueError as e:
            if self._log_warnings:
                self._log.warning(f"Unable to parse instrument {code_info.symbol}: {e}.")


if __name__ == "__main__":
    clock = LiveClock()
    http_client = UpbitHttpClient(
        clock=clock,
        key=get_api_key(),
        secret=get_api_secret(),
        base_url="https://api.upbit.com/",
    )

    # UpbitCodeInfo(msgspec.Struct, frozen=True):
    #
    # market: str
    # korean_name: str
    # english_name: str

    ip = UpbitInstrumentProvider(http_client, clock)
    print(ip._instruments)

    code_info = UpbitCodeInfo("KRW-BTC", "비트코인", "블라블라")
    res = asyncio.run(ip._parse_instrument(code_info, clock.timestamp_ns()))

    code_info = UpbitCodeInfo("USDT-BTC", "비트코인", "블라블라")
    res = asyncio.run(ip._parse_instrument(code_info, clock.timestamp_ns()))
    print(ip._instruments)

    asyncio.run(ip.load_all_async())

    asyncio.run(
        ip.load_ids_async(
            [
                InstrumentId(Symbol("KRW-BTC"), UPBIT_VENUE),
                InstrumentId(Symbol("KRW-ETH"), UPBIT_VENUE),
            ]
        )
    )

    asyncio.run(ip.load_async(InstrumentId(Symbol("KRW-BTC"), UPBIT_VENUE)))
