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

from __future__ import annotations

import json

from nautilus_trader.core.correctness import PyCondition

from nautilus_trader.adapters.upbit.common.enums import UpbitTradeFee


################################################################################
# HTTP responses
################################################################################


class UpbitSymbol(str):
    """
    Binance compatible symbol.
    """

    def __new__(cls, symbol: str) -> UpbitSymbol:  # noqa: PYI034
        PyCondition.valid_string(symbol, "symbol")

        # Format the string on construction to be Binance compatible
        return super().__new__(cls, symbol.upper().replace(" ", "").replace("/", ""))

    def parse_as_nautilus(self) -> str:
        return str(self)

    def calculate_upbit_fee(self) -> UpbitTradeFee:
        # Hard coded market quotient extraction
        quote = str(self).split("-")[0]
        if quote == "KRW":
            return UpbitTradeFee.KRW_COMMON
        elif quote == "BTC":
            return UpbitTradeFee.BTC
        elif quote == "USDT":
            raise ValueError(
                "Since Upbit does not have enough support for the liquidity side, "
                "USDT commission, which varies on maker/taker, cannot be inferred."
            )
        else:
            raise ValueError(f"The quote currency {quote} is not supported in Upbit.")


class UpbitSymbols(str):
    """
    Binance compatible list of symbols.
    """

    def __new__(cls, symbols: list[str]) -> UpbitSymbols:  # noqa: PYI034
        PyCondition.not_empty(symbols, "symbols")

        upbit_symbols: list[UpbitSymbol] = [UpbitSymbol(symbol) for symbol in symbols]
        return super().__new__(
            cls, json.dumps(upbit_symbols).replace('"', "").replace(" ", "")[1:-1]
        )

    def parse_str_to_list(self) -> list[UpbitSymbol]:
        upbit_symbols: list[UpbitSymbol] = json.loads(
            f"{{{self}}}"
        )  # TODO: 이 기괴한 문법 머임? 테스트 필요
        return upbit_symbols
