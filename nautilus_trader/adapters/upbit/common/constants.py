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
from typing import Final

from nautilus_trader.model.identifiers import Venue


UPBIT_VENUE: Final[Venue] = Venue("UPBIT")

# TODO: 무슨 용도인지 확인 필요
UPBIT_MIN_CALLBACK_RATE: Final[Decimal] = Decimal("0.1")
UPBIT_MAX_CALLBACK_RATE: Final[Decimal] = Decimal("10.0")

UPBIT_MAX_CANDLE_COUNT: int = 200