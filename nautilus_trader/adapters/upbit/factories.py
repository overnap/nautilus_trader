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
from functools import lru_cache

from nautilus_trader.adapters.upbit.common.constants import UPBIT_VENUE
from nautilus_trader.adapters.upbit.common.credentials import get_api_key
from nautilus_trader.adapters.upbit.common.credentials import get_api_secret
from nautilus_trader.adapters.binance.common.credentials import get_ed25519_private_key
from nautilus_trader.adapters.binance.common.credentials import get_rsa_private_key
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceKeyType
from nautilus_trader.adapters.binance.common.urls import get_http_base_url
from nautilus_trader.adapters.binance.common.urls import get_ws_base_url
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.config import BinanceExecClientConfig
from nautilus_trader.adapters.binance.futures.data import BinanceFuturesDataClient
from nautilus_trader.adapters.binance.futures.execution import BinanceFuturesExecutionClient
from nautilus_trader.adapters.binance.futures.providers import BinanceFuturesInstrumentProvider
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.adapters.binance.spot.data import BinanceSpotDataClient
from nautilus_trader.adapters.binance.spot.execution import BinanceSpotExecutionClient
from nautilus_trader.adapters.binance.spot.providers import BinanceSpotInstrumentProvider
from nautilus_trader.adapters.upbit.common.data import UpbitDataClient
from nautilus_trader.adapters.upbit.common.execution import UpbitExecutionClient
from nautilus_trader.adapters.upbit.config import UpbitDataClientConfig, UpbitExecClientConfig
from nautilus_trader.adapters.upbit.http.client import UpbitHttpClient
from nautilus_trader.adapters.upbit.spot.providers import UpbitInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.nautilus_pyo3 import Quota
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.model.identifiers import Venue


UPBIT_HTTP_CLIENTS: dict[str, UpbitHttpClient] = {}


@lru_cache(1)
def get_cached_upbit_http_client(
    clock: LiveClock,
    api_key: str | None = None,
    api_secret: str | None = None,
    base_url: str | None = None,
) -> UpbitHttpClient:
    """
    Cache and return a Binance HTTP client with the given key and secret.

    If a cached client with matching key and secret already exists, then that
    cached client will be returned.

    Parameters
    ----------
    clock : LiveClock
        The clock for the client.
    account_type : BinanceAccountType
        The account type for the client.
    api_key : str, optional
        The API key for the client.
    api_secret : str, optional
        The API secret for the client.
    key_type : BinanceKeyType, default 'HMAC'
        The private key cryptographic algorithm type.
    base_url : str, optional
        The base URL for the API endpoints.
    is_testnet : bool, default False
        If the client is connecting to the testnet API.
    is_us : bool, default False
        If the client is connecting to Binance US.

    Returns
    -------
    UpbitHttpClient

    """
    global UPBIT_HTTP_CLIENTS

    api_key = api_key or get_api_key()
    api_secret = api_secret or get_api_secret()
    default_http_base_url = "https://api.upbit.com/"

    # Set up rate limit quotas TODO: 업비트에 맞게 수정
    ratelimiter_default_quota = Quota.rate_per_second(10)
    ratelimiter_quotas: list[tuple[str, Quota]] = [
        ("orders", Quota.rate_per_second(8)),
        ("exchange", Quota.rate_per_second(30)),
        ("market", Quota.rate_per_second(10)),
    ]

    client_key: str = "|".join((api_key, api_secret, base_url))
    if client_key not in UPBIT_HTTP_CLIENTS:
        client = UpbitHttpClient(
            clock=clock,
            key=api_key,
            secret=api_secret,
            base_url=base_url or default_http_base_url,
            ratelimiter_quotas=ratelimiter_quotas,
            ratelimiter_default_quota=ratelimiter_default_quota,
        )
        UPBIT_HTTP_CLIENTS[client_key] = client
    return UPBIT_HTTP_CLIENTS[client_key]


@lru_cache(1)
def get_cached_upbit_instrument_provider(
    client: UpbitHttpClient,
    clock: LiveClock,
    config: InstrumentProviderConfig,
    venue: Venue | None = None,
) -> UpbitInstrumentProvider:
    """
    Cache and return an instrument provider for the Binance Spot/Margin exchange.

    If a cached provider already exists, then that provider will be returned.

    Parameters
    ----------
    client : BinanceHttpClient
        The client for the instrument provider.
    clock : LiveClock
        The clock for the instrument provider.
    account_type : BinanceAccountType
        The Binance account type for the instrument provider.
    is_testnet : bool, default False
        If the provider is for the Spot testnet.
    config : InstrumentProviderConfig
        The configuration for the instrument provider.
    venue : Venue
        The venue for the instrument provider.

    Returns
    -------
    UpbitInstrumentProvider

    """
    return UpbitInstrumentProvider(
        client=client,
        clock=clock,
        config=config,
        venue=venue or UPBIT_VENUE,
    )


class UpbitLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a Binance live data client factory.
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: UpbitDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> UpbitDataClient:
        """
        Create a new Binance data client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The custom client ID.
        config : BinanceDataClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        UpbitDataClient

        Raises
        ------
        ValueError
            If `config.account_type` is not a valid `BinanceAccountType`.

        """
        # Get HTTP client singleton
        client = get_cached_upbit_http_client(
            clock=clock,
            api_key=config.api_key,
            api_secret=config.api_secret,
            base_url=config.base_url_http,
        )

        # Get instrument provider singleton
        provider = get_cached_upbit_instrument_provider(
            client=client,
            clock=clock,
            config=config.instrument_provider,
            venue=config.venue,
        )
        return UpbitDataClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            name=name,
            config=config,
        )


class UpbitLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides a Binance live execution client factory.
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: UpbitExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> UpbitExecutionClient:
        """
        Create a new Binance execution client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The custom client ID.
        config : BinanceExecClientConfig
            The configuration for the client.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        UpbitExecutionClient

        Raises
        ------
        ValueError
            If `config.account_type` is not a valid `BinanceAccountType`.

        """
        # Get HTTP client singleton
        client = get_cached_upbit_http_client(
            clock=clock,
            api_key=config.api_key,
            api_secret=config.api_secret,
            base_url=config.base_url_http,
        )

        # Get instrument provider singleton
        provider = get_cached_upbit_instrument_provider(
            client=client,
            clock=clock,
            config=config.instrument_provider,
            venue=config.venue,
        )

        return UpbitExecutionClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            name=name,
            config=config,
        )
