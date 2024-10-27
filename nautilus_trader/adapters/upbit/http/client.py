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

import hashlib
import jwt  # TODO: 의존성 제거
import urllib.parse
from typing import Any

import msgspec

import nautilus_trader
from nautilus_trader.adapters.binance.http.error import BinanceClientError
from nautilus_trader.adapters.binance.http.error import BinanceServerError
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.core.nautilus_pyo3 import HttpResponse
from nautilus_trader.core.nautilus_pyo3 import Quota
from nautilus_trader.core.nautilus_pyo3 import UUID4


class UpbitHttpClient:
    """
    Provides a `Binance` asynchronous HTTP client.

    Parameters
    ----------
    clock : LiveClock
        The clock for the client.
    key : str
        The Binance API key for requests.
    secret : str
        The Binance API secret for signed requests.
    base_url : str, optional
        The base endpoint URL for the client.
    ratelimiter_quotas : list[tuple[str, Quota]], optional
        The keyed rate limiter quotas for the client.
    ratelimiter_quota : Quota, optional
        The default rate limiter quota for the client.

    """

    def __init__(
        self,
        clock: LiveClock,
        key: str,
        secret: str,
        base_url: str,
        ratelimiter_quotas: list[tuple[str, Quota]] | None = None,
        ratelimiter_default_quota: Quota | None = None,
    ) -> None:
        self._clock: LiveClock = clock
        self._log: Logger = Logger(type(self).__name__)
        self._key: str = key
        self._secret: str = secret

        self._base_url: str = base_url
        self._headers: dict[str, Any] = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": nautilus_trader.USER_AGENT,
        }
        self._client = HttpClient(
            keyed_quotas=ratelimiter_quotas or [],
            default_quota=ratelimiter_default_quota,
        )

    @property
    def base_url(self) -> str:
        """
        Return the base URL being used by the client.

        Returns
        -------
        str

        """
        return self._base_url

    @property
    def api_key(self) -> str:
        """
        Return the Binance API key being used by the client.

        Returns
        -------
        str

        """
        return self._key

    @property
    def headers(self):
        """
        Return the headers being used by the client.

        Returns
        -------
        str

        """
        return self._headers

    def _prepare_params(self, params: dict[str, Any]) -> str:
        # Encode a dict into a URL query string
        return urllib.parse.urlencode(params)

    def _get_auth(self, data: str) -> str:
        m = hashlib.sha512()
        m.update(data.encode())

        payload = {
            "access_token": self._key,
            "nonce": UUID4().value,
            "query_hash": m.hexdigest(),
        }

        jwt_token = jwt.encode(payload, self._secret)
        return f"Bearer {jwt_token}"

    # TODO: 좋지 못한 패턴? 이러면 private 웹소켓 쓸려면 http client도 무조건 필요
    def get_auth_without_data(self) -> str:
        payload = {
            "access_key": self._key,
            "nonce": UUID4().value,
        }

        jwt_token = jwt.encode(payload, self._secret)
        return f"Bearer {jwt_token}"

    async def sign_request(
        self,
        http_method: HttpMethod,
        url_path: str,
        payload: dict[str, str] | None = None,
        ratelimiter_keys: list[str] | None = None,
    ) -> Any:
        if payload is None:
            payload = {}
        query_string = self._prepare_params(payload)
        auth_headers = self._headers.copy()
        auth_headers["Authorization"] = self._get_auth(query_string)

        return await self.send_request(
            http_method,
            url_path,
            payload=payload,
            ratelimiter_keys=ratelimiter_keys,
            auth_headers=auth_headers,
        )

    async def send_request(
        self,
        http_method: HttpMethod,
        url_path: str,
        payload: dict[str, str] | None = None,
        ratelimiter_keys: list[str] | None = None,
        auth_headers: dict[str, Any] | None = None,
    ) -> bytes:
        if payload:
            url_path += "?" + urllib.parse.urlencode(payload)
            payload = None  # Don't send payload in the body

        self._log.debug(f"{url_path} {payload}", LogColor.MAGENTA)
        print(f"{url_path}")

        response: HttpResponse = await self._client.request(
            http_method,
            url=self._base_url + url_path,
            headers=auth_headers if auth_headers is not None else self._headers,
            body=msgspec.json.encode(payload) if payload else None,
            keys=ratelimiter_keys,
        )

        if 400 <= response.status < 500:
            raise BinanceClientError(  # TODO: 에러 수정
                status=response.status,
                message=msgspec.json.decode(response.body) if response.body else None,
                headers=response.headers,
            )
        elif response.status >= 500:
            raise BinanceServerError(
                status=response.status,
                message=msgspec.json.decode(response.body) if response.body else None,
                headers=response.headers,
            )

        return response.body
