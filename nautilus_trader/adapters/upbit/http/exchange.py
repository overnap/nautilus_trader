import msgspec.json
from nautilus_trader.core.nautilus_pyo3 import HttpMethod

from nautilus_trader.adapters.upbit.common.enums import UpbitSecurityType
from nautilus_trader.adapters.upbit.common.schemas.account import UpbitBalanceInfo
from nautilus_trader.adapters.upbit.http.client import UpbitHttpClient
from nautilus_trader.adapters.upbit.http.endpoint import UpbitHttpEndpoint


class UpbitAccountsHttp(UpbitHttpEndpoint):
    def __init__(
        self,
        client: UpbitHttpClient,
        base_endpoint: str,
    ):
        methods = {HttpMethod.GET: UpbitSecurityType.TRADE}
        url_path = base_endpoint + "accounts"

        super().__init__(
            client,
            methods,
            url_path,
        )

        self._get_resp_decoder = msgspec.json.Decoder(list[UpbitBalanceInfo])

    async def get(self) -> list[UpbitBalanceInfo]:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, params=None)
        return self._get_resp_decoder.decode(raw)
