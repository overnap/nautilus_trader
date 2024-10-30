import msgspec.json
from nautilus_trader.core.nautilus_pyo3 import HttpMethod

from nautilus_trader.adapters.upbit.common.enums import (
    UpbitSecurityType,
    UpbitOrderSide,
    UpbitOrderType,
    UpbitTimeInForce,
)
from nautilus_trader.adapters.upbit.common.schemas.exchange import UpbitAsset, UpbitOrder
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbol
from nautilus_trader.adapters.upbit.http.client import UpbitHttpClient
from nautilus_trader.adapters.upbit.http.endpoint import UpbitHttpEndpoint
from nautilus_trader.core.correctness import PyCondition

from nautilus_trader.model.identifiers import VenueOrderId, ClientOrderId

from nautilus_trader.model.objects import Quantity, Price


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

        self._resp_decoder = msgspec.json.Decoder(list[UpbitAsset])

    async def get(self) -> list[UpbitAsset]:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, params=None)
        return self._resp_decoder.decode(raw)


class UpbitOrderHttp(UpbitHttpEndpoint):
    def __init__(
        self,
        client: UpbitHttpClient,
        base_endpoint: str,
    ):
        methods = {
            HttpMethod.GET: UpbitSecurityType.TRADE,
            HttpMethod.DELETE: UpbitSecurityType.TRADE,
        }
        url_path = base_endpoint + "order"

        super().__init__(
            client,
            methods,
            url_path,
        )

        self._resp_decoder = msgspec.json.Decoder(UpbitOrder)

    class GetDeleteParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        uuid: str | None = None
        identifier: str | None = None

    async def get(self, params: GetDeleteParameters) -> UpbitOrder:
        if params.uuid is None and params.identifier is None:
            raise ValueError("Get order without `uuid` and `identifier`!")

        method_type = HttpMethod.GET
        raw = await self._method(method_type, params)
        return self._resp_decoder.decode(raw)

    async def delete(self, params: GetDeleteParameters) -> UpbitOrder:
        if params.uuid is None and params.identifier is None:
            raise ValueError("Delete order without `uuid` and `identifier`!")

        method_type = HttpMethod.DELETE
        raw = await self._method(method_type, params)
        return self._resp_decoder.decode(raw)


class UpbitOrdersHttp(UpbitHttpEndpoint):
    def __init__(
        self,
        client: UpbitHttpClient,
        base_endpoint: str,
    ):
        methods = {
            HttpMethod.GET: UpbitSecurityType.TRADE,
            HttpMethod.POST: UpbitSecurityType.TRADE,
        }
        url_path = base_endpoint + "orders"

        super().__init__(
            client,
            methods,
            url_path,
        )

        self._post_resp_decoder = msgspec.json.Decoder(UpbitOrder)

    class PostParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        market: str
        side: UpbitOrderSide
        ord_type: UpbitOrderType
        volume: str | None = None
        price: str | None = None
        identifier: str | None = None
        time_in_force: UpbitTimeInForce | None = None

    async def post(self, params: PostParameters) -> UpbitOrder:
        if (
            params.ord_type == UpbitOrderType.LIMIT or params.ord_type == UpbitOrderType.MARKET
        ) and params.volume is None:
            raise ValueError(f"Post `ord_type == {params.ord_type.name}` order without `volume`!")
        if (
            params.ord_type == UpbitOrderType.LIMIT or params.ord_type == UpbitOrderType.PRICE
        ) and params.price is None:
            raise ValueError(f"Post `ord_type == {params.ord_type.name}` order without `price`!")

        print("ORDER! ", params)
        method_type = HttpMethod.POST
        raw = await self._method(method_type, params)
        return self._post_resp_decoder.decode(raw)


class UpbitExchangeHttpAPI:
    def __init__(self, client: UpbitHttpClient):
        PyCondition.not_none(client, "client")
        self.client = client

        self.base_endpoint = "/v1/"

        # Create Endpoints
        self._endpoint_accounts = UpbitAccountsHttp(client, self.base_endpoint)
        self._endpoint_order = UpbitOrderHttp(client, self.base_endpoint)
        self._endpoint_orders = UpbitOrdersHttp(client, self.base_endpoint)

    async def query_asset(self) -> list[UpbitAsset]:
        """
        Query asset information
        """
        return await self._endpoint_accounts.get()

    async def query_order(
        self,
        venue_order_id: VenueOrderId | None = None,
        client_order_id: ClientOrderId | None = None,
    ) -> UpbitOrder:
        """
        Query order information
        """
        return await self._endpoint_order.get(
            params=self._endpoint_order.GetDeleteParameters(
                uuid=venue_order_id,
                identifier=client_order_id,
            ),
        )

    async def cancel_order(
        self,
        venue_order_id: VenueOrderId | None = None,
        client_order_id: ClientOrderId | None = None,
    ) -> UpbitOrder:
        """
        Query cancel order
        """
        return await self._endpoint_order.delete(
            params=self._endpoint_order.GetDeleteParameters(
                uuid=venue_order_id,
                identifier=client_order_id,
            ),
        )

    async def new_order(
        self,
        market: UpbitSymbol,
        side: UpbitOrderSide,
        order_type: UpbitOrderType,
        time_in_force: UpbitTimeInForce | None = None,
        volume: Quantity | None = None,
        price: Price | None = None,
        client_order_id: ClientOrderId | None = None,
    ) -> UpbitOrder:
        """
        Query new order
        """
        return await self._endpoint_orders.post(
            params=self._endpoint_orders.PostParameters(
                market=str(market),
                side=side,
                volume=str(volume) if volume else None,
                price=str(price) if price else None,
                ord_type=order_type,
                identifier=client_order_id.value,
                time_in_force=time_in_force,
            ),
        )
