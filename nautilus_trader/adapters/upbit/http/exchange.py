import msgspec.json
from nautilus_trader.core.nautilus_pyo3 import HttpMethod

from nautilus_trader.adapters.upbit.common.enums import (
    UpbitSecurityType,
    UpbitOrderSideHttp,
    UpbitOrderSideWebSocket,
    UpbitOrderType,
    UpbitTimeInForce,
    UpbitOrderStatus,
    UpbitOrderBy,
)
from nautilus_trader.adapters.upbit.common.schemas.exchange import UpbitAsset, UpbitOrder
from nautilus_trader.adapters.upbit.common.symbol import UpbitSymbol
from nautilus_trader.adapters.upbit.http.client import UpbitHttpClient
from nautilus_trader.adapters.upbit.http.endpoint import UpbitHttpEndpoint
from nautilus_trader.core.correctness import PyCondition

from nautilus_trader.model.identifiers import VenueOrderId, ClientOrderId

from nautilus_trader.model.objects import Quantity, Price

import pandas as pd


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

        self.limit_key = "exchange"

    async def get(self) -> list[UpbitAsset]:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, params=None, ratelimiter_keys=[])
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

        self.limit_key = "exchange"

    class GetDeleteParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        uuid: str | None = None
        identifier: str | None = None

    async def get(self, params: GetDeleteParameters) -> UpbitOrder:
        if params.uuid is None and params.identifier is None:
            raise ValueError("Get order without `uuid` and `identifier`!")

        method_type = HttpMethod.GET
        raw = await self._method(method_type, params, ratelimiter_keys=[])
        return self._resp_decoder.decode(raw)

    async def delete(self, params: GetDeleteParameters) -> UpbitOrder:
        if params.uuid is None and params.identifier is None:
            raise ValueError("Delete order without `uuid` and `identifier`!")

        method_type = HttpMethod.DELETE
        raw = await self._method(method_type, params, ratelimiter_keys=[])
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
        self._get_resp_decoder = msgspec.json.Decoder(list[UpbitOrder])

        self.limit_key = "orders"

    class PostParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        market: str
        side: UpbitOrderSideHttp
        ord_type: UpbitOrderType
        volume: str | None = None
        price: str | None = None
        identifier: str | None = None
        time_in_force: UpbitTimeInForce | None = None

    class GetOpenParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        market: str
        limit: int = 100
        order_by: UpbitOrderBy = UpbitOrderBy.DESC
        # TODO: 나중에 구현할 필요가 있을 수도 있음. 지금 인터페이스로는 크게 필요 없어 보임.
        # states: list[UpbitOrderStatus] = [UpbitOrderStatus.WAIT, UpbitOrderStatus.WATCH]
        # page: int = 1

    class GetClosedParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        market: str
        start_time: str | None = None
        end_time: str | None = None
        limit: int = 1000
        order_by: UpbitOrderBy = UpbitOrderBy.DESC
        # TODO: 나중에 구현할 필요가 있을 수도 있음. 지금 인터페이스로는 크게 필요 없어 보임.
        # states: list[UpbitOrderStatus] = [UpbitOrderStatus.CANCEL, UpbitOrderStatus.DONE]

    async def post(self, params: PostParameters) -> UpbitOrder:
        if (
            params.ord_type == UpbitOrderType.LIMIT or params.ord_type == UpbitOrderType.MARKET
        ) and params.volume is None:
            raise ValueError(f"Post `ord_type == {params.ord_type.name}` order without `volume`!")
        if (
            params.ord_type == UpbitOrderType.LIMIT or params.ord_type == UpbitOrderType.PRICE
        ) and params.price is None:
            raise ValueError(f"Post `ord_type == {params.ord_type.name}` order without `price`!")

        method_type = HttpMethod.POST
        raw = await self._method(method_type, params, ratelimiter_keys=[])
        return self._post_resp_decoder.decode(raw)

    async def get_open(self, params: GetOpenParameters) -> list[UpbitOrder]:
        if params.page < 1:
            raise ValueError(f"`page` must be greater or equal than 1, was {params.page}")
        if params.limit < 1 or params.limit > 100:
            raise ValueError(f"`limit` must be in range [1, 100], was {params.limit}")

        method_type = HttpMethod.GET
        raw = await self._method(method_type, params, add_path="open", ratelimiter_keys=[])
        return self._get_resp_decoder.decode(raw)

    async def get_closed(self, params: GetClosedParameters) -> list[UpbitOrder]:
        if params.limit < 1 or params.limit > 1000:
            raise ValueError(f"`limit` must be in range [1, 1000], was {params.limit}")

        method_type = HttpMethod.GET
        raw = await self._method(method_type, params, add_path="closed", ratelimiter_keys=[])
        return self._get_resp_decoder.decode(raw)


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
                uuid=venue_order_id.value if venue_order_id else None,
                identifier=client_order_id.value if client_order_id else None,
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
                uuid=venue_order_id.value,
                identifier=client_order_id.value,
            ),
        )

    async def new_order(
        self,
        market: UpbitSymbol,
        side: UpbitOrderSideHttp,
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

    async def query_orders(
        self,
        market: UpbitSymbol,
        is_open: bool,
        start_time: pd.Timestamp | None = None,
        end_time: pd.Timestamp | None = None,
    ) -> list[UpbitOrder]:
        """
        Query orders information
        """

        if is_open:
            orders = await self._endpoint_orders.get_open(
                params=self._endpoint_orders.GetOpenParameters(
                    market=str(market),
                )
            )

            if start_time or end_time:
                orders_filtered = []
                for order in orders:
                    dt = pd.to_datetime(order.created_at, format="ISO8601")
                    if (start_time is None or start_time <= dt) and (
                        end_time is None or dt <= end_time
                    ):
                        orders_filtered.append(order)
                return orders_filtered
            else:
                return orders
        else:
            return await self._endpoint_orders.get_closed(
                params=self._endpoint_orders.GetClosedParameters(
                    market=str(market),
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                )
            )
