from dataclasses import dataclass, field
from typing import *

from pybithumb import Bithumb

from jsoner import JsonSerializable, deserialize


@dataclass
class PriceQuantity(JsonSerializable):
    quantity: str = None
    price: str = None


@dataclass
class OrderBook(JsonSerializable):
    timestamp: int = 0
    order_currency: str = None
    payment_currency: str = None
    bids: List[PriceQuantity] = field(default_factory=list)
    asks: List[PriceQuantity] = field(default_factory=list)


def get_orderbook(symbol: str, limit=30):
    raw = Bithumb.get_orderbook(symbol, limit=limit)
    orderbooks = deserialize(raw, OrderBook)
    return orderbooks
