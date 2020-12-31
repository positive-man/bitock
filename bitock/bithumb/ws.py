import logging as _logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from queue import Queue
from typing import *

import websocket

from jsoner import JsonSerializable, deserialize

logger = _logging.getLogger('BithumbWebsocket')

T = TypeVar('T')


class SubscriptionType(Enum):
    TICKER = 'ticker'
    TRANSACTION = 'transaction'
    ORDER_BOOK_DEPTH = 'orderbookdepth'


class TickType(Enum):
    H_HOUR = "30M"
    HOUR = "1H"
    H_DAY = "12H"
    DAY = "24H"
    MID = "MID"


@dataclass
class SubscriptionRequestData(JsonSerializable):
    type: str = None
    symbols: List[AnyStr] = None
    tickTypes: List[AnyStr] = None


@dataclass
class TickerDataContent(JsonSerializable):
    tickType: str = None
    date: str = None
    time: str = None
    openPrice: str = None
    closePrice: str = None
    lowPrice: str = None
    highPrice: str = None
    value: str = None
    volume: str = None
    sellVolume: str = None
    buyVolume: str = None
    prevClosePrice: str = None
    chgRate: str = None
    chgAmt: str = None
    volumePower: str = None
    symbol: str = None


@dataclass
class ApiData(JsonSerializable):
    type: str = None
    content: any = None


@dataclass
class TickerData(ApiData):
    type: str = None
    content: TickerDataContent = None


class WsApi(Generic[T]):
    def __init__(self,
                 subscription_request: SubscriptionRequestData,
                 type_hint: Type[JsonSerializable]):
        self.uri = 'wss://pubwss.bithumb.com/pub/ws'
        self.subscription_request = subscription_request
        self.type_hint = type_hint
        self.ws = websocket.WebSocket()
        self.subscribers: List[Callable[[T]], None] = []
        self.stopped = False

    def subscribe(self, subscriber: Callable[[T], None]):
        self.subscribers.append(subscriber)

    def connect(self):
        logger.info(f'Connecting to Bithumb Websocket API...')
        self.ws.connect(self.uri)
        logger.info(f'Connection Response: {self.ws.recv()}')
        self.ws.send(self.subscription_request.serialize())
        logger.info(f'Subscription Response: {self.ws.recv()}')
        while not self.stopped:
            # 구독자 없으면, 데이터 받지 않음
            if not self.subscribers:
                time.sleep(0.1)
                continue

            received = self.ws.recv()
            data = deserialize(received, self.type_hint)
            for subscriber in self.subscribers:
                subscriber(data)

        self.ws.close()

    def connect_async(self):
        threading.Thread(target=self.connect).start()

    def disconnect(self):
        self.stopped = True

    def get_records(self):
        pass


class TickerApi(WsApi[TickerData]):

    def __init__(self, symbols: List[str], tick_types: List[TickType]):
        sub_req = SubscriptionRequestData(
            type='ticker',
            symbols=symbols,
            tickTypes=[tick_type.value for tick_type in tick_types]
        )

        super().__init__(sub_req, TickerData)


@dataclass
class OrderRequest(JsonSerializable):
    symbol: str = None
    orderType: str = None
    price: str = None
    quantity: str = None
    total: str = None


@dataclass
class OrderBookDepthContent(JsonSerializable):
    datetime: int = None
    list: List[OrderRequest] = None


@dataclass
class OrderBookDepth(JsonSerializable):
    type: str = None
    content: OrderBookDepthContent = None


class OrderBookDepthApi(WsApi[OrderBookDepth]):

    def __init__(self,
                 symbols: List[str],
                 tick_types: List[TickType]):
        sub_req = SubscriptionRequestData(
            type='orderbookdepth',
            symbols=symbols,
            tickTypes=[tick_type.value for tick_type in tick_types]
        )

        super().__init__(sub_req, OrderBookDepth)


@dataclass
class TransactionItem(JsonSerializable):
    symbol: str = None  # 통화코드
    buySellGb: str = None  # 체결종류(1:매도체결, 2:매수체결)
    contPrice: str = None  # 체결가격
    contQty: str = None  # 체결수량
    contAmt: str = None  # 체결금액
    contDtm: str = None  # 체결시각
    updn: str = None  # 업다운: up, dn


@dataclass
class TransactionContent(JsonSerializable):
    list: List[TransactionItem] = None


@dataclass
class Transaction(JsonSerializable):
    type: str = None
    content: TransactionContent = None


class TransactionApi(WsApi[Transaction]):
    def __init__(self,
                 symbols,
                 tick_types: List[TickType],
                 record_limit=100):
        sub_req = SubscriptionRequestData(
            type='transaction',
            symbols=symbols,
            tickTypes=[tick_type.value for tick_type in tick_types]
        )

        super().__init__(sub_req, Transaction)
        self.records: Dict[str, Queue] = {}
        for symbol in symbols:
            self.records.update({symbol: Queue(maxsize=record_limit)})

        def update(transaction: Transaction):
            for item in transaction.content.list:
                self.records.get(item.symbol).put(item)

        self.subscribe(update)


if __name__ == '__main__':
    from pybithumb import Bithumb

    transaction_api = TransactionApi(symbols=[ticker + '_KRW' for ticker in Bithumb.get_tickers()], tick_types=[TickType.HOUR])
    transaction_api.subscribe(lambda x: print(x))
    transaction_api.connect_async()

    order_api = OrderBookDepthApi(symbols=[ticker + '_KRW' for ticker in Bithumb.get_tickers()], tick_types=[TickType.HOUR])
    order_api.subscribe(lambda x: print(x))
    order_api.connect()


