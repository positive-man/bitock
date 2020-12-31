import abc
import csv
import dataclasses
import logging as _logging
import os
import threading
import time
from datetime import datetime
from enum import Enum
from queue import Queue
from typing import *

from pybithumb import Bithumb

import bithumb.rest as bithumb_rest
import bithumb.ws as bithumb_websock

_logging.basicConfig(level=_logging.INFO, format='[%(asctime)s] %(message)s')
logger = _logging.getLogger('main')
TICKERS = Bithumb.get_tickers()
KRW = 'KRW'
TICKERS_WITH_KRW = [f'{ticker}_{KRW}' for ticker in TICKERS]

STRENGTH_THRESHOLD = 120
BUY_SELL_THRESHOLD = 1.2


def now_str() -> str:
    return datetime.now().strftime('%Y%m%d_%H%M%S')


class OrderType(Enum):
    BUY = 'BUY'
    SELL = 'SELL'


@dataclasses.dataclass
class Order:
    timestamp: str
    symbol: str
    order_type: str
    price: float
    buy_price: float
    return_rate: float
    details: str


class OrderLogger:
    def __init__(self, name):
        self.name = name
        self.path = os.path.join('records', f'{self.name}-{now_str()}.csv')
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.queue = Queue()
        self.queue.put(list(Order.__annotations__.keys()))

        def flush():
            while True:
                with open(self.path, 'a', encoding='utf-8', newline='') as f:
                    while not self.queue.empty():
                        csv.writer(f).writerow(self.queue.get())

                time.sleep(1)

        threading.Thread(target=flush).start()

    def log(self, order: Order):
        logger.info(order)
        self.queue.put(list(order.__dict__.values()))


order_logger = OrderLogger('GLOBAL')


class Worker(abc.ABC):
    @abc.abstractmethod
    def on_received(self, data: bithumb_websock.TickerData):
        pass


class VolumnPowerBasedWorker(Worker):
    def __init__(self, symbol, reverse=False):
        self.symbol = symbol
        self.holding = False
        self.name = symbol
        self.reverse = reverse
        self.buy_price = 0

    def on_received(self, data: bithumb_websock.TickerData):
        # 체결 강도만 가지고 해보자

        if 90 < float(data.content.volumePower) < 110:
            return
        else:
            orderbook = bithumb_rest.get_orderbook(self.symbol)
            if float(data.content.volumePower) > 110:
                self.buy(min([float(ask.price) for ask in orderbook.asks]), None)
            elif float(data.content.volumePower) < 90:
                self.sell(max(float(bid.price) for bid in orderbook.bids), None)

        # if float(data.content.volumePower) < STRENGTH_THRESHOLD:
        #     return
        #
        # orderbooks = bithumb_rest.get_orderbook(self.symbol)
        # if orderbooks is None:
        #     return
        #
        # sum_of_asks = sum([float(ask.quantity) for ask in orderbooks.asks])  # 팔고 싶은 애들
        # sum_of_bids = sum([float(bid.quantity) for bid in orderbooks.bids])  # 사고 싶은 애들
        #
        # log = {
        #     'sum_of_asks': sum_of_asks,
        #     'sum_of_bids': sum_of_bids,
        #     'ticker': data,
        # }
        #
        # if sum_of_asks / sum_of_bids > BUY_SELL_THRESHOLD:
        #     self.buy(min([float(ask.price) for ask in orderbooks.asks]), log)
        # if sum_of_bids / sum_of_asks > BUY_SELL_THRESHOLD:
        #     self.sell(max(float(bid.price) for bid in orderbooks.bids), log)

    def buy(self, price: float, details: any):
        if self.holding:
            return
        else:
            self.holding = True

        self.buy_price = price
        order_logger.log(Order(
            timestamp=now_str(),
            symbol=self.symbol,
            order_type=OrderType.BUY.value,
            price=price,
            buy_price=0,
            return_rate=0,
            details=details
        ))

    def sell(self, price: float, details: any):
        if self.holding:
            self.holding = False
        else:
            return

        order_logger.log(Order(
            timestamp=now_str(),
            symbol=self.symbol,
            order_type=OrderType.SELL.value,
            price=price,
            buy_price=self.buy_price,
            return_rate=(price / self.buy_price - 1) * 100,
            details=details
        ))

        self.buy_price = 0


class WorkerManager(Worker):

    def __init__(self):
        self.workers: List[VolumnPowerBasedWorker] = []

    def add_worker(self, worker: VolumnPowerBasedWorker):
        self.workers.append(worker)

    def on_received(self, data: bithumb_websock.TickerData):
        for worker in self.workers:
            if worker.symbol == data.content.symbol:
                worker.on_received(data)


def main():
    worker_manager = WorkerManager()
    for symbol in TICKERS_WITH_KRW:
        worker_manager.add_worker(
            VolumnPowerBasedWorker(symbol=symbol)
        )

    ticker_api = bithumb_websock.TickerApi(TICKERS_WITH_KRW, [bithumb_websock.TickType.H_HOUR])
    ticker_api.subscribe(worker_manager.on_received)
    ticker_api.connect()


if __name__ == '__main__':
    main()
