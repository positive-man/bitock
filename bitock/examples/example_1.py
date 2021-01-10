"""
체결 강도 및 매수매도 주문량 기반 알고리즘
"""

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
        self.path = os.path.join('../records', f'{self.name}-{now_str()}.csv')
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


# 음...
# 받아와서 큐에 넣어
# 빼는 놈을 따로 만들어...


class VolumnPowerBasedWorker(Worker):
    def __init__(self, symbol, reverse=False):
        self.symbol = symbol
        self.holding = False
        self.name = symbol
        self.reverse = reverse
        self.buy_price = 0

    def on_received(self, data: bithumb_websock.TickerData):
        orderbooks = bithumb_rest.get_orderbook(self.symbol)
        data_created = datetime(year=int(data.content.date[:4]),
                                month=int(data.content.date[4:6]),
                                day=int(data.content.date[6:8]),
                                hour=int(data.content.time[:2]),
                                minute=int(data.content.time[2:4]),
                                second=int(data.content.time[4:6]))
        if not orderbooks:
            return

        sum_of_asks = sum([float(ask.quantity) for ask in orderbooks.asks])  # 팔고 싶은 애들
        sum_of_bids = sum([float(bid.quantity) for bid in orderbooks.bids])  # 사고 싶은 애들

        detail = {
            'value': data.content.value,
            'volume_power': data.content.volumePower,
            'sum_of_asks': sum_of_asks,
            'sum_of_bids': sum_of_bids,
            'data_created': data_created,
            'max_of_bids': max([bid.price for bid in orderbooks.bids]),
            'min_of_asks': min([ask.price for ask in orderbooks.asks])
        }

        if self.holding:  # 가지고 있으면
            max_of_bids = max(float(bid.price) for bid in orderbooks.bids)
            min_of_asks = min(float(ask.price) for ask in orderbooks.asks)

            # 현재 수익률
            return_rate = (max_of_bids / self.buy_price - 1) * 100
            if return_rate > 2 or return_rate < -2:
                if abs((max_of_bids / min_of_asks - 1) * 100) > 2:
                    # 매수/메도 갭 2% 이상
                    logger.warning(f'max_of_bids({max_of_bids}), min_of_asks({min_of_asks}) 갭 2% 이상')
                    return
                else:
                    self.sell(max_of_bids, detail)
        elif float(data.content.volumePower) > 110 and sum_of_asks > sum_of_bids * 1.1:
            if float(data.content.value) > 3000_0000:
                self.buy(min([float(ask.price) for ask in orderbooks.asks]), detail)

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
    input()


if __name__ == '__main__':
    main()
