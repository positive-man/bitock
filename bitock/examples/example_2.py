import argparse
import logging
import logging.handlers
from datetime import datetime, timedelta
from enum import Enum
from multiprocessing.pool import ThreadPool

from pandas import DataFrame
from pybithumb import Bithumb
import threading

PAYMENT_CURRENCY = 'KRW'

TICKERS = Bithumb.get_tickers(PAYMENT_CURRENCY)

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FORMAT = '%(asctime)s, %(levelname)s, %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--title', required=True)
arg_parser.add_argument('--tick', required=True)
args = arg_parser.parse_args()


class FileLogFormatter(logging.Formatter):
    def __init__(self):
        # noinspection SpellCheckingInspection
        logging.Formatter.__init__(self,
                                   fmt=LOG_FORMAT)


file_handler = logging.handlers.RotatingFileHandler(
    filename=args.title + '.log.csv',
    encoding='utf-8',
    maxBytes=4 * 1024 * 1024,
    backupCount=2)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(FileLogFormatter())
logger = logging.getLogger()
logger.handlers.append(file_handler)


def _current_price(ticker):
    """
    체결 정보 로드하고 최근 1분으로 필터링하고 평균을 구하여 리턴함
    """

    def parse_date(s: str):
        try:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')
        except:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

    transactions: list = Bithumb.get_transaction_history(ticker)
    now = datetime.now()
    transactions = list(
        filter(
            lambda transaction: now - parse_date(transaction.get('transaction_date')) < timedelta(minutes=1),
            transactions
        )
    )

    total = sum([transaction.get('price') for transaction in transactions])
    count = len(transactions)
    return total / count


class Decision(Enum):
    BUY = 'BUY',
    SELL = 'SELL'
    NONE = 'NONE'


class Simulator:

    def __init__(self, ticker):
        self.ticker = ticker
        self.holding = False
        self.buy_price = 0

    def decide(self) -> [Decision, float]:
        candlesticks: DataFrame = Bithumb.get_candlestick(order_currency=self.ticker, chart_instervals=args.tick)
        close_list = candlesticks.get('close').values.tolist()

        # 현재가(마지막 스틱의 종가)
        cur_price = close_list[-1]
        # 5 평균
        cur_ma_5 = sum(close_list[-5:]) / 5
        # 20 평균
        cur_ma_20 = sum(close_list[-20:]) / 20
        # 전 가격
        prv_price = close_list[-2]
        # 전 5 평균
        prv_ma_5 = sum(close_list[-6:-1]) / 5
        # 전 20 평균
        prv_ma_20 = sum(close_list[-21:-1]) / 20

        decision = None

        if not self.holding and cur_price > cur_ma_20 and prv_price < prv_ma_20:
            decision = Decision.BUY
            self.holding = True
            self.buy_price = cur_price
        elif self.holding:
            if cur_price < cur_ma_5 and prv_price > prv_ma_5:
                decision = Decision.SELL
                self.holding = False

        if decision:
            return_rate = (cur_price / self.buy_price - 1) * 100
            elements = [
                decision,
                self.ticker,
                return_rate,
                self.buy_price,
                cur_price,
                cur_ma_5,
                cur_ma_20,
                prv_price,
                prv_ma_5,
                prv_ma_20
            ]
            msg = ', '.join(
                [str(element) for element in elements]
            )

            logging.info(msg)


def main():
    simulators = []
    for ticker in TICKERS:
        simulators.append(Simulator(ticker))

    while True:
        delay_looker = threading.Timer(15, lambda : logging.warning('DELAY OCCURS'))
        delay_looker.start()
        with ThreadPool(processes=4) as pool:
            def run(sml):
                try:
                    sml.decide()
                except BaseException as e:
                    logging.warning(str(e))

            pool.map(run, simulators)
        delay_looker.cancel()

if __name__ == '__main__':
    main()
