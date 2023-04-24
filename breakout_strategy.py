import locale
import backtrader as bt
import math
from openpyxl import Workbook
import sqlite3
import pandas as pd
import datetime

locale.setlocale(locale.LC_ALL, 'ko_KR')

global excel_trade

wb = Workbook()

excel_trade = wb.active
excel_trade.append(['날짜', '수량', '가격', '수수료', '내용', '롱/숏', '범위', '타겟', 'ROR', '손익', '누적수익률', '최대 낙폭', '고점 대비'])

con_m = sqlite3.connect("./minute1.db")
con_d = sqlite3.connect("./day1.db")

class MyStrategy(bt.Strategy):
    def __init__(self):

        self.add_timer(name="sell", when=datetime.time(8,59))
        self.add_timer(name="reset", when=datetime.time(9,00))

        self.dataclose = self.datas[0].close # 종가
        self.datalow = self.datas[0].low # 저가
        self.datahigh = self.datas[0].high
        self.dataopen = self.datas[0].open
        self.datavolume = self.datas[0].volume

        # self.SMA5 = bt.indicators.MovingAverageSimple(self.data1, period=5) # 5일 이평선
        # self.SMA5 = bt.indicators.MovingAverageSimple(self.data1, period=5) # 5일 이평선
        # self.SMA8 = bt.indicators.MovingAverageSimple(self.data1, period=8) # 8일 이평선
        # self.SMA18 = bt.indicators.MovingAverageSimple(self.data1, period=18) # 18일 이평선
        # self.SMA20 = bt.indicators.MovingAverageSimple(self.data1, period=20) # 20일 이평선
        # self.SMA25 = bt.indicators.MovingAverageSimple(self.data1, period=25) # 25일 이평선
        self.SMA65 = bt.indicators.MovingAverageSimple(self.data1, period=65) # 65일 이평선

        self.pre_value = 0
        self.holding = 0
        self.buy_num = 0
        self.buy_order = []

        self.target = 0
        self.short_target = 0
        self.range = 0
        self.short_range = 0

        self.signal = 0

        self.max_low = 0
        self.max_value = 10000000

        self.sma5 = 0
        self.sma8 = 0
        self.sma20 = 0
        self.sma25 = 0
        self.sma65 = 0

        # self.sma2 = 0
        # self.sma6 = 0
        # self.sma18 = 0

        self.lev = 10
        self.short_lev = 3

    def log(self, size=None, price=None, comm="", text="", ls="", range="", target="", ror="", output="", hpr="", max_low="", mdd=""):

        date = self.datas[0].datetime.datetime(0)
        
        excel_trade.append([date, size, price, comm, text, ls, range, target, ror, output, hpr, max_low, mdd])
        print(f'{date} {size}개 {price}원 {comm} {text} {ls} {range} {target} {ror} {output} {hpr} {max_low} {mdd}')

    def log_print(self):
        ref = len(self)
        date = self.datas[0].datetime.datetime(0)
        open = self.dataopen[0]
        low = self.datalow[0]
        close = self.dataclose[0]
        high = self.datahigh[0]
        print(f'[{ref}] [날짜 : {date}] [시가 : {open}] [고가 : {high}] [저가 : {low}] [종가 : {close}]')

    def next(self):

        _, isowk, isowkday = self.datetime.date().isocalendar()
        txt = '{}, {}, Week {}, Day {}, O {}, H {}, L {}, C {}'.format(
            len(self), self.datetime.datetime(),
            isowk, isowkday,
            self.data.open[0], self.data.high[0],
            self.data.low[0], self.data.close[0])

        # print(txt)

        sma5 = (self.sma5 + self.dataopen[0])/5
        sma8 = (self.sma8 + self.dataopen[0])/8
        sma20 = (self.sma20 + self.dataopen[0])/20
        sma25 = (self.sma25 + self.dataopen[0])/25
        sma65 = (self.sma65 + self.dataopen[0])/65
        
        # sma2 = (self.sma2 + self.dataopen[0])/2
        # sma6 = (self.sma6 + self.dataopen[0])/6
        # sma18 = (self.sma18 + self.dataopen[0])/18

        if len(self.buy_order) == 0 and self.holding == 0 and self.target > 0:

            if (self.datahigh[0] > self.target) and (sma5 < self.target or sma8 < self.target or sma20 < self.target):
            # if (self.datahigh[0] > self.target) and (sma2 < self.target):
                self.pre_value = self.broker.getvalue()
                self.buy(size=math.floor((self.broker.getcash()*self.lev/2)/self.dataclose[0]/(1+0.0004*self.lev)))
                print(self.data.datetime.datetime(), self.dataclose[0])
                self.signal = 1

            elif (self.datalow[0] < self.short_target) and (self.short_target < sma25 or self.short_target < sma65):
            # elif (self.datalow[0] < self.short_target) and (self.short_target < sma6 or self.short_target < sma18):
                self.pre_value = self.broker.getvalue()
                self.sell(size=math.floor((self.broker.getcash()*self.short_lev/2)/self.dataclose[0]/(1+0.0004*self.short_lev)))
                self.signal = -1

        elif self.holding > 0:
            low_rate = (self.datalow[0] - self.buy_price)/self.buy_price*100*self.lev
            if low_rate < self.max_low:
                self.max_low = low_rate
        elif self.holding < 0:
            low_rate = (self.buy_price - self.datahigh[0])/self.buy_price*100*self.short_lev
            if low_rate < self.max_low:
                self.max_low = low_rate

    def notify_timer(self, timer, when, *args, **kwargs):

            if kwargs['name'] == 'sell':
                if self.holding > 0:
                    print('-- {} Create sell order'.format(self.data.datetime.datetime()))
                    self.sell(size=self.holding)
                elif self.holding < 0:
                    print('-- {} Create buy order'.format(self.data.datetime.datetime()))
                    self.buy(size=self.holding)
            else:
                self.sma5 = 0
                self.sma8 = 0
                self.sma20 = 0
                self.sma25 = 0
                self.sma65 = 0

                # self.sma2 = 0
                # self.sma6 = 0
                # self.sma18 = 0

                for i in range(-64,0):
                    self.sma65 += self.data1.close[i]
                    if i > -25:
                        self.sma25 += self.data1.close[i]
                    if i > -20:
                        self.sma20 += self.data1.close[i]
                    if i > -8:
                        self.sma8 += self.data1.close[i]
                    if i > -5:
                        self.sma5 += self.data1.close[i]

                # for i in range(-17,0):
                #     self.sma18 += self.data1.close[i]
                #     if i > -6:
                #         self.sma6 += self.data1.close[i]
                #     if i > -2:
                #         self.sma2 += self.data1.close[i]

                # print('-- {} Reset high and low'.format(self.data.datetime.datetime()))
                self.range = (self.data1.high[-1] - self.data1.low[-1])*0.65
                self.short_range = (self.data1.high[-1] - self.data1.low[-1])*0.6
                # self.range = (self.data1.high[-1] - self.data1.low[-1])*0.6
                # self.short_range = (self.data1.high[-1] - self.data1.low[-1])*0.65
                self.target = self.data1.open[0] + self.range
                self.short_target = self.data1.open[0] - self.short_range


        # print('strategy notify_timer with when {}'.format(when))
        
    def notify_trade(self, trade):

        if trade.isclosed:
            print(','.join(map(str, [
                'TRADE', 'CLOSE',
                trade.value,
                trade.pnl,
                trade.commission,
            ]
            )))
        elif trade.justopened:
            print(','.join(map(str, [
                'TRADE', 'OPEN',
                trade.value,
                trade.pnl,
                trade.commission,
            ]
            )))

    def notify_order(self, order):

        if order.status in [order.Submitted]:
            if order.isbuy():
                print(self.data.datetime.datetime(0), 'BUY Submitted',order.size)
                if self.signal == 1:
                    self.buy_order.append(order)
            else:
                print(self.data.datetime.datetime(0), 'SELL Submitted',order.size)
                if self.signal == -1:
                    self.buy_order.append(order)

        elif order.status in [order.Margin, order.Rejected, order.Canceled]:
            print('ORDER FAILED with status:', order.getstatusname())

        elif order.status in [order.Completed]:
            if order.isbuy():
                    if self.signal == 1:
                        self.holding += order.size

                        # 매수 성공시
                        if self.buy_num == 0:
                            self.buy_num = 1
                        self.log(size=order.executed.size, price=order.executed.price, comm=order.executed.comm, text="매수성공", ls="long", range=self.range, target=self.target)

                        self.buy_price = order.executed.price
                        self.buy_order.clear()

                    elif self.signal == -1:
                        if self.max_value < self.broker.getvalue():
                            self.max_value = self.broker.getvalue()
                        mdd = (self.broker.getvalue() - self.max_value) / self.max_value*100

                        ror = self.broker.getvalue() / self.pre_value
                        output = round(((self.broker.getvalue() - self.pre_value) / self.pre_value)*100,2)
                        hpr = round((self.broker.getvalue() / 10000000),2)

                        if output > 0:
                            self.log(size=order.executed.size, price=order.executed.price, comm=order.executed.comm, text="익절성공", ls="short", ror=ror ,output=str(output)+'%', hpr=hpr, max_low=self.max_low, mdd=mdd)
                        else:
                            self.log(size=order.executed.size, price=order.executed.price, comm=order.executed.comm, text="손절발생", ls="short", ror=ror ,output=str(output)+'%', hpr=hpr, max_low=self.max_low, mdd=mdd)

                        self.buy_num = 0
                        self.holding = 0
                        self.pre_value = 0
                        self.buy_price = 0
                        self.signal = 0
                        self.max_low = 0


            elif order.issell():
                    if self.signal == -1:
                        self.holding += order.size

                        # 매수 성공시
                        if self.buy_num == 0:
                            self.buy_num = 1
                        self.log(size=order.size, price=order.executed.price, comm=order.executed.comm, text="매수성공", ls="short", range=self.short_range, target=self.short_target)

                        self.buy_price = order.executed.price
                        self.buy_order.clear()

                    elif self.signal == 1:
                        if self.max_value < self.broker.getvalue():
                            self.max_value = self.broker.getvalue()
                        mdd = (self.broker.getvalue() - self.max_value) / self.max_value*100

                        ror = self.broker.getvalue() / self.pre_value
                        output = round(((self.broker.getvalue() - self.pre_value) / self.pre_value)*100,2)
                        hpr = round((self.broker.getvalue() / 10000000),2)

                        if output > 0:
                            self.log(size=order.executed.size, price=order.executed.price, comm=order.executed.comm, text="익절성공", ls="long", ror=ror ,output=str(output)+'%', hpr=hpr, max_low=self.max_low, mdd=mdd)
                        else:
                            self.log(size=order.executed.size, price=order.executed.price, comm=order.executed.comm, text="손절발생", ls="long", ror=ror ,output=str(output)+'%', hpr=hpr, max_low=self.max_low, mdd=mdd)

                        self.buy_num = 0
                        self.holding = 0
                        self.pre_value = 0
                        self.buy_price = 0
                        self.signal = 0
                        self.max_low = 0
                
if __name__ == "__main__":
    cerebro = bt.Cerebro()
    cerebro.addstrategy(MyStrategy)
    cerebro.broker.setcash(10000000)
    cerebro.broker.setcommission(commission=0.0004, leverage=10)
    cerebro.broker.set_coc(True)

    df = pd.read_sql("SELECT * FROM bitcoin1m", con_m, index_col='Date', parse_dates=['Date'])
    df1 = pd.read_sql("SELECT * FROM bitcoin1d", con_d, index_col='Date', parse_dates=['Date'])
    data = bt.feeds.PandasData(
        dataname=df,
        timeframe=bt.TimeFrame.Minutes
    )
    data1 = bt.feeds.PandasData(dataname=df1)
    cerebro.adddata(data)
    cerebro.adddata(data1)

    cerebro.run()  # run it all

    wb.save('./bitcoin/breakout_strategy/lev10_2_1.xlsx')
    # wb.save('./bitcoin/breakout_strategy/eth5_2.xlsx')

    print("done!")