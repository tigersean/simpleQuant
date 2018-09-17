# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2018 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import datetime
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Timer

import pandas as pd
from pytdx.hq import TdxHq_API

from simpleQuant.Util import (util_date_stamp, util_date_str2int,
                              util_date_valid, util_get_real_date,
                              util_get_real_datelist, util_get_trade_gap,
                              util_log_info, util_time_stamp,
                              util_web_ping, future_ip_list, stock_ip_list, exclude_from_stock_ip_list, Util_setting,
                              trade_date_sse,SETTINGS, STOCKDATA)
                              
from simpleQuant.Util.Util_date import util_if_tradetime
from simpleQuant.Util.Util_setting import stock_ip_list
from simpleQuant.Util.Util_transform import util_to_json_from_pandas
from simpleQuant.Fetch.base import _select_market_code


"""
准备做一个多连接的连接池执行器Executor
当持续获取数据/批量数据的时候,可以减小服务器的压力,并且可以更快的进行并行处理
"""


class QA_Tdx_Executor():
    def __init__(self, thread_num=2, *args, **kwargs):
        self.thread_num = thread_num
        self._queue = queue.Queue(maxsize=200)
        self.api_no_connection = TdxHq_API()
        self._api_worker = Thread(
             target=self.api_worker, args=(), name='API Worker')
        self._api_worker.start()

        self.executor = ThreadPoolExecutor(self.thread_num)

    def __getattr__(self, item):        
        try:
            api = self.get_available()
            func = api.__getattribute__(item)

            def wrapper(*args, **kwargs):
                res = self.executor.submit(func, *args, **kwargs)
                self._queue.put(api)
                return res
            return wrapper
        except:
            return self.__getattr__(item)

    def _queue_clean(self):
        self._queue = queue.Queue(maxsize=200)

    def _test_speed(self, ip, port=7709,time_out=0.7):

        api = TdxHq_API(raise_exception=True, auto_retry=False)
        _time = datetime.datetime.now()
        try:
            with api.connect(ip, port,time_out):
                _time = datetime.datetime.now()
                if len(api.get_security_list(0, 1)) > 800:
                    return (datetime.datetime.now() - _time).total_seconds()
                else:
                    return datetime.timedelta(9, 9, 0).total_seconds()
        except Exception :
            return datetime.timedelta(9, 9, 0).total_seconds()

    def get_market(self, code):
        code = str(code)
        if code[0] in ['5', '6', '9'] or code[:3] in ["009", "126", "110", "201", "202", "203", "204"]:
            return 1
        return 0

    def get_frequence(self, frequence):
        if frequence in ['day', 'd', 'D', 'DAY', 'Day']:
            frequence = 9
        elif frequence in ['w', 'W', 'Week', 'week']:
            frequence = 5
        elif frequence in ['month', 'M', 'm', 'Month']:
            frequence = 6
        elif frequence in ['Q', 'Quarter', 'q']:
            frequence = 10
        elif frequence in ['y', 'Y', 'year', 'Year']:
            frequence = 11
        elif str(frequence) in ['5', '5m', '5min', 'five']:
            frequence = 0
        elif str(frequence) in ['1', '1m', '1min', 'one']:
            frequence = 8
        elif str(frequence) in ['15', '15m', '15min', 'fifteen']:
            frequence = 1
        elif str(frequence) in ['30', '30m', '30min', 'half']:
            frequence = 2
        elif str(frequence) in ['60', '60m', '60min', '1h']:
            frequence = 3

        return frequence

    @property
    def ipsize(self):
        return len(self._queue.qsize())

    @property
    def api(self):
        return self.get_available()

    def get_available(self):

        if self._queue.empty() is False:
            return self._queue.get_nowait()
        else:
            Timer(0, self.api_worker).start()
            return self._queue.get()

    def api_worker(self):        
        if self._queue.qsize() < 80:
            for item in stock_ip_list:
                _sec = self._test_speed(ip=item['ip'], port=item['port'],time_out=0.7)
                if _sec < 0.15:
                    try:
                        self._queue.put(TdxHq_API(heartbeat=False).connect(
                            ip=item['ip'], port=item['port'],time_out=0.7))
                    except:
                        pass
        else:
            self._queue_clean()
            Timer(0, self.api_worker).start()
        Timer(300, self.api_worker).start()
    

    def get_realtime_concurrent(self, code):
        code = [code] if type(code) is str else code

        try:
            data = {self.get_security_quotes([(self.get_market(
                x), x) for x in code[80 * pos:80 * (pos + 1)]]) for pos in range(int(len(code) / 80) + 1)}
            return (pd.concat([self.api_no_connection.to_df(i.result()) for i in data]), datetime.datetime.now())
        except:
            pass

                
    def fetch_get_stock_day(self, code, start_date, end_date, frequence='day'):            
            start_date = str(start_date)[0:10]
            end_date = str(end_date)[0:10]
            today_ = datetime.date.today()
            lens = util_get_trade_gap(start_date, today_) 
                       
            data =self.get_security_bars_concurrent(code=code, _type=self.get_frequence(frequence), lens=lens)           
            # 这里的问题是: 如果只取了一天的股票,而当天停牌, 那么就直接返回None了
            if len(data) < 1:
                return None

            data = data[data['open'] != 0]

            data = data.assign(date=data['datetime'].apply(lambda x: str(x[0:10])),
                               code=str(code),
                               date_stamp=data['datetime'].apply(lambda x: util_date_stamp(str(x)[0:10])))\
                .set_index('date', drop=False, inplace=False)            
            data = data.drop(['year', 'month', 'day', 'hour', 'minute', 'datetime','date_stamp'], axis=1)[
                start_date:end_date]            
            return data


    def get_security_bars_concurrent(self, code, _type, lens):
        try:
            # data = {self.get_security_quotes([(self.get_market(
            #     x), x) for x in code[80 * pos:80 * (pos + 1)]]) for pos in range(int(len(code) / 80) + 1)}
            # data = pd.concat([api.to_df(api.get_security_bars(frequence, _select_market_code(
            #     code), code, (int(lens / 800) - i) * 800, 800)) for i in range(int(lens / 800) + 1)], axis=0)
            data = {(self.get_security_bars(_type, _select_market_code(
                code), code, (int(lens / 800) - i) * 800, 800)) for i in range(int(lens / 800) + 1)}
            return pd.concat([self.api_no_connection.to_df(i.result()) for i in data])
        except:
            pass

    def save_hdf(self):
        pass 

if __name__ == '__main__':
    import time
    import sys
    _time1 = datetime.datetime.now()  

    # DATABASE.realtime.create_index([('code', QA_util_sql_mongo_sort_ASCENDING),
    #                                 ('datetime', QA_util_sql_mongo_sort_ASCENDING)])

    # print(len(code))
    x = QA_Tdx_Executor()   
    print(x.fetch_get_stock_day('600000','2018-09-01','2018-09-09','day'))  
    print(x.get_realtime_concurrent('600000'))  
    sys.exit(0)