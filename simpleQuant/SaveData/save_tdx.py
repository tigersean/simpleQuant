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

import concurrent
import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from dateutil.parser import parse
import pandas as pd


from simpleQuant.Fetch import fetch_get_stock_block
from simpleQuant.Fetch.base import _select_market_code
from simpleQuant.Fetch.tdx_fetch import (
    fetch_get_option_day,
    fetch_get_index_day,
    fetch_get_index_min,
    fetch_get_stock_day,
    fetch_get_stock_info,
    fetch_get_stock_list,
    fetch_get_future_list,
    fetch_get_index_list,
    fetch_get_future_day,
    fetch_get_future_min,
    fetch_get_stock_min,
    fetch_get_stock_transaction,
    fetch_get_stock_xdxr, select_best_ip)
from simpleQuant.Fetch.tdx_fetch import (
    fetch_get_50etf_option_contract_time_to_market)
from simpleQuant.Util import (STOCKDATA, util_get_next_day,
                              util_get_real_date, util_log_info,
                              util_to_json_from_pandas, trade_date_sse)

# ip=select_best_ip()

frequenceDict={'day':'D',
               'week':'W',
               'month':'M',
               '1min':'M1',
               '5min':'M5',
               '15min':'M15',
               '30min':'M30',
               '60min':'M60'}
def now_time():
    return str(util_get_real_date(str(datetime.date.today() - datetime.timedelta(days=1)), trade_date_sse, -1)) + \
        ' 17:00:00' if datetime.datetime.now().hour < 15 else str(util_get_real_date(
            str(datetime.date.today()), trade_date_sse, -1)) + ' 15:00:00'

def _sel_market_code(code):
    code = str(code)
    if code[0] in ['5', '6', '9'] or code[:3] in ["009", "126", "110", "201", "202", "203", "204"]:
        return 'SH'
    return 'SZ'


def _save_stock_data(client=STOCKDATA, stock_list=None, ui_log=None, ui_progress=None, frequence='day'):
    if stock_list is None: 
        stock_list = fetch_get_stock_list().code.unique().tolist()
    data_cli = client
    err = []
    
    def __saving_work_job(code, tb, frequence):
        
        try:
            util_log_info(
                '##JOB01 Now Saving STOCK_'+ frequence +'==== {}'.format(str(code)), ui_log)
            # 首选查找数据库 是否 有 这个代码的数据
            tb.create_table(_sel_market_code(code)+str(code))
            end_date = str(now_time())[0:10]
            # 当前数据库已经包含了这个代码的数据， 继续增量更新
            # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现
            if tb.nrows > 0:
                # 接着上次获取的日期继续更新
                start_date = tb[-1]['datetime']
                start_date=util_get_next_day(parse(str(start_date)).date().strftime('%Y-%m-%d'))
                util_log_info('UPDATE_STOCK_'+ frequence+' \n Trying updating {} from {} to {}'.format(
                                 code, start_date, end_date),  ui_log)
            # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
            else:
                start_date = '1990-01-01'
                util_log_info('UPDATE_STOCK_'+frequence +'\n Trying updating {} from {} to {}'.format
                                 (code, start_date, end_date),  ui_log)
            if (start_date < end_date):                
                __data=fetch_get_stock_day(str(code), start_date, end_date, '00', frequence)                
                if (__data is None)==False: 
                    if len(__data)>0 :                        
                        tb.save_data(__data)
        except Exception as error0:
            if (error0.__str__() !='ERROR CODE'):
                print(error0)
                err.append(str(code))

    for item in range(len(stock_list)):
        util_log_info('The {} of Total {}'.format
                         (item, len(stock_list)))

        strProgressToLog = 'DOWNLOAD PROGRESS {} {}'.format(
            str(float(item / len(stock_list) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float(item / len(stock_list) * 100))
        util_log_info(strProgressToLog, ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=intProgressToLog)

        __saving_work_job(stock_list[item], 
                     data_cli[_sel_market_code(stock_list[item])+frequenceDict[frequence]],frequence)

    if len(err) < 1:
        util_log_info('SUCCESS save stock '+ frequence +' ^_^',  ui_log)
    else:
        util_log_info(' ERROR CODE \n ',  ui_log)
        util_log_info(err, ui_log)


def SU_save_stock_xdxr(client=STOCKDATA, ui_log=None, ui_progress=None):
    """[summary]

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """
    stock_list = fetch_get_stock_list().code.unique().tolist()
    #client.drop_collection('stock_xdxr')
    try:
        
        coll = client.stock_xdxr
        coll.create_index([('code', pymongo.ASCENDING),
                        ('date', pymongo.ASCENDING)], unique=True)
    except:
        client.drop_collection('stock_xdxr')
        coll = client.stock_xdxr
        coll.create_index([('code', pymongo.ASCENDING),
                        ('date', pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, coll):
        util_log_info('##JOB02 Now Saving XDXR INFO ==== {}'.format(
            str(code)), ui_log=ui_log)
        try:
            coll.insert_many(
                util_to_json_from_pandas(
                    fetch_get_stock_xdxr(str(code))), ordered=False)

        except:

            err.append(str(code))
    for i_ in range(len(stock_list)):
        util_log_info('The {} of Total {}'.format(
            i_, len(stock_list)), ui_log=ui_log)
        strLogInfo = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(stock_list) * 100))[0:4] + '%')
        intLogProgress = int(float(i_ / len(stock_list) * 100))
        util_log_info(strLogInfo, ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=intLogProgress)
        __saving_work(stock_list[i_], coll)


def _save_stock_min(client=STOCKDATA, stock_list=None, ui_log=None, ui_progress=None):
    """save stock_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """
    if stock_list is None :
        stock_list = fetch_get_stock_list().code.unique().tolist()
    coll = client
    err = []

    def __saving_work(code, tb):
        util_log_info(
            '##JOB03 Now Saving STOCK_MIN ==== {}'.format(str(code)), ui_log=ui_log)
        try:
            for type in ['1min', '5min', '15min', '30min', '60min']:
                cli=tb[_sel_market_code(code)+frequenceDict[type]]\
                         .create_table(_sel_market_code(code)+str(code))
                end_time = str(now_time())[0:19]

                if cli.count() > 0:
                    start_time = cli[-1]['datetime']

                    util_log_info(
                        '##JOB03.{} Now Saving {} from {} to {} =={} '.format(['1min', '5min', '15min', '30min', '60min'].index(type),
                                                                              str(code), start_time, end_time, type),
                        ui_log=ui_log)
                    if start_time != end_time:
                        __data = fetch_get_stock_min(
                            str(code), start_time, end_time, type)
                        if len(__data) > 1:
                            cli.insert_many(__data[1::])
                else:
                    start_time = '2015-01-01'
                    util_log_info(
                        '##JOB03.{} Now Saving {} from {} to {} =={} '.format(['1min', '5min', '15min', '30min', '60min'].index(type),
                                                                              str(code), start_time, end_time, type),
                        ui_log=ui_log)
                    if start_time != end_time:
                        __data = fetch_get_stock_min(
                            str(code), start_time, end_time, type)
                        if len(__data) > 1:
                            cli.save_data(__data)
        except Exception as e:
            util_log_info(e, ui_log=ui_log)
            err.append(code)
            util_log_info(err, ui_log=ui_log)

    executor = ThreadPoolExecutor(max_workers=4)
    #executor.map((__saving_work,  stock_list[i_], coll),URLS)
    res = {executor.submit(
        __saving_work,  stock_list[i_], coll) for i_ in range(len(stock_list))}
    count = 0
    for i_ in concurrent.futures.as_completed(res):
        util_log_info('The {} of Total {}'.format(
            count, len(stock_list)), ui_log=ui_log)

        strProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(count / len(stock_list) * 100))[0:4] + '%')
        intProgress = int(count / len(stock_list) * 10000.0)
        util_log_info(strProgress, ui_log, ui_progress=ui_progress,
                         ui_progress_int_value=intProgress)
        count = count + 1
    if len(err) < 1:
        util_log_info('SUCCESS', ui_log=ui_log)
    else:
        util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        util_log_info(err, ui_log=ui_log)


def SU_save_index_day(client=STOCKDATA, ui_log=None, ui_progress=None):
    """save index_day

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """

    __index_list = fetch_get_stock_list('index')
    coll = client.index_day
    coll.create_index([('code', pymongo.ASCENDING),
                       ('date_stamp', pymongo.ASCENDING)])
    err = []

    def __saving_work(code, coll):

        try:
            ref_ = coll.find({'code': str(code)[0:6]})
            end_time = str(now_time())[0:10]
            if ref_.count() > 0:
                start_time = ref_[ref_.count() - 1]['date']

                util_log_info('##JOB04 Now Saving INDEX_DAY==== \n Trying updating {} from {} to {}'.format
                                 (code, start_time, end_time), ui_log=ui_log)

                if start_time != end_time:
                    coll.insert_many(
                        util_to_json_from_pandas(
                            fetch_get_index_day(str(code), util_get_next_day(start_time), end_time)))
            else:
                try:
                    start_time = '1990-01-01'
                    util_log_info('##JOB04 Now Saving INDEX_DAY==== \n Trying updating {} from {} to {}'.format
                                     (code, start_time, end_time), ui_log=ui_log)
                    coll.insert_many(
                        util_to_json_from_pandas(
                            fetch_get_index_day(str(code), start_time, end_time)))
                except:
                    start_time = '2009-01-01'
                    util_log_info('##JOB04 Now Saving INDEX_DAY==== \n Trying updating {} from {} to {}'.format
                                     (code, start_time, end_time), ui_log=ui_log)
                    coll.insert_many(
                        util_to_json_from_pandas(
                            fetch_get_index_day(str(code), start_time, end_time)))
        except Exception as e:
            util_log_info(e, ui_log=ui_log)
            err.append(str(code))
            util_log_info(err, ui_log=ui_log)

    for i_ in range(len(__index_list)):
        # __saving_work('000001')
        util_log_info('The {} of Total {}'.format(
            i_, len(__index_list)), ui_log=ui_log)

        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(__index_list) * 100))[0:4] + '%')
        intLogProgress = int(float(i_ / len(__index_list) * 10000.0))
        util_log_info(strLogProgress, ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=intLogProgress)
        __saving_work(__index_list.index[i_][0], coll)
    if len(err) < 1:
        util_log_info('SUCCESS', ui_log=ui_log)
    else:
        util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        util_log_info(err, ui_log=ui_log)


def SU_save_index_min(client=STOCKDATA, ui_log=None, ui_progress=None):
    """save index_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """

    __index_list = fetch_get_stock_list('index')
    coll = client.index_min
    coll.create_index([('code', pymongo.ASCENDING), ('time_stamp',
                                                     pymongo.ASCENDING), ('date_stamp', pymongo.ASCENDING)])
    err = []

    def __saving_work(code, coll):

        util_log_info(
            '##JOB05 Now Saving Index_MIN ==== {}'.format(str(code)), ui_log=ui_log)
        try:

            for type in ['1min', '5min', '15min', '30min', '60min']:
                ref_ = coll.find(
                    {'code': str(code)[0:6], 'type': type})
                end_time = str(now_time())[0:19]
                if ref_.count() > 0:
                    start_time = ref_[ref_.count() - 1]['datetime']

                    util_log_info(
                        '##JOB05.{} Now Saving {} from {} to {} =={} '.
                        format(['1min', '5min', '15min', '30min', '60min'].
                               index(type), str(code), start_time, end_time, type),
                        ui_log=ui_log)

                    if start_time != end_time:
                        __data = fetch_get_index_min(
                            str(code), start_time, end_time, type)
                        if len(__data) > 1:
                            coll.insert_many(
                                util_to_json_from_pandas(__data[1::]))
                else:
                    start_time = '2015-01-01'

                    util_log_info(
                        '##JOB05.{} Now Saving {} from {} to {} =={} '.
                        format(['1min', '5min', '15min', '30min', '60min'].
                               index(type), str(code), start_time, end_time, type),
                        ui_log=ui_log)

                    if start_time != end_time:
                        __data = fetch_get_index_min(
                            str(code), start_time, end_time, type)
                        if len(__data) > 1:
                            coll.insert_many(
                                util_to_json_from_pandas(__data))
        except:
            err.append(code)

    executor = ThreadPoolExecutor(max_workers=4)

    res = {executor.submit(
        __saving_work, __index_list.index[i_][0], coll) for i_ in range(len(__index_list))}  # multi index ./.
    count = 0
    for i_ in concurrent.futures.as_completed(res):
        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(count / len(__index_list) * 100))[0:4] + '%')
        intLogProgress = int(float(count / len(__index_list) * 10000.0))
        util_log_info('The {} of Total {}'.format(
            count, len(__index_list)), ui_log=ui_log)
        util_log_info(strLogProgress, ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=intLogProgress)
        count = count + 1
    if len(err) < 1:
        util_log_info('SUCCESS', ui_log=ui_log)
    else:
        util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        util_log_info(err, ui_log=ui_log)


def SU_save_etf_day(client=STOCKDATA, ui_log=None, ui_progress=None):
    """save etf_day

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """

    __index_list = fetch_get_stock_list('etf')
    coll = client.index_day
    coll.create_index([('code', pymongo.ASCENDING),
                       ('date_stamp', pymongo.ASCENDING)])
    err = []

    def __saving_work(code, coll):

        try:

            ref_ = coll.find({'code': str(code)[0:6]})
            end_time = str(now_time())[0:10]
            if ref_.count() > 0:
                start_time = ref_[ref_.count() - 1]['date']

                util_log_info('##JOB06 Now Saving ETF_DAY==== \n Trying updating {} from {} to {}'.format
                                 (code, start_time, end_time), ui_log=ui_log)

                if start_time != end_time:
                    coll.insert_many(
                        util_to_json_from_pandas(
                            fetch_get_index_day(str(code), util_get_next_day(start_time), end_time)))
            else:
                start_time = '1990-01-01'
                util_log_info('##JOB06 Now Saving ETF_DAY==== \n Trying updating {} from {} to {}'.format
                                 (code, start_time, end_time), ui_log=ui_log)

                if start_time != end_time:
                    coll.insert_many(
                        util_to_json_from_pandas(
                            fetch_get_index_day(str(code), start_time, end_time)))
        except:
            err.append(str(code))
    for i_ in range(len(__index_list)):
        # __saving_work('000001')
        util_log_info('The {} of Total {}'.format(
            i_, len(__index_list)), ui_log=ui_log)

        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(__index_list) * 100))[0:4] + '%')
        intLogProgress = int(float(i_ / len(__index_list) * 10000.0))
        util_log_info(strLogProgress, ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=intLogProgress)

        __saving_work(__index_list.index[i_][0], coll)
    if len(err) < 1:
        util_log_info('SUCCESS', ui_log=ui_log)
    else:
        util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        util_log_info(err, ui_log=ui_log)


def SU_save_etf_min(client=STOCKDATA, ui_log=None, ui_progress=None):
    """save etf_min

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """

    __index_list = fetch_get_stock_list('etf')
    coll = client.index_min
    coll.create_index([('code', pymongo.ASCENDING), ('time_stamp',
                                                     pymongo.ASCENDING), ('date_stamp', pymongo.ASCENDING)])
    err = []

    def __saving_work(code, coll):

        util_log_info(
            '##JOB07 Now Saving ETF_MIN ==== {}'.format(str(code)), ui_log=ui_log)
        try:

            for type in ['1min', '5min', '15min', '30min', '60min']:
                ref_ = coll.find(
                    {'code': str(code)[0:6], 'type': type})
                end_time = str(now_time())[0:19]
                if ref_.count() > 0:
                    start_time = ref_[ref_.count() - 1]['datetime']

                    util_log_info(
                        '##JOB07.{} Now Saving {} from {} to {} =={} '
                        .format(['1min', '5min', '15min', '30min', '60min']
                                .index(type), str(code), start_time, end_time, type),
                        ui_log=ui_log)

                    if start_time != end_time:
                        __data = fetch_get_index_min(
                            str(code), start_time, end_time, type)
                        if len(__data) > 1:
                            coll.insert_many(
                                util_to_json_from_pandas(__data[1::]))
                else:
                    start_time = '2015-01-01'

                    util_log_info(
                        '##JOB07.{} Now Saving {} from {} to {} =={} '
                        .format(['1min', '5min', '15min', '30min', '60min']
                                .index(type), str(code), start_time, end_time, type), ui_log=ui_log)

                    if start_time != end_time:
                        __data = fetch_get_index_min(
                            str(code), start_time, end_time, type)
                        if len(__data) > 1:
                            coll.insert_many(
                                util_to_json_from_pandas(__data))
        except:
            err.append(code)

    executor = ThreadPoolExecutor(max_workers=4)

    res = {executor.submit(
        __saving_work, __index_list.index[i_][0], coll) for i_ in range(len(__index_list))}  # multi index ./.
    count = 0
    for i_ in concurrent.futures.as_completed(res):

        util_log_info('The {} of Total {}'.format(
            count, len(__index_list)), ui_log=ui_log)
        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(count / len(__index_list) * 100))[0:4] + '%')
        intLogProgress = int(float(count / len(__index_list) * 10000.0))

        util_log_info(strLogProgress, ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=intLogProgress)
        count = count + 1
    if len(err) < 1:
        util_log_info('SUCCESS', ui_log=ui_log)
    else:
        util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        util_log_info(err, ui_log=ui_log)


def SU_save_stock_list(client=STOCKDATA, ui_log=None, ui_progress=None):
    """save stock_list

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """
    client.drop_collection('stock_list')
    coll = client.stock_list
    coll.create_index('code')

    try:
        # 🛠todo 这个应该是第一个任务 JOB01， 先更新股票列表！！
        util_log_info('##JOB08 Now Saving STOCK_LIST ====', ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=5000)
        stock_list_from_tdx = fetch_get_stock_list()
        pandas_data = util_to_json_from_pandas(stock_list_from_tdx)
        coll.insert_many(pandas_data)
        util_log_info("完成股票列表获取", ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=10000)
    except Exception as e:
        util_log_info(e, ui_log=ui_log)
        print(" Error save_tdx.SU_save_stock_list exception!")

        pass


def SU_save_stock_block(client=STOCKDATA, ui_log=None, ui_progress=None):
    """save stock_block

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """

    client.drop_collection('stock_block')
    coll = client.stock_block
    coll.create_index('code')

    try:
        util_log_info('##JOB09 Now Saving STOCK_BlOCK ====', ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=5000)
        coll.insert_many(util_to_json_from_pandas(
            fetch_get_stock_block('tdx')))
        util_log_info('tdx Block ====', ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=5000)

        # 🛠todo fixhere here 获取同花顺板块， 还是调用tdx的
        coll.insert_many(util_to_json_from_pandas(
            fetch_get_stock_block('ths')))
        util_log_info('ths Block ====', ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=8000)

        util_log_info('完成股票板块获取=', ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=10000)

    except Exception as e:
        util_log_info(e, ui_log=ui_log)
        print(" Error save_tdx.SU_save_stock_block exception!")
        pass


def SU_save_stock_info(client=STOCKDATA, ui_log=None, ui_progress=None):
    """save stock_info

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """

    client.drop_collection('stock_info')
    stock_list = fetch_get_stock_list().code.unique().tolist()
    coll = client.stock_info
    coll.create_index('code')
    err = []

    def __saving_work(code, coll):
        util_log_info(
            '##JOB010 Now Saving STOCK INFO ==== {}'.format(str(code)), ui_log=ui_log)
        try:
            coll.insert_many(
                util_to_json_from_pandas(
                    fetch_get_stock_info(str(code))))

        except:
            err.append(str(code))
    for i_ in range(len(stock_list)):
        # __saving_work('000001')

        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(stock_list) * 100))[0:4] + '%')
        intLogProgress = int(float(i_ / len(stock_list) * 10000.0))
        util_log_info('The {} of Total {}'.format(i_, len(stock_list)))
        util_log_info(strLogProgress, ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=intLogProgress)

        __saving_work(stock_list[i_], coll)
    if len(err) < 1:
        util_log_info('SUCCESS', ui_log=ui_log)
    else:
        util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        util_log_info(err, ui_log=ui_log)


def SU_save_stock_transaction(client=STOCKDATA, ui_log=None, ui_progress=None):
    """save stock_transaction

    Keyword Arguments:
        client {[type]} -- [description] (default: {STOCKDATA})
    """

    stock_list = fetch_get_stock_list().code.unique().tolist()
    coll = client.stock_transaction
    coll.create_index('code')
    err = []

    def __saving_work(code):
        util_log_info(
            '##JOB11 Now Saving STOCK_TRANSACTION ==== {}'.format(str(code)), ui_log=ui_log)
        try:
            coll.insert_many(
                util_to_json_from_pandas(
                    # 🛠todo  str(stock_list[code]) 参数不对？
                    fetch_get_stock_transaction(str(code), '1990-01-01', str(now_time())[0:10])))
        except:
            err.append(str(code))
    for i_ in range(len(stock_list)):
        # __saving_work('000001')
        util_log_info('The {} of Total {}'.format(
            i_, len(stock_list)), ui_log=ui_log)

        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(i_ / len(stock_list) * 100))[0:4] + '%')
        intLogProgress = int(float(i_ / len(stock_list) * 10000.0))

        util_log_info(strLogProgress, ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=intLogProgress)
        __saving_work(stock_list[i_])
    if len(err) < 1:
        util_log_info('SUCCESS', ui_log=ui_log)
    else:
        util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        util_log_info(err, ui_log=ui_log)


def SU_save_option_day(client=STOCKDATA, ui_log=None, ui_progress=None):
    '''
    :param client:
    :return:
    '''
    option_contract_list = fetch_get_50etf_option_contract_time_to_market()
    coll_option_day = client.option_day
    coll_option_day.create_index(
        [("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)])
    err = []

    # 索引 code

    def __saving_work(code, coll_option_day):
        try:
            util_log_info('##JOB12 Now Saving OPTION_DAY==== {}'.format(
                str(code)), ui_log=ui_log)

            # 首选查找数据库 是否 有 这个代码的数据
            # 期权代码 从 10000001 开始编码  10001228
            ref = coll_option_day.find({'code': str(code)[0:8]})
            end_date = str(now_time())[0:10]

            # 当前数据库已经包含了这个代码的数据， 继续增量更新
            # 加入这个判断的原因是因为如果是刚上市的 数据库会没有数据 所以会有负索引问题出现
            if ref.count() > 0:

                # 接着上次获取的日期继续更新
                start_date = ref[ref.count() - 1]['date']
                util_log_info(' 上次获取期权日线数据的最后日期是 {}'.format(
                    start_date), ui_log=ui_log)

                util_log_info('UPDATE_OPTION_DAY \n 从上一次下载数据开始继续 Trying update {} from {} to {}'.format(
                    code, start_date, end_date),  ui_log=ui_log)
                if start_date != end_date:

                    start_date0 = util_get_next_day(start_date)
                    df0 = fetch_get_option_day(code=code, start_date=start_date0, end_date=end_date,
                                                  frequence='day', ip=None, port=None)
                    retCount = df0.iloc[:, 0].size
                    util_log_info("日期从开始{}-结束{} , 合约代码{} , 返回了{}条记录 , 准备写入数据库"
                                     .format(start_date0, end_date, code, retCount), ui_log=ui_log)
                    coll_option_day.insert_many(
                        util_to_json_from_pandas(df0))
                else:
                    util_log_info("^已经获取过这天的数据了^ {}".format(
                        start_date), ui_log=ui_log)

            else:
                start_date = '1990-01-01'
                util_log_info('UPDATE_OPTION_DAY \n 从新开始下载数据 Trying update {} from {} to {}'.format
                                 (code, start_date, end_date), ui_log=ui_log)
                if start_date != end_date:

                    df0 = fetch_get_option_day(code=code, start_date=start_date, end_date=end_date,
                                                  frequence='day', ip=None, port=None)
                    retCount = df0.iloc[:, 0].size
                    util_log_info("日期从开始{}-结束{} , 合约代码{} , 获取了{}条记录 , 准备写入数据库^_^ "
                                     .format(start_date, end_date, code, retCount),
                                     ui_log=ui_log)

                    coll_option_day.insert_many(
                        util_to_json_from_pandas(df0))
                else:
                    util_log_info(
                        "*已经获取过这天的数据了* {}".format(start_date), ui_log=ui_log)

        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in range(len(option_contract_list)):
        util_log_info('The {} of Total {}'.format(
            item, len(option_contract_list)), ui_log=ui_log)

        strLogProgress = 'DOWNLOAD PROGRESS {} '.format(
            str(float(item / len(option_contract_list) * 100))[0:4] + '%')
        intLogProgress = int(float(item / len(option_contract_list) * 10000.0))
        util_log_info(strLogProgress, ui_log=ui_log,
                         ui_progress=ui_progress, ui_progress_int_value=intLogProgress)

        __saving_work(option_contract_list[item].code, coll_option_day)

    if len(err) < 1:
        util_log_info('SUCCESS save option day ^_^ ', ui_log=ui_log)
    else:
        util_log_info(' ERROR CODE \n ', ui_log=ui_log)
        util_log_info(err, ui_log=ui_log)


def SU_save_future_list(client=STOCKDATA, ui_log=None, ui_progress=None):
    future_list = fetch_get_future_list()
    coll_future_list = client.future_list
    coll_future_list.create_index("code", unique=True)
    try:
        coll_future_list.insert_many(
            util_to_json_from_pandas(future_list), ordered=False)
    except:
        pass


def SU_save_index_list(client=STOCKDATA, ui_log=None, ui_progress=None):
    index_list = fetch_get_index_list()
    coll_index_list = client.index_list
    coll_index_list.create_index("code", unique=True)

    try:
        coll_index_list.insert_many(
            util_to_json_from_pandas(index_list), ordered=False)
    except:
        pass


def SU_save_future_day(client=STOCKDATA, ui_log=None, ui_progress=None):
    pass


def SU_save_future_min(client=STOCKDATA, ui_log=None, ui_progress=None):
    pass


if __name__ == '__main__':
    #SU_save_stock_day()
    # SU_save_stock_xdxr()
    # SU_save_stock_min()
    # SU_save_stock_transaction()
    # SU_save_index_day()
    # SU_save_stock_list()
    # SU_save_index_min()
    #SU_save_index_list()
    #SU_save_future_list()
    _save_stock_data(stock_list=['000402','000001'],frequence='month')
    print(now_time())
