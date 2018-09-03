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
""""
yutiansut
util tool
"""
# path

# bar
from QUANTAXIS.QAUtil.QABar import (QA_util_make_hour_index,
                                    QA_util_make_min_index, QA_util_time_gap)
# config
from QUANTAXIS.QAUtil.QACfg import QA_util_cfg_initial, QA_util_get_cfg
# date
from simpleQuant.Util.Trade_date import (util_date_int2str, util_date_stamp,
                                     util_date_str2int, util_date_today,
                                     util_date_valid, util_calc_time,
                                     util_get_date_index, util_to_datetime,
                                     util_get_index_date, util_id2date,
                                     util_is_trade, util_ms_stamp,
                                     util_realtime, util_select_hours,
                                     util_select_min, util_time_delay,
                                     util_time_now, util_time_stamp)
# trade date
from simpleQuant.Util.Date_trade import (QA_util_date_gap,
                                           QA_util_get_real_date,
                                           QA_util_get_real_datelist,
                                           QA_util_get_trade_gap,
                                           QA_util_get_trade_range,
                                           QA_util_if_trade,
                                           QA_util_if_tradetime,
                                           QA_util_get_next_day,
                                           QA_util_get_last_day,
                                           QA_util_get_order_datetime,
                                           QA_util_get_trade_datetime,
                                           trade_date_sse)
# list function
from .QAList import (QA_util_diff_list,
                                     QA_util_multi_demension_list)

from QUANTAXIS.QAUtil.QACode import util_code_tostr, util_code_tolist

from QUANTAXIS.QAUtil.QAParameter import (MARKET_TYPE, ORDER_STATUS, TRADE_STATUS, DATASOURCE, OUTPUT_FORMAT,
                                          ORDER_DIRECTION, ORDER_MODEL, ORDER_EVENT, FREQUENCE, BROKER_TYPE,
                                          ACCOUNT_EVENT, BROKER_EVENT, EVENT_TYPE, MARKET_EVENT, ENGINE_EVENT,
                                          RUNNING_ENVIRONMENT, AMOUNT_MODEL, MARKET_ERROR)

from simpleQuant.Util.QASetting import (QA_Setting, DATABASE, future_ip_list, QASETTING,
                                        info_ip_list, stock_ip_list, exclude_from_stock_ip_list)

from simpleQuant.Util.Transform import (util_to_json_from_pandas,
                                          util_to_list_from_numpy,
                                          util_to_list_from_pandas,
                                          util_to_pandas_from_json,
                                          util_to_pandas_from_list)

