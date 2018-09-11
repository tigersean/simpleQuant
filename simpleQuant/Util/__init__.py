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
from simpleQuant.Util.Util_bar import (util_make_hour_index,
                                    util_make_min_index, util_time_gap)

# date
from simpleQuant.Util.Util_date import (util_date_int2str, util_date_stamp,
                                     util_date_str2int, util_date_today,
                                     util_date_valid, util_calc_time,
                                     util_get_date_index, util_to_datetime,
                                     util_get_index_date, util_id2date,
                                     util_is_trade, util_ms_stamp,
                                     util_realtime, util_select_hours,
                                     util_select_min, util_time_delay,
                                     util_time_now, util_time_stamp)
# trade date
from simpleQuant.Util.Util_date import (util_date_gap,
                                        util_get_real_date,
                                        util_get_real_datelist,
                                        util_get_trade_gap,
                                        util_get_trade_range,
                                        util_if_trade,
                                        util_if_tradetime,
                                        util_get_next_day,
                                        util_get_last_day,
                                        util_get_order_datetime,
                                        util_get_trade_datetime,
                                        trade_date_sse)
# list function
from simpleQuant.Util.Util_list import (util_diff_list,
                                     util_multi_demension_list)

from simpleQuant.Util.Util_code import util_code_tostr, util_code_tolist

from simpleQuant.Parameter import (MARKET_TYPE, ORDER_STATUS, TRADE_STATUS, DATASOURCE, OUTPUT_FORMAT,
                                          ORDER_DIRECTION, ORDER_MODEL, ORDER_EVENT, FREQUENCE, BROKER_TYPE,
                                          ACCOUNT_EVENT, BROKER_EVENT, EVENT_TYPE, MARKET_EVENT, ENGINE_EVENT,
                                          RUNNING_ENVIRONMENT, AMOUNT_MODEL, MARKET_ERROR)

from simpleQuant.Util.Util_setting import (Util_setting, future_ip_list, SETTINGS,
                                        info_ip_list, stock_ip_list, exclude_from_stock_ip_list)

from simpleQuant.Util.Util_transform import (util_to_json_from_pandas,
                                          util_to_list_from_numpy,
                                          util_to_list_from_pandas,
                                          util_to_pandas_from_json,
                                          util_to_pandas_from_list)

from simpleQuant.Util.Localize import qa_path, setting_path, cache_path, download_path, log_path

# log
from simpleQuant.Util.Util_logs import (util_log_debug, util_log_expection, util_log_info)

# RANDOM class
from simpleQuant.Util.Util_random import util_random_with_topic

# 网络相关
from simpleQuant.Util.Util_webutil import util_web_ping
from simpleQuant.Util.Util_mail import util_send_mail