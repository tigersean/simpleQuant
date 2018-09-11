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
#
#
import datetime
import threading
import time

from simpleQuant.Parameter import MARKET_TYPE
from simpleQuant.Util.Util_logs import util_log_info

trade_date_sse=['1990-12-19']

# 🛠todo 时间函数 建议使用这些
#  字符串 和 datetime date time 类型之间的转换
#  util__str_to_dateime
#
#  util__datetime_to_str19
#  util__datetime_to_str10

#  util__str10_to_datetime
#  util__str19_to_datetime

#  util__int10_to_datetime
#  util__int19_to_datetime

#  util__date_to_str10
#  util__date_to_str19

#  util__time_to_str10
#  util__time_to_str19

#  util__str10_to_date
#  util__str10_to_time

#  util__str19_to_time
#  util__str19_to_date

# 或者有更好的方案


def util_time_now():
    """
    返回当前时间
    :return: 类型datetime.datetime
    """
    return datetime.datetime.now()


def util_date_today():
    """
    返回当前日期
    :return: 类型datetime.date
    """
    return datetime.date.today()


def util_today_str():
    """
    返回今天的日期字符串
    :return: 类型字符串 2011-11-11
    """
    dt = util_date_today()
    str = util_datetime_to_strdate(dt)
    return str


def util_date_str2int(date):
    """
    日期字符串 '2011-09-11' 变换成 整数 20110911
    日期字符串 '2018-12-01' 变换成 整数 20181201
    :param date: str日期字符串
    :return: 类型int
    """
    # return int(str(date)[0:4] + str(date)[5:7] + str(date)[8:10])
    if isinstance(date, str):
        return int(str().join(date.split('-')))
    elif isinstance(date, int):
        return date


def util_date_int2str(int_date):
    """
    类型datetime.datatime
    :param date: int 8位整数
    :return: 类型str
    """
    #int_date=int()
    date=str(int_date)
    if len(date)==8:
        return str(date[0:4] + '-' + date[4:6] + '-' + date[6:8])
    elif len(date)==10:
        return date


def util_to_datetime(time):
    """
    字符串 '2018-01-01'  转变成 datatime 类型
    :param time: 字符串str -- 格式必须是 2018-01-01 ，长度10
    :return: 类型datetime.datatime
    """
    if len(str(time)) == 10:
        _time = '{} 00:00:00'.format(time)
    elif len(str(time)) == 19:
        _time = str(time)
    else:
        util_log_info('WRONG DATETIME FORMAT {}'.format(time))
    return datetime.datetime.strptime(_time, '%Y-%m-%d %H:%M:%S')


def util_datetime_to_strdate(dt):
    """
    :param dt:  pythone datetime.datetime
    :return:  1999-02-01 string type
    """
    strdate = "%04d-%02d-%02d" % (dt.year, dt.month, dt.day)
    return strdate


def util_datetime_to_strdatetime(dt):
    """
    :param dt:  pythone datetime.datetime
    :return:  1999-02-01 09:30:91 string type
    """
    strdatetime = "%04d-%02d-%02d %02d:%02d:%02d" % (
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
    return strdatetime


def util_date_stamp(date):
    """
    字符串 '2018-01-01'  转变成 float 类型时间 类似 time.time() 返回的类型
    :param date: 字符串str -- 格式必须是 2018-01-01 ，长度10
    :return: 类型float
    """
    datestr = str(date)[0:10]
    date = time.mktime(time.strptime(datestr, '%Y-%m-%d'))
    return date


def util_time_stamp(time_):
    """
    字符串 '2018-01-01 00:00:00'  转变成 float 类型时间 类似 time.time() 返回的类型
    :param time_: 字符串str -- 数据格式 最好是%Y-%m-%d %H:%M:%S 中间要有空格
    :return: 类型float
    """
    if len(str(time_)) == 10:
        # yyyy-mm-dd格式
        return time.mktime(time.strptime(time_, '%Y-%m-%d'))
    elif len(str(time_)) == 16:
        # yyyy-mm-dd hh:mm格式
        return time.mktime(time.strptime(time_, '%Y-%m-%d %H:%M'))
    else:
        timestr = str(time_)[0:19]
        return time.mktime(time.strptime(timestr, '%Y-%m-%d %H:%M:%S'))


def util_pands_timestamp_to_date(pandsTimestamp):
    """
    转换 pandas 的时间戳 到 datetime.date类型
    :param pandsTimestamp: 类型 pandas._libs.tslib.Timestamp
    :return: datetime.datetime类型
    """
    return pandsTimestamp.to_pydatetime().date()


def util_pands_timestamp_to_datetime(pandsTimestamp):
    """
    转换 pandas 的时间戳 到 datetime.datetime类型
    :param pandsTimestamp: 类型 pandas._libs.tslib.Timestamp
    :return: datetime.datetime类型
    """
    return pandsTimestamp.to_pydatetime()


def util_stamp2datetime(timestamp):
    """
    datestamp转datetime
    pandas转出来的timestamp是13位整数 要/1000
    It’s common for this to be restricted to years from 1970 through 2038.
    从1970年开始的纳秒到当前的计数 转变成 float 类型时间 类似 time.time() 返回的类型
    :param timestamp: long类型
    :return: 类型float
    """
    try:
        return datetime.datetime.fromtimestamp(timestamp)
    except Exception as e:
        # it won't work ??
        return datetime.datetime.fromtimestamp(timestamp / 1000)
    #


def util_ms_stamp(ms):
    """
    直接返回不做处理
    :param ms:  long类型 -- tick count
    :return: 返回ms
    """
    return ms


def util_date_valid(date):
    """
    判断字符串是否是 1982-05-11 这种格式
    :param date: date 字符串str -- 格式 字符串长度10
    :return: boolean -- 格式是否正确
    """
    try:
        time.strptime(date, "%Y-%m-%d")
        return True
    except:
        return False


def util_realtime(strtime, client):
    """
    查询数据库中的数据
    :param strtime: strtime  str字符串                 -- 1999-12-11 这种格式
    :param client: client  pymongo.MongoClient类型    -- mongodb 数据库 从 util_sql_mongo_setting 中 util_sql_mongo_setting 获取
    :return: Dictionary  -- {'time_real': 时间,'id': id}
    """
    time_stamp = util_date_stamp(strtime)
    coll = client.quantaxis.trade_date
    temp_str = coll.find_one({'date_stamp': {"$gte": time_stamp}})
    time_real = temp_str['date']
    time_id = temp_str['num']
    return {'time_real': time_real, 'id': time_id}


def util_id2date(idx, client):
    """
    从数据库中查询 通达信时间
    :param idx: 字符串 -- 数据库index
    :param client: pymongo.MongoClient类型    -- mongodb 数据库 从 util_sql_mongo_setting 中 util_sql_mongo_setting 获取
    :return:         Str -- 通达信数据库时间
    """
    coll = client.quantaxis.trade_date
    temp_str = coll.find_one({'num': idx})
    return temp_str['date']


def util_is_trade(date, code, client):
    """
    判断是否是交易日
    从数据库中查询
    :param date: str类型 -- 1999-12-11 这种格式    10位字符串
    :param code: str类型 -- 股票代码 例如 603658 ， 6位字符串
    :param client: pymongo.MongoClient类型    -- mongodb 数据库 从 util_sql_mongo_setting 中 util_sql_mongo_setting 获取
    :return:  Boolean -- 是否是交易时间
    """
    coll = client.quantaxis.stock_day
    date = str(date)[0:10]
    is_trade = coll.find_one({'code': code, 'date': date})
    try:
        len(is_trade)
        return True
    except:
        return False


def util_get_date_index(date, trade_list):
    """
    返回在trade_list中的index位置
    :param date: str类型 -- 1999-12-11 这种格式    10位字符串
    :param trade_list: ？？
    :return: ？？
    """
    return trade_list.index(date)


def util_get_index_date(id, trade_list):
    """
    :param id:  ：？？
    :param trade_list:  ？？
    :return: ？？
    """
    return trade_list[id]


def util_select_hours(time=None, gt=None, lt=None, gte=None, lte=None):
    'quantaxis的时间选择函数,约定时间的范围,比如早上9点到11点'
    if time is None:
        __realtime = datetime.datetime.now()
    else:
        __realtime = time

    fun_list = []
    if gt != None:
        fun_list.append('>')
    if lt != None:
        fun_list.append('<')
    if gte != None:
        fun_list.append('>=')
    if lte != None:
        fun_list.append('<=')

    assert len(fun_list) > 0
    true_list = []
    try:
        for item in fun_list:
            if item == '>':
                if __realtime.strftime('%H') > gt:
                    true_list.append(0)
                else:
                    true_list.append(1)
            elif item == '<':
                if __realtime.strftime('%H') < lt:
                    true_list.append(0)
                else:
                    true_list.append(1)
            elif item == '>=':
                if __realtime.strftime('%H') >= gte:
                    true_list.append(0)
                else:
                    true_list.append(1)
            elif item == '<=':
                if __realtime.strftime('%H') <= lte:
                    true_list.append(0)
                else:
                    true_list.append(1)

    except:
        return Exception
    if sum(true_list) > 0:
        return False
    else:
        return True


def util_select_min(time=None, gt=None, lt=None, gte=None, lte=None):
    """
    'quantaxis的时间选择函数,约定时间的范围,比如30分到59分'
    :param time:
    :param gt:
    :param lt:
    :param gte:
    :param lte:
    :return:
    """
    if time is None:
        __realtime = datetime.datetime.now()
    else:
        __realtime = time

    fun_list = []
    if gt != None:
        fun_list.append('>')
    if lt != None:
        fun_list.append('<')
    if gte != None:
        fun_list.append('>=')
    if lte != None:
        fun_list.append('<=')

    assert len(fun_list) > 0
    true_list = []
    try:
        for item in fun_list:
            if item == '>':
                if __realtime.strftime('%M') > gt:
                    true_list.append(0)
                else:
                    true_list.append(1)
            elif item == '<':
                if __realtime.strftime('%M') < lt:
                    true_list.append(0)
                else:
                    true_list.append(1)
            elif item == '>=':
                if __realtime.strftime('%M') >= gte:
                    true_list.append(0)
                else:
                    true_list.append(1)
            elif item == '<=':
                if __realtime.strftime('%M') <= lte:
                    true_list.append(0)
                else:
                    true_list.append(1)
    except:
        return Exception
    if sum(true_list) > 0:
        return False
    else:
        return True


def util_time_delay(time_=0):
    """
    '这是一个用于复用/比如说@装饰器的延时函数\
    使用threading里面的延时,为了是不阻塞进程\
    有时候,同时发进去两个函数,第一个函数需要延时\
    第二个不需要的话,用sleep就会阻塞掉第二个进程'
    :param time_:
    :return:
    """
    def _exec(func):
        threading.Timer(time_, func)
    return _exec


def util_calc_time(func, *args, **kwargs):
    """
    '耗时长度的装饰器'
    :param func:
    :param args:
    :param kwargs:
    :return:
    """
    _time = datetime.datetime.now()
    func(*args, **kwargs)
    print(datetime.datetime.now() - _time)
    # return datetime.datetime.now() - _time


def util_if_trade(day):
    '''
    '日期是否交易'
    查询上面的 交易日 列表
    :param day: 类型 str eg: 2018-11-11
    :return: Boolean 类型
    '''
    if day in trade_date_sse:
        return True
    else:
        return False


def util_if_tradetime(_time,market=MARKET_TYPE.STOCK_CN,code=None):
    '时间是否交易'
    _time = datetime.datetime.strptime(str(_time)[0:19], '%Y-%m-%d %H:%M:%S')
    if market is MARKET_TYPE.STOCK_CN:
        if util_if_trade(str(_time.date())[0:10]):
            if _time.hour in [10, 13, 14]:
                return True
            elif _time.hour in [9] and _time.minute >= 15:  #修改成9:15 加入 9:15-9:30的盘前竞价时间
                return True
            elif _time.hour in [11] and _time.minute <= 30:
                return True
            else:
                return False
        else:
            return False
    elif market is MARKET_TYPE.FUTURE_CN:
        # 暂时用螺纹
        if code[0:2] in ['rb','RB']:
            if util_if_trade(str(_time.date())[0:10]):
                if _time.hour in [9, 10, 14, 21, 22]:
                    return True
                elif _time.hour in [13] and _time.minute >= 30:
                    return True
                elif _time.hour in [11] and _time.minute <= 30:
                    return True
                else:
                    return False
            else:
                return False
                
def util_get_next_day(date,n=1):
    '''
    得到下一个(n)交易日
    :param date: 类型 str eg: 2018-11-11
    :param n:  整形
    :return: 字符串 str eg: 2018-11-12
    '''
    date=str(date)[0:10]
    return util_date_gap(date,n,'gt')

def util_get_last_day(date,n=1):
    '''
    得到上一个(n)交易日
    :param  date: 类型 str eg: 2018-11-11
    :param n:  整形
    :return: 字符串 str eg: 2018-11-10
    '''
    date=str(date)[0:10]
    return util_date_gap(date,n,'lt')
    
def util_get_real_date(date, trade_list=trade_date_sse, towards=-1):
    """
    获取真实的交易日期,其中,第三个参数towards是表示向前/向后推
    towards=1 日期向后迭代
    towards=-1 日期向前迭代
    @ yutiansut

    """
    date=str(date)[0:10]
    if towards == 1:
        while date not in trade_list:
            date = str(datetime.datetime.strptime(
                str(date)[0:10], '%Y-%m-%d') + datetime.timedelta(days=1))[0:10]
        else:
            return str(date)[0:10]
    elif towards == -1:
        while date not in trade_list:
            date = str(datetime.datetime.strptime(
                str(date)[0:10], '%Y-%m-%d') - datetime.timedelta(days=1))[0:10]
        else:
            return str(date)[0:10]


def util_get_real_datelist(start, end):
    """
    取数据的真实区间,返回的时候用 start,end=util_get_real_datelist
    @yutiansut
    2017/8/10

    当start end中间没有交易日 返回None, None
    @yutiansut/ 2017-12-19
    """
    real_start = util_get_real_date(start, trade_date_sse, 1)
    real_end = util_get_real_date(end, trade_date_sse, -1)
    if trade_date_sse.index(real_start) > trade_date_sse.index(real_end):
        return None, None
    else:
        return (real_start, real_end)


def util_get_trade_range(start, end):
    '给出交易具体时间'
    start, end = util_get_real_datelist(start, end)
    if start is not None:
        return trade_date_sse[trade_date_sse.index(start):trade_date_sse.index(end) + 1:1]
    else:
        return None


def util_get_trade_gap(start, end):
    '返回start_day到end_day中间有多少个交易天 算首尾'
    start, end = util_get_real_datelist(start, end)
    if start is not None:
        return trade_date_sse.index(end) + 1 - trade_date_sse.index(start)
    else:
        return 0


def util_date_gap(date, gap, methods):
    '''
    :param date: 字符串起始日 类型 str eg: 2018-11-11
    :param gap: 整数 间隔多数个交易日
    :param methods:  gt大于 ，gte 大于等于， 小于lt ，小于等于lte ， 等于===
    :return: 字符串 eg：2000-01-01
    '''
    try:
        if methods in ['>', 'gt']:
            return trade_date_sse[trade_date_sse.index(date) + gap]
        elif methods in ['>=', 'gte']:
            return trade_date_sse[trade_date_sse.index(date) + gap - 1]
        elif methods in ['<', 'lt']:
            return trade_date_sse[trade_date_sse.index(date) - gap]
        elif methods in ['<=', 'lte']:
            return trade_date_sse[trade_date_sse.index(date) - gap + 1]
        elif methods in ['==', '=', 'eq']:
            return date

    except:
        return 'wrong date'


def util_get_trade_datetime(dt=datetime.datetime.now()):
    """交易的真实日期
    
    Returns:
        [type] -- [description]
    """

    #dt= datetime.datetime.now()

    

    if util_if_trade(str(dt.date())) and dt.time()<datetime.time(15,0,0):
        return str(dt.date())
    else:
        return util_get_real_date(str(dt.date()),trade_date_sse,1)

def util_get_order_datetime(dt):
    """委托的真实日期
    
    Returns:
        [type] -- [description]
    """

    #dt= datetime.datetime.now()
    dt = datetime.datetime.strptime(str(dt)[0:19], '%Y-%m-%d %H:%M:%S')

    if util_if_trade(str(dt.date())) and dt.time()<datetime.time(15,0,0) :
        return str(dt)
    else:
        #print('before')
        #print(util_date_gap(str(dt.date()),1,'lt'))
        return '{} {}'.format(util_date_gap(str(dt.date()),1,'lt'),dt.time())


def future_change(df):
    for i in range(len(df)):

        weekday=df.datetime[i].weekday()

        if df.loc[i,'inttime']>1500:#after 2400,no need to change

            delta=1

        if weekday==0:#周一为0，周日为6；其它情形皆为前一日，即delta=1

            delta=3

        df.loc[i,'datetime']=df.datetime[i]-datetime.timedelta(days=delta)

        if df.loc[i,'inttime']<900:#after 2400,no need to change，but monday's data should be Saturdays'

            if weekday==0:#改为周六

                delta=2

            df.loc[i,'datetime']=df.datetime[i]-datetime.timedelta(days=delta)


        df=df.sort_values('datetime',ascending=False)

        df=df.set_index('datetime')



month_data = ['1996-03-31', '1996-06-30','1996-09-30','1996-12-31', '1997-03-31','1997-06-30',
              '1997-09-30', '1997-12-31','1998-03-31','1998-06-30','1998-09-30','1998-12-31',
              '1999-03-31', '1999-06-30',
              '1999-09-30',
              '1999-12-31',
              '2000-03-31',
              '2000-06-30',
              '2000-09-30',
              '2000-12-31',
              '2001-03-31',
              '2001-06-30',
              '2001-09-30',
              '2001-12-31',
              '2002-03-31',
              '2002-06-30',
              '2002-09-30',
              '2002-12-31',
              '2003-03-31',
              '2003-06-30',
              '2003-09-30',
              '2003-12-31',
              '2004-03-31',
              '2004-06-30',
              '2004-09-30',
              '2004-12-31',
              '2005-03-31',
              '2005-06-30',
              '2005-09-30',
              '2005-12-31',
              '2006-03-31',
              '2006-06-30',
              '2006-09-30',
              '2006-12-31',
              '2007-03-31',
              '2007-06-30',
              '2007-09-30',
              '2007-12-31',
              '2008-03-31',
              '2008-06-30',
              '2008-09-30',
              '2008-12-31',
              '2009-03-31',
              '2009-06-30',
              '2009-09-30',
              '2009-12-31',
              '2010-03-31',
              '2010-06-30',
              '2010-09-30',
              '2010-12-31',
              '2011-03-31',
              '2011-06-30',
              '2011-09-30',
              '2011-12-31',
              '2012-03-31',
              '2012-06-30',
              '2012-09-30',
              '2012-12-31',
              '2013-03-31',
              '2013-06-30',
              '2013-09-30',
              '2013-12-31',
              '2014-03-31',
              '2014-06-30',
              '2014-09-30',
              '2014-12-31',
              '2015-03-31',
              '2015-06-30',
              '2015-09-30',
              '2015-12-31',
              '2016-03-31',
              '2016-06-30',
              '2016-09-30',
              '2016-12-31',
              '2017-03-31',
              '2017-06-30',
              '2017-09-30',
              '2017-12-31',
              '2018-03-31',
              '2018-06-30',
              '2018-09-30',
              '2018-12-31']





if __name__ == '__main__':
    print(trade_date_sse.count)
