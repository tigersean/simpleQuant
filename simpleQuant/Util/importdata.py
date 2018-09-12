import os.path
import struct
import sqlite3
import datetime
import math
import sys
import numpy as np
from dateutil.parser import parse

import tables as tb

from simpleQuant.Util.Util_logs import util_log_info
import simpleQuant.Fetch.tdx_fetch as fetch


def ProgressBar(cur, total):
    percent = '{:.2%}'.format(cur / total)
    sys.stdout.write('\r')
    sys.stdout.write("[%-50s] %s" % ('=' * int(math.floor(cur * 50 / total)),percent))
    sys.stdout.flush()


class H5Record(tb.IsDescription):
    datetime = tb.UInt64Col()        #IGNORE:E1101
    open = tb.UInt32Col()       #IGNORE:E1101
    high = tb.UInt32Col()       #IGNORE:E1101
    low = tb.UInt32Col()        #IGNORE:E1101
    close = tb.UInt32Col()      #IGNORE:E1101
    vol = tb.UInt64Col()     #IGNORE:E1101
    amount = tb.UInt64Col()      #IGNORE:E1101
    
class H5Index(tb.IsDescription):
    datetime = tb.UInt64Col()        #IGNORE:E1101
    start    = tb.UInt64Col()        #IGNORE:E1101
    

def ImportStockName(connect):
    """更新每只股票的名称、当前是否有效性、起始日期及结束日期
        如果导入的代码表中不存在对应的代码，则认为该股已失效"""
    cur = connect.cursor()    
    
    oldStockDict = {}
    market={'sz','sh'}
    newStockDict=fetch.fetch_get_stock_list().to_dict('index')
    cur.execute("create table if not exists stock_info (id varchar(8) PRIMARY KEY,\
    code varchar(6) ,name varchar(20),market char(8),type smallint,valid smallint,\
    startDate integer,endDate integer)")
    a = cur.execute("select id, code, name, valid, market from stock_info") 
    a = a.fetchall()

    for oldstock in a:
        oldstockid, oldcode, oldname, oldvalid, market = oldstock[0], oldstock[1], oldstock[2], int(oldstock[3]), oldstock[4]
        oldStockDict[oldcode] = oldstockid
        
        #新的代码表中无此股票，则置为无效
        if (oldvalid == 1) and ((oldcode,market) not in newStockDict):            
            cur.execute("update stock_info set valid=0 where id=%s" % oldstockid)
        
        #股票名称发生变化，更新股票名称;如果原无效，则置为有效
        if (oldcode, market) in newStockDict:
            if oldname != newStockDict[(oldcode,market)]['name']:
                cur.execute("update stock_info set name='%s' where id=%s" % 
                            (newStockDict[(oldcode,market)]['name'], oldstockid))
            if oldvalid == 0:
                cur.execute("update stock_info set valid=1, endDate=99999999 where id=%i" % oldstockid)

    
    today = datetime.date.today()
    today = today.year * 10000 + today.month * 100 + today.day
    count = 0
    for (code, market) in newStockDict:
        if code not in oldStockDict:
            count += 1
            sql = "insert into stock_info(id, code, name, market, type, valid, startDate, endDate) \
                   values ('%s', '%s', '%s', '%s', '%i', '%i', %i, %i)" \
                   % (market+code, code, newStockDict[(code,market)]['name'], newStockDict[(code,market)]['sse'], 0, 1, today, 99999999)
            cur.execute(sql)
        pass            
    
    print('%s新增股票数：%i' % (market.upper(),count))
    connect.commit()
    cur.close()    


def ImportDayData(connect, dest_dir):
    """
    导入通达信日线数据，只导入基础信息数据库中存在的股票
    """
    cur = connect.cursor()
    today = datetime.date.today()
    h5fileDict = {'SH': tb.open_file(dest_dir + "/sh_day.h5", "a", filters=tb.Filters(complevel=9,complib='blosc', shuffle=True)),
                  'SZ': tb.open_file(dest_dir + "/sz_day.h5", "a", filters=tb.Filters(complevel=9,complib='blosc', shuffle=True))}
    
    h5groupDict = {}
    for market in h5fileDict:
        try:
            group = h5fileDict[market].get_node("/", "data")
        except:
            group = h5fileDict[market].create_group("/", "data")
        h5groupDict[market] = group  
    
    
    a = cur.execute("select id, market, code, valid, type from stock_info order by id")
    a = a.fetchall()
    stock_count = 0
    record_count = 0
    total = len(a)
    for i, stock in enumerate(a):
        def fmtdatestr(datetime):
            return int(datetime.replace('-',''))

        ProgressBar(i+1, total)
        stockid, marketid, code = stock[0], stock[1], stock[2]
        valid, stktype = stock[3], stock[4]        
        tablename = marketid + code
        
        try:
            table = h5fileDict[market].get_node(h5groupDict[market], tablename)
        except:
            table = h5fileDict[market].create_table(h5groupDict[market], tablename, H5Record)
        
        if table.nrows > 0:
            startdate = table[0]['datetime']/10000
            lastdatetime = table[-1]['datetime']/10000
        else:
            startdate = None
            lastdatetime = 19900101
        
        update_flag = False
        row = table.row
        data=fetch.fetch_get_stock_day(code, parse(str(lastdatetime)).strftime('%Y-%m-%d'),
                                    today.strftime('%Y-%m-%d'))
        for i, record in data.iterrows():            
            row['datetime'] = fmtdatestr(record['date'])
            row['open'] = record['open']
            row['high'] = record['high']
            row['low'] = record['low']
            row['close'] = record['close']
            row['vol'] = record['vol']
            row['amount'] = record['amount']
            row.append()
            record_count +=1
            if not update_flag:
                update_flag = True
                                
        if update_flag:
            stock_count += 1
            if (stock_count%100==0):
                table.flush()
            
        if startdate is not None and valid == 0:
            cur.execute("update stock set valid=1, startdate=%i, enddate=%i where stockid=%i" %
                        (startdate, 99999999, stockid))
    table.flush()    
    connect.commit()
                                
    for market in h5fileDict:
        h5fileDict[market].close()
        
    print("\n共导入股票数:", stock_count)
    print("共导入日线数:", record_count)


def ImportMinData(connect, src_dir, dest_dir, data_type):
    """
    导入通达信分钟线、5分钟线数据，只导入基础信息数据库中存在的股票
    """
    if data_type != '1min' and data_type != '5min':
        print("错误的参数: %s" % data_type)
        return
    
    cur = connect.cursor()
    
    if data_type == '1min':
        print("导入1分钟数据")
        h5fileDict = {'sh': tb.open_file(dest_dir + "/sh_1min.h5", "a", filters=tb.Filters(complevel=9,complib='zlib', shuffle=True)),
                      'sz': tb.open_file(dest_dir + "/sz_1min.h5", "a", filters=tb.Filters(complevel=9,complib='zlib', shuffle=True))}
        
    else:
        print("导入5分钟数据")
        h5fileDict = {'sh': tb.open_file(dest_dir + "/sh_5min.h5", "a", filters=tb.Filters(complevel=9,complib='zlib', shuffle=True)),
                      'sz': tb.open_file(dest_dir + "/sz_5min.h5", "a", filters=tb.Filters(complevel=9,complib='zlib', shuffle=True))}
    
    h5groupDict = {}
    for market in h5fileDict:
        try:
            group = h5fileDict[market].get_node("/", "data")
        except:
            group = h5fileDict[market].create_group("/", "data")
        h5groupDict[market] = group
    
    
    a = cur.execute("select marketid, market from market")
    marketDict = {}
    for mark in a:
        marketDict[mark[0]] = mark[1].upper()

    a = cur.execute("select marketid, code, type from stock order by marketid")
    a = a.fetchall()
    stock_count = 0
    record_count = 0
    total = len(a)
    for i, stock in enumerate(a):
        ProgressBar(i+1, total)
        marketid, code, stktype = stock[0], stock[1], stock[2]
        market = marketDict[marketid]
        tablename = market + code
        filename = dirDict[market] + "/" + tablename.lower() + file_suffix
        
        if not os.path.exists(filename):
            continue
        
        try:
            table = h5fileDict[market].get_node(h5groupDict[market], tablename)
        except:
            table = h5fileDict[market].create_table(h5groupDict[market], tablename, H5Record)
        
        if table.nrows > 0:
            lastdatetime = table[-1]['datetime']
        else:
            lastdatetime = None 
        
        update_flag = False
        row = table.row
        data=fetch.fetch_get_stock_day(code, lastdatetime,today)
        for record in data:
            row['datetime'] = record[0] * 10000
            row['openPrice'] = record[1] * 10
            row['highPrice'] = record[2] * 10
            row['lowPrice'] = record[3] * 10
            row['closePrice'] = record[4] * 10
            row['transAmount'] = round(record[5] * 0.001)
            if stktype == 2:
            #指数
                row['transCount'] = record[6]
            else:
                row['transCount'] = round(record[6] * 0.01)
            row.append()
            record_count +=1
            if not update_flag:
                update_flag = True                    
                                
        if update_flag:
            stock_count += 1
            table.flush()        
            
    connect.commit()
                                
    for market in h5fileDict:
        h5fileDict[market].close()
        
    print("\n共导入股票数:", stock_count)
    if data_type == '1min':
        print("共导入1分钟线数:", record_count)
    else:
        print("共导入5分钟线数:", record_count)
    

            
def UpdateIndex(filename, data_type):
    
    def getWeekDate(olddate):
        y = olddate//100000000
        m = olddate//1000000 - y*100
        d = olddate//10000 - (y*10000+m*100)
        tempdate = datetime.date(y,m,d)
        tempweekdate = tempdate - datetime.timedelta(tempdate.weekday())
        newdate = tempweekdate.year*100000000 + tempweekdate.month*1000000 + tempweekdate.day*10000
        return newdate

    def getMonthDate(olddate):
        y = olddate//100000000
        m = olddate//1000000 - y*100
        return(y*100000000 + m*1000000 + 10000)

    def getQuarterDate(olddate):
        quarterDict={1:1,2:1,3:1,4:4,5:4,6:4,7:7,8:7,9:7,10:10,11:10,12:10}
        y = olddate//100000000
        m = olddate//1000000 - y*100
        return( y*100000000 + quarterDict[m]*1000000 + 10000 )
    
    def getHalfyearDate(olddate):
        halfyearDict={1:1,2:1,3:1,4:1,5:1,6:1,7:7,8:7,9:7,10:7,11:7,12:7}
        y = olddate//100000000
        m = olddate//1000000 - y*100
        return( y*100000000 + halfyearDict[m]*1000000 + 10000 )
    
    def getYearDate(olddate):
        y = olddate//100000000
        return(y*100000000 + 1010000)

    def getMin60Date(olddate):
        mint = olddate-olddate//10000*10000
        if mint<=1030:
            newdate = olddate//10000*10000 + 1030
        elif mint<=1130:
            newdate = olddate//10000*10000 + 1130
        elif mint<=1400:
            newdate = olddate//10000*10000 + 1400
        else:
            newdate = olddate//10000*10000 + 1500
        return newdate
    
    def getMin15Date(olddate):
        mint = olddate-olddate//10000*10000
        if mint<=945:
            newdate = olddate//10000*10000 + 945
        elif mint<=1000:
            newdate = olddate//10000*10000 + 1000
        elif mint<=1015:
            newdate = olddate//10000*10000 + 1015
        elif mint<=1030:
            newdate = olddate//10000*10000 + 1030
        elif mint<=1045:
            newdate = olddate//10000*10000 + 1045
        elif mint<=1100:
            newdate = olddate//10000*10000 + 1100
        elif mint<=1115:
            newdate = olddate//10000*10000 + 1115
        elif mint<=1130:
            newdate = olddate//10000*10000 + 1130
        elif mint<=1315:
            newdate = olddate//10000*10000 + 1315
        elif mint<=1330:
            newdate = olddate//10000*10000 + 1330
        elif mint<=1345:
            newdate = olddate//10000*10000 + 1345
        elif mint<=1400:
            newdate = olddate//10000*10000 + 1400
        elif mint<=1415:
            newdate = olddate//10000*10000 + 1415
        elif mint<=1430:
            newdate = olddate//10000*10000 + 1430
        elif mint<=1445:
            newdate = olddate//10000*10000 + 1445
        else:
            newdate = olddate//10000*10000 + 1500
        return newdate    
    
    def getMin30Date(olddate):
        mint = olddate-olddate//10000*10000
        if mint<=1000:
            newdate = olddate//10000*10000 + 1000
        elif mint<=1030:
            newdate = olddate//10000*10000 + 1030
        elif mint<=1100:
            newdate = olddate//10000*10000 + 1100
        elif mint<=1130:
            newdate = olddate//10000*10000 + 1130
        elif mint<=1330:
            newdate = olddate//10000*10000 + 1330
        elif mint<=1400:
            newdate = olddate//10000*10000 + 1400
        elif mint<=1430:
            newdate = olddate//10000*10000 + 1430
        else:
            newdate = olddate//10000*10000 + 1500
        return newdate    
    
    def getNewDate(index_type, olddate):
        if index_type == 'week':
            return getWeekDate(olddate)
        elif index_type == 'month':
            return getMonthDate(olddate)
        elif index_type == 'quarter':
            return getQuarterDate(olddate)
        elif index_type == 'halfyear':
            return getHalfyearDate(olddate)
        elif index_type == 'year':
            return getYearDate(olddate)
        elif index_type == 'min15':
            return getMin15Date(olddate)
        elif index_type == 'min30':
            return getMin30Date(olddate)
        elif index_type == 'min60':
            return getMin60Date(olddate)
        else:
            return None
    
    
    if data_type != 'day' and data_type != 'min':
        print("非法参数值data_type:", data_type)
        return
    
    print('更新 %s 扩展线索引' % filename)
    h5file = tb.open_file(filename, "a", filters=tb.Filters(complevel=9,complib='zlib', shuffle=True))
    
    if data_type == 'day':
        index_list = ('week', 'month', 'quarter', 'halfyear', 'year')
    else:
        index_list = ('min15', 'min30', 'min60')

    groupDict = {}
    for index_type in index_list:
        try:
            groupDict[index_type] = h5file.get_node("/", index_type)
        except:
            groupDict[index_type] = h5file.create_group("/", index_type)
        
    
    root_group = h5file.get_node("/data")
    table_total = root_group._v_nchildren
    table_count = 0
    for table in root_group._f_walknodes():
        table_count += 1
        ProgressBar(table_count, table_total)
        
        for index_type in index_list:
            try:
                index_table = h5file.get_node(groupDict[index_type],table.name)
            except:
                index_table = h5file.create_table(groupDict[index_type],table.name, H5Index)
    
            total = table.nrows
            if 0 == total:
                continue
    
            index_total = index_table.nrows
            index_row = index_table.row
            if index_total:
                index_last_date = int(index_table[-1]['datetime'])
                last_date = getNewDate(index_type, int(table[-1]['datetime']))
                if index_last_date == last_date:
                    continue
                startix = int(index_table[-1]['start'])
                pre_index_date = int(index_table[-1]['datetime'])
            else:
                startix = 0
                date = int(table[0]['datetime'])
                pre_index_date = getNewDate(index_type,date)
                index_row['datetime'] = pre_index_date
                index_row['start'] = 0
                index_row.append()
                #week_table.flush()
                
            index = startix
            for row in table[startix:]:
                date = int(row['datetime'])
                cur_index_date = getNewDate(index_type, date)
                if cur_index_date != pre_index_date:
                    index_row['datetime'] = cur_index_date
                    index_row['start'] = index
                    index_row.append()
                    pre_index_date = cur_index_date
                index += 1
            index_table.flush()
            
    h5file.close()
    print('\n')

if __name__ == '__main__':   
    
    import time
    starttime = time.time()
    
    src_dir = "/backup/extraHome/quantification/data"
    dest_dir = "/backup/extraHome/quantification/data"
    #dest_dir="/Volumes/Macintosh HD/Users/jungong/workspace/data"
    connect = sqlite3.connect(dest_dir + "/stock.db")
    
    
    ImportStockName(connect)
    
    
    ImportDayData(connect, dest_dir)
    #ImportMinData(connect, src_dir, dest_dir, '5min')
    #ImportMinData(connect, src_dir, dest_dir, '1min')

    #UpdateIndex(dest_dir + "\\sh_day.h5", "day")
    #UpdateIndex(dest_dir + "\\sz_day.h5", "day")
    #UpdateIndex(dest_dir + "\\sh_5min.h5", 'min')
    #UpdateIndex(dest_dir + "\\sz_5min.h5", 'min')
    #UpdateIndex(dest_dir + "\\sh_1min.h5", 'min')
    #UpdateIndex(dest_dir + "\\sz_1min.h5", 'min')

    connect.close()
    
    endtime = time.time()
    print("\nTotal time:")
    print("%.2fs" % (endtime-starttime))
    print("%.2fm" % ((endtime-starttime)/60))