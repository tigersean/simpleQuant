
import tables as tb
import pandas as pd
import numpy
import datetime
import pytz
from dateutil.parser import parse

from simpleQuant.Data.simpleTable import SimpleTable

EPOCH = datetime.datetime(1970,1,1,tzinfo=pytz.utc)

def util_hdf5_setting(uri='../data/', _groupName='DEFAULT'):

    h5fileDict = {'SHD': HDF_DAY(uri + "sh_day.h5",_groupName),
                  'SZD': HDF_DAY(uri + "sz_day.h5",_groupName),
                  'SHM5': HDF_MIN(uri + "sh_min5.h5",_groupName,
                  filters=tb.Filters(complevel=9,complib='blosc', shuffle=True)),
                  'SZM5': HDF_MIN(uri + "sz_min5.h5",_groupName,
                  filters=tb.Filters(complevel=9,complib='blosc', shuffle=True)),
                  'SHM1': HDF_MIN(uri + "sh_min1.h5",_groupName,
                  filters=tb.Filters(complevel=9,complib='blosc', shuffle=True)),
                  'SZM1': HDF_MIN(uri + "sz_min1.h5",_groupName,
                  filters=tb.Filters(complevel=9,complib='blosc', shuffle=True)),
                  'SHM15': HDF_MIN(uri + "sh_min15.h5",_groupName),
                  'SZM15': HDF_MIN(uri + "sz_min15.h5",_groupName),
                  'SHM30': HDF_MIN(uri + "sh_min30.h5",_groupName),
                  'SZM30': HDF_MIN(uri + "sz_min30.h5",_groupName),
                  'SHM60': HDF_MIN(uri + "sh_min60.h5",_groupName),
                  'SZM60': HDF_MIN(uri + "sz_min60.h5",_groupName),
                  'SHW': HDF_DAY(uri + "sh_week.h5",_groupName),
                  'SZW': HDF_DAY(uri + "sz_week.h5",_groupName),
                  'SHM': HDF_DAY(uri + "sh_month.h5",_groupName),
                  'SZM': HDF_DAY(uri + "sz_month.h5",_groupName)}
    return h5fileDict

class HDF_DAY(SimpleTable):
    datetime = tb.Int64Col(pos=0)        #IGNORE:E1101
    open = tb.Float32Col()       #IGNORE:E1101
    high = tb.Float32Col()       #IGNORE:E1101
    low = tb.Float32Col()        #IGNORE:E1101
    close = tb.Float32Col()      #IGNORE:E1101
    vol = tb.UInt64Col()     #IGNORE:E1101
    amount = tb.UInt64Col()      #IGNORE:E1101

    def save_data(self,  data):                
        def fmtdatestr(datetime):
            return int(datetime.replace('-',''))
        row = self.row
        for i, record in data.iterrows():            
            row['datetime'] = fmtdatestr(record['date'])
            row['open'] = record['open']
            row['high'] = record['high']
            row['low'] = record['low']
            row['close'] = record['close']
            row['vol'] = record['vol']
            row['amount'] = record['amount']
            row.append()
        self.flush()

class HDF_MIN(SimpleTable):
    datetime = tb.Int64Col(pos=0)        #IGNORE:E1101
    open = tb.Float32Col()       #IGNORE:E1101
    high = tb.Float32Col()       #IGNORE:E1101
    low = tb.Float32Col()        #IGNORE:E1101
    close = tb.Float32Col()      #IGNORE:E1101
    vol = tb.UInt64Col()     #IGNORE:E1101
    amount = tb.UInt64Col()      #IGNORE:E1101

    def save_data(self,  data):                
        def fmtdatestr(datetime):
            return int(datetime.replace('-',''))
        row = self.row
        for i, record in data.iterrows():            
            row['datetime'] = fmtdatestr(record['date'])
            row['open'] = record['open']
            row['high'] = record['high']
            row['low'] = record['low']
            row['close'] = record['close']
            row['vol'] = record['vol']
            row['amount'] = record['amount']
            row.append()
        self.flush()

if __name__ == '__main__':
    cl=util_hdf5_setting()
    #aa=tdx_f.fetch_get_stock_day('000001', '2013-07-01', '2013-07-09')[['date','open','high','low','close','vol','amount']]
    #cl.create_table('SH600000')
    #cl.save_data(aa)
    tb=cl['SZM']
    tb.create_table('SZ000402')
    print(tb[1:10])
    print(tb.query('datetime>20180901'))
