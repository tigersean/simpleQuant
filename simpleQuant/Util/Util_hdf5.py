
import tables as tb
import pandas as pd

from simpleQuant.Data.simpleTable import SimpleTable

def util_hdf5_setting(uri='../data/', _groupName='DEFAULT'):
    h5fileDict = {'SHD': HDF_DAY(uri + "sh_day.h5",_groupName),
                  'SZD': HDF_DAY(uri + "sz_day.h5",_groupName),
                  'SH5': HDF_DAY(uri + "sh_min5.h5",_groupName,
                  filters=tb.Filters(complevel=9,complib='blosc', shuffle=True)),
                  'SZ5': HDF_DAY(uri + "sz_min5.h5",_groupName,
                  filters=tb.Filters(complevel=9,complib='blosc', shuffle=True)),
                  'SHM1': HDF_DAY(uri + "sh_min1.h5",_groupName,
                  filters=tb.Filters(complevel=9,complib='blosc', shuffle=True)),
                  'SZM1': HDF_DAY(uri + "sz_min1.h5",_groupName,
                  filters=tb.Filters(complevel=9,complib='blosc', shuffle=True)),
                  'SHM15': HDF_DAY(uri + "sh_min15.h5",_groupName),
                  'SZM15': HDF_DAY(uri + "sz_min15.h5",_groupName),
                  'SHM30': HDF_DAY(uri + "sh_min30.h5",_groupName),
                  'SZM30': HDF_DAY(uri + "sz_min30.h5",_groupName),
                  'SHM60': HDF_DAY(uri + "sh_min60.h5",_groupName),
                  'SZM60': HDF_DAY(uri + "sz_min60.h5",_groupName),
                  'SHW': HDF_DAY(uri + "sh_week.h5",_groupName),
                  'SZW': HDF_DAY(uri + "sz_week.h5",_groupName),
                  'SHM': HDF_DAY(uri + "sh_month.h5",_groupName),
                  'SZM': HDF_DAY(uri + "sz_month.h5",_groupName)}
    return h5fileDict

class HDF_DAY(SimpleTable):
    datetime = tb.UInt64Col()        #IGNORE:E1101
    open = tb.UInt32Col()       #IGNORE:E1101
    high = tb.UInt32Col()       #IGNORE:E1101
    low = tb.UInt32Col()        #IGNORE:E1101
    close = tb.UInt32Col()      #IGNORE:E1101
    vol = tb.UInt64Col()     #IGNORE:E1101
    amount = tb.UInt64Col()      #IGNORE:E1101

    def save_data(self,  data):
        def fmtdatestr(datetime):
            return int(datetime.replace('-',''))
        #self.create_table(table_name=table_name)
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
    tb=cl['SZW']
    tb.create_table('SZ000402')
    print(tb[1:10])
