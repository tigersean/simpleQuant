
import tables as tb

from simpleQuant.Data.simpleTable import SimpleTable

def util_hdf5_setting(uri='day.h5', _groupName='SH', _tableName=''):
    client=HDF_DAY("day.h5", table_name=_tableName, group_name=_groupName )
    return client

class HDF_DAY(SimpleTable):
    datetime = tb.UInt64Col()        #IGNORE:E1101
    open = tb.UInt32Col()       #IGNORE:E1101
    high = tb.UInt32Col()       #IGNORE:E1101
    low = tb.UInt32Col()        #IGNORE:E1101
    close = tb.UInt32Col()      #IGNORE:E1101
    vol = tb.UInt64Col()     #IGNORE:E1101
    amount = tb.UInt64Col()      #IGNORE:E1101
    
if __name__ == '__main__':
    cl=util_hdf5_setting(_tableName='600000')
    row = cl.row
    for i in range(50):
       row['open']=i*1
       row['close']= i*3
       row['vol']=i*100
       row.append()
    tbl.flush()