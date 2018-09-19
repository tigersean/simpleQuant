
import tables as tb

from simpleQuant.Data.simpleTable import SimpleTable

def util_hdf5_setting(uri='day.h5', _groupName='SH', _tableName=''):
    client=HDF_DAY("day.h5", table_name=_tableName, groupName=_groupName )
    return client

class HDF_DAY(SimpleTable):
    datetime = tb.UInt64Col()        #IGNORE:E1101
    open = tb.UInt32Col()       #IGNORE:E1101
    high = tb.UInt32Col()       #IGNORE:E1101
    low = tb.UInt32Col()        #IGNORE:E1101
    close = tb.UInt32Col()      #IGNORE:E1101
    vol = tb.UInt64Col()     #IGNORE:E1101
    amount = tb.UInt64Col()      #IGNORE:E1101
    
