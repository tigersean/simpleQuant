
import tables as tb
import pandas as pd

from simpleQuant.Data.simpleTable import SimpleTable

def util_hdf5_setting(uri='day.h5', _groupName='SH'):
    client=HDF_DAY(uri, group_name=_groupName )
    return client

class HDF_DAY(SimpleTable):
    datetime = tb.UInt64Col()        #IGNORE:E1101
    open = tb.UInt32Col()       #IGNORE:E1101
    high = tb.UInt32Col()       #IGNORE:E1101
    low = tb.UInt32Col()        #IGNORE:E1101
    close = tb.UInt32Col()      #IGNORE:E1101
    vol = tb.UInt64Col()     #IGNORE:E1101
    amount = tb.UInt64Col()      #IGNORE:E1101

    def save_data(self,dataframe):        
        pass

    
if __name__ == '__main__':
    cl=util_hdf5_setting()
    cl.create_table('SH600000')
    row = cl.row
    for i in range(50):
       row['open']=i*1
       row['close']= i*3
       row['vol']=i*100
       row.append()
    cl.flush()
    print(cl.datetime)
    print(cl.read_where('close>5'))