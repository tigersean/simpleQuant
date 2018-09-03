import re

import pandas as pd

#个股资金快照

ggzj='http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?\
type=ct&st=(BalFlowMain)&sr=-1&p=1&ps=4000&js=&\
token=894050c76af8597a853f5b408b759f5d&cmd=C._AB&sty=DCFFITA&rt=51144985'

#行业版块资金

bkzj="http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?\
type=CT&cmd=C._BKHY&st=(BalFlowMain)&sr=-1&p=1&ps=100&js=\
&token=894050c76af8597a853f5b408b759f5d&cb=&sty=DCFFITA&rt=51144985"

#概念版块资金

gnzj="http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?\
type=CT&cmd=C._BKGN&st=(BalFlowMain)&sr=-1&p=1&ps=300&\
js=&token=894050c76af8597a853f5b408b759f5d&cb=&sty=DCFFITA&rt=l51144985"

#版块个股资金code+markt

bklb="http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?\
type=ct&st=(BalFlowMain)&sr=-1&p=1&ps=500&js=\
&token=894050c76af8597a853f5b408b759f5d&sty=DCFFITA&rt=51181654&cmd=C."

#版块、个股历史资金流 code+market

gglszj="http://ff.eastmoney.com/EM_CapitalFlowInterface/api/js?\
type=hff&rtntype=2&js=&cb=&check=TMLBMSPROCR\
&acces_token=1942f5da9b46b069953c873404aad4b5&_=1535525712559&id="

#个股、版块分钟资金流code+maket

fzzjl="http://ff.eastmoney.com/EM_CapitalFlowInterface/api/js?type=ff\
&check=MLBMS&cb=&js=&rtntype=3&acces_token=1942f5da9b46b069953c873404aad4b5\
&_=1535450561774&id="

def get_east_fund_snapshot(uri):   
   req=urlopen(uri,timeout=60).read()
   lines=req.decode()   
   rst=re.findall('[\[](.*?)[\]]',lines)[0]
   ax=rst.replace("-,","0.00,").replace('-"','0.00"')[1:-1].split('","')   
   return pd.DataFrame((x.split(',') for x in ax), dtype=float,\
             columns=['market','code','name',\
                 'price','changeratio',\
                 'r0_net','r0_ratio',\
                 'r1_net','r1_ratio',\
                 'r2_net','r2_ratio',\
                 'r3_net','r3_ratio',\
                 'r4_net','r4_ratio',\
                 'date','rises'])

 

def get_east_fund_fz(uri):
  pass

   

if __name__ == '__main__':
  ggdf=get_east_fund_snapshot(ggzj)
  gndf=get_east_fund_snapshot(gnzj)
  bkdf=get_east_fund_snapshot(bkzj)