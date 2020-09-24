## 일간 변동률 누적합

from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

sec = pdr.get_data_yahoo('005930.KS', start= '2018-05-04')
msft = pdr.get_data_yahoo('MSFT', start='2018-05-04')

sec_dpc = (((sec['Close'] / sec['Close'].shift(1)) - 1)*100)
sec_dpc_cs = sec_dpc.cumsum()
print(sec_dpc_cs)