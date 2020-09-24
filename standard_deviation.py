from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

sec = pdr.get_data_yahoo('005930.KS', start= '2018-05-04')
msft = pdr.get_data_yahoo('MSFT', start='2018-05-04')

sec_dpc = (((sec['Close'] / sec['Close'].shift(1)) - 1)*100)
sec_dpc.iloc[0] = 0

# 시리즈의 describe(메서드를 이용하면 평균과 표준편차를 확인할 수 있다.
print(sec_dpc.describe())

# 전체데이터수 : 587개
# 평균값 : 0.033460
# 표준편차 : 1.760001 이라는 것을 알 수 있다.