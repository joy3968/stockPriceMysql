from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

dow = pdr.get_data_yahoo('^DJI', '2000-01-04') # 2000년 이후 다우존스 지수(^DJI)데이터 다운
kospi = pdr.get_data_yahoo('^KS11', '2000-01-04') # 2000년 이후 KOSPI(^DJI)데이터 다운

d = (dow.Close / dow.Close.loc['2000-01-04']) * 100
k = (kospi.Close / kospi.Close.loc['2000-01-04']) * 100

import matplotlib.pyplot as plt
plt.figure(figsize=(9,5))

plt.plot(d.index, d, 'r--', label='Dow Jones Industrial') # 붉은 점선
plt.plot(k.index, k, 'b', label='KOSPI') # 푸른 실선
plt.grid(True)
plt.legend(loc='best')
plt.show()

# 지난 20년간 KOSPI의 상승률이 다우존스 지수 상승률과 비슷함을
# 확인할 수 있다.