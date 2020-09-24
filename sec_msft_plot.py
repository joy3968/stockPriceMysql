# 삼성전자와 마이크로소프트의 종가 데이터를 그래프로 출력하기

import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

sec = pdr.get_data_yahoo('005930.KS', start= '2018-05-04')
msft = pdr.get_data_yahoo('MSFT', start='2018-05-04')

import matplotlib.pyplot as plt


# plot(x, y, 마커형태, label='라벨')
# 'b'는 블루이고, 'best'는 그래프가 표시되지 않은 부분을 찾아서
# 적절한 위치에 범례를 표시해 준다.
plt.plot(sec.index, sec.Close, 'b', label='Samsung Electronic')
plt.plot(msft.index, msft.Close, 'r--', label='Microsoft')
plt.legend(loc='best')
plt.show()

# -> 50000원 대의 삼성전자와 130달러의 마이크로소프트 주가를 같이 표시하니 마이크로 소프트가
# 거의 0에 수렴하는 직선으로 표시된다. 이렇게 되면 주식별 수익률 비교가 어렵기 때문에
# 일간변동률을 이용해서 비교해 준다.
