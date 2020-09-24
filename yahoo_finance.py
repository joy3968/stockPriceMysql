# 1장 판다스를 활용한 데이터 분석
# 1. 야후 파이낸스로 주식 시세 분석

# 수정 종가는 액면 분할 등으로 인해 주식 가격에 변동이 있을 경우 가격 변동 이전에
# 거래된 가격을 현재 주식 가격에 맞춰 수정하여 표시한 가격이다. 액면 분할 이후에는 종가(Close)와
# 수정 종가가 동일해야 하는데 국내 주식에 대한 액면 분할 처리가 제대로 되지 않아서 수정 종가가 잘못
# 나와있다. 여기서는 종가(Close)만 사용한다.

import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf

sec = pdr.get_data_yahoo('005930.KS', start= '2018-05-04')
msft = pdr.get_data_yahoo('MSFT', start='2018-05-04')

# print(sec.head(10))
# 거래량 컬럼은 없애준다.
tmp_msft = msft.drop(columns='Volume')
print(tmp_msft.tail())

# 데이터 프레임 구성을 확인하기 위해 인덱스를 확인한다.
# 데이터프레임의 컬럼 정보는 columns 속성으로 확인

print(sec.index)
print()
print(sec.columns)

