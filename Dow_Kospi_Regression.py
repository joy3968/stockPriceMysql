## 다우존스 지수와 KOSPI의 회귀분석

# 앞에서 다우존스 KOSPI의 상관관계를 산점도를 이용해 분석했다.
# 이번에는 스탯츠 모델 linregress() 함수를 이용하여 선형회귀 모델을 생성한뒤
# 회귀선을 그려서 분석한다.

import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

from scipy import stats
import matplotlib.pylab as plt

dow = pdr.get_data_yahoo('^DJI', '2000-01-04') # 2000년 이후 다우존스 지수(^DJI)데이터 다운
kospi = pdr.get_data_yahoo('^KS11', '2000-01-04') # 2000년 이후 KOSPI(^DJI)데이터 다운

# 다우존스 지수의 종가 컬럼과 KOSPI 지수의 종가 컬럼으로 데이터프레임 생성
df = pd.DataFrame({'X':dow['Close'], 'Y':kospi['Close']})
df = df.fillna(method='bfill')
df = df.fillna(method='ffill')

regr = stats.linregress(df.X, df.Y) #선형회귀모델 객체 regr 생성
regr_line = f'Y = {regr.slope:.2f} X + {regr.intercept:.2f}' # 범례에 회귀선을 표시하는 문자

plt.figure(figsize=(7,7))
plt.plot(df.X, df.Y, '.') # 산점도를 '.' 으로 표시
plt.plot(df.X, regr.slope * df.X + regr.intercept, 'r') # 회귀선을 붉은 색으로 표시
plt.legend(['DOW x KOSPI', regr_line])
plt.title(f'DOW x KOSPI (R = {regr.rvalue:.2f})')
plt.xlabel('Dow Jones Industrial Average')
plt.ylabel('KOSPI')
plt.show()


