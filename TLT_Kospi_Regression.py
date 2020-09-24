## 미국국채와 KOSPI의 회귀분석

# 동일한 방법으로 KOSPI 지수와 미국 국채의 상관관계를 살펴본다.
# 미국 국채에에 해당하는 iShares Barclays20 + Yr Treas.Bond(TLT)데이터를 다운로드 한다.
# 조회 시작일은 2002년 7월 30일이다.


import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

from scipy import stats
import matplotlib.pylab as plt

tlt = pdr.get_data_yahoo('TLT', '2002-07-30') # TLT다운로드
dow = pdr.get_data_yahoo('^DJI', '2000-01-04') # 2000년 이후 다우존스 지수(^DJI)데이터 다운
kospi = pdr.get_data_yahoo('^KS11', '2000-01-04') # 2000년 이후 KOSPI(^DJI)데이터 다운

# 다우존스 지수의 종가 컬럼과 KOSPI 지수의 종가 컬럼으로 데이터프레임 생성
df = pd.DataFrame({'X':tlt['Close'], 'Y':kospi['Close']})
df = df.fillna(method='bfill')
df = df.fillna(method='ffill')

regr = stats.linregress(df.X, df.Y) #선형회귀모델 객체 regr 생성
regr_line = f'Y = {regr.slope:.2f} X + {regr.intercept:.2f}' # 범례에 회귀선을 표시하는 문자

plt.figure(figsize=(7,7))
plt.plot(df.X, df.Y, '*') # 산점도를 '*'으로 표시
plt.plot(df.X, regr.slope * df.X + regr.intercept, 'r') # 회귀선을 붉은 색으로 표시
plt.legend(['TLT x KOSPI', regr_line])
plt.title(f'TLT x KOSPI (R = {regr.rvalue:.2f})')
plt.xlabel('iShares Barclays 20 + Yr Treas.Bond(TLT)')
plt.ylabel('KOSPI')
plt.show()


# ETF 데이터로 분석한 결과 미국 국채와 KOSPI 지수 상관계수는 0.74정도 이다.
# 다우존스 지수와 비교한 것에 비해 살짝 낮다. 큰 차이는 아니지만 국내 주식에 이미 투자를 하고 있다면
# 다우존스 지수에 분산투자 하는 것보다 미국 채권에 분산 투자 하는 것이 조금이라도 리스크 완화에 도움이
# 된다는 의미이다.

