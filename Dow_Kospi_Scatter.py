## 3.3 산점도 분석

# 다우존스와 KOSPI의 관계를 분석하는데 산점도를 사용한다.
# 산점도란 독립 변수 x와 종속 변수 y의 상관관계를 확인할 때 사용하는 그래프이다.
# x를 다우존스 지수로, y를 KOSPI로 정한다.

# 데이터프레임의 fillna()함수를 사용하여 NaN을 채울 수 있는데, 인수로 bfill(backward fill)
# 이면 NaN뒤에 있는 값으로 NaN을 덮어써서 NaN을 제거할 수 있다.
# 그리고 마지막 행에 NaN이 있으면 ffill(forward fill)인수로 fillna() 함수를
# 한번 더 호출함으로써 제일 마지막 행의 NaN을 덮어써 제거할 수 있다.

import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

dow = pdr.get_data_yahoo('^DJI', '2000-01-04') # 2000년 이후 다우존스 지수(^DJI)데이터 다운
kospi = pdr.get_data_yahoo('^KS11', '2000-01-04') # 2000년 이후 KOSPI(^DJI)데이터 다운

df = pd.DataFrame({'DOW':dow['Close'], 'KOSPI':kospi['Close']})
# print(df)
# 뒤에값을 앞에 가져옴
df = df.fillna(method='bfill')
# print(df)
# 이전의 값을 앞에 가져옴
df = df.fillna(method='ffill')
# print(df)

import matplotlib.pyplot as plt
plt.figure(figsize=(7,7))
plt.scatter(df['DOW'], df['KOSPI'], marker='.')
plt.xlabel('Dow Jones Industrial Average')
plt.ylabel('KOSPI')
plt.show()

# 점의 분포가 y=x인 직선 형태에 가까울 수록 직접적인 관계가 있다고 볼 수 있는데
# 다우존스 지수와 KOSPI는 어느정도 영향을 미치긴 하지만 그리 강하지는 않다.
# 산점도 그래픽만으로는 분석이 어려우므로 선형 회귀 분석으로 더 정확히 분석한다.