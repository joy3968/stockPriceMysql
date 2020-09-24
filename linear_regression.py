## 선형 회귀 분석

# 회귀 모델이란 연속적인 데이터 Y와 이 Y의 원인이 되는 X간의 관계를
# 추정하는 관계식을 의미한다. 실제로 데이터 값에는 측정상의 한계로 인한 잡음(noise)
# 이 존재하기 때문에 정확한 관계식을 표현하는 확률 변수의 오차항을 둔다.

# 사이파이 패키지의 서브 패키지인 stats 는 다양한 통계 함수를 제공한다.
# linregress() 함수를 이용하면 시리즈 객체 두 개만으로 간단히 선형 회귀 모델을 생성하여
# 분석할 수 있다.

import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

dow = pdr.get_data_yahoo('^DJI', '2000-01-04') # 2000년 이후 다우존스 지수(^DJI)데이터 다운
kospi = pdr.get_data_yahoo('^KS11', '2000-01-04') # 2000년 이후 KOSPI(^DJI)데이터 다운

df = pd.DataFrame({'DOW':dow['Close'], 'KOSPI':kospi['Close']})
# 뒤에값을 앞에 가져옴
df = df.fillna(method='bfill')
# 이전의 값을 앞에 가져옴
df = df.fillna(method='ffill')

from scipy import stats
regr = stats.linregress(df['DOW'], df['KOSPI'])
print(regr)

# slope : 기울기
# intercept : y 절편
# rvalue : r값(상관계수)
# pvalue : p값
# stderr : 표준편차

# stats 에서 생성한 모델을 이용하면 선형회귀식을 구할 수 있다.
# 기울기(slope)가 약 0.08이고 y절편이 444.48이므로
# Y의 기대치는 E(Y) = 444.48 + 0.08 * x 로 나타낼 수 있다.
# 따라서 임의의 x값이 주어질 경우 이에 해당하는 y값을 예측할 수 있다.