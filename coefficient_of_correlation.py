## 상관계수에 따른 리스크 완화

# 데이터프레임으로 상관계수 구하기
# 데이터프레임은 상관계수를 구하는 corr()함수를 제공한다.

import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

dow = pdr.get_data_yahoo('^DJI', '2000-01-04') # 2000년 이후 다우존스 지수(^DJI)데이터 다운
kospi = pdr.get_data_yahoo('^KS11', '2000-01-04') # 2000년 이후 KOSPI(^DJI)데이터 다운

# 다우존스 지수의 종가 컬럼과 KOSPI 지수의 종가 컬럼으로 데이터프레임 생성
df = pd.DataFrame({'DOW':dow['Close'], 'KOSPI':kospi['Close']})

# print(df.corr())

## 시리즈로 상관계수 구하기
# print(df['DOW'].corr(df['KOSPI'])) # df.DOW.corr(df.KOSPI) 와 같다.


## 결정계수
# 결정계수 (R-squared)는 관측된 데이터에서 추정한 회귀선이 실제로 데이터를 어느 정도
# 설명하는지를 나타내는 계수로, 두 변수간의 상관관계 정도를 나타내는
# 상관계수(R-value)를 제곱한 값이다.

r_value = df['DOW'].corr(df['KOSPI'])
print(r_value)

r_squared = r_value ** 2
print(r_squared)

# 결정계수는 상관계수를 제곱해서 구한다.