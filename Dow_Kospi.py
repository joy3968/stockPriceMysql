## 회귀 분석과 상관 관계

# 회귀 분석은 데이터의 상관관계를 분석하는 데 쓰이는 통계 분석 방법이다. 회귀 분석은 회귀 모형을
# 설정한 후 실제로 관측된 표본을 대상으로 회귀 모형의 계수를 추정한다.
# 독립변수라고 불리는 하나 이상의 변수와 종속 변수라 불리는 하나의 변수 간의 관계를
# 나타내는 회귀식이 도출된다면, 임의의 독립 변수에 대하여 종속 변숫값을 추측해 볼 수 있는데
# 이를 예측(prediction)이라 한다.

## KOSPI와 다우존스 지수 비교
# 다우존스 지수 : 찰스 다우가 창안한 주가 지수로서 미국 증권거래소에 상장된 30개 우량기업으로 구성
# 시가 총액이 아닌 주가 평균 방식으로 계산하기 때문에 주가가 큰 종목의 움직임에 민감하게 반응한다.
# 주가 기준이 달라서, 어느 지수가 더 좋은 성과를 냈는지 한눈에 보기 어렵다.

from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

dow = pdr.get_data_yahoo('^DJI', '2000-01-04') # 2000년 이후 다우존스 지수(^DJI)데이터 다운
kospi = pdr.get_data_yahoo('^KS11', '2000-01-04') # 2000년 이후 KOSPI(^DJI)데이터 다운

import matplotlib.pyplot as plt

plt.figure(figsize=(9,5))
plt.plot(dow.index, dow.Close, 'r--', label='Dow Jones Industrial') # 붉은 점선
plt.plot(kospi.index, kospi.Close, 'b', label='KOSPI') # 푸른 실선
plt.grid(True)
plt.legend(loc='best')
plt.show()