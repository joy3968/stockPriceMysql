# 일간변동률을 구하면 가격이 다른 두 주가의 수익률을 비교할 수 있다.

# 오늘 변동률 = {(오늘종가 - 어제종가) / 어제종가 }* 100
# 어제의 종가란 정확히 말하자면 이전 거래일의 종가를 의미한다.
# 데이터프레임의 종가 컬럼인 sec['Close']로 확인해본다.

from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

sec = pdr.get_data_yahoo('005930.KS', start= '2018-05-04')
msft = pdr.get_data_yahoo('MSFT', start='2018-05-04')

# print(type(sec['Close']))
# print()
# print(sec['Close'])

# 액면분할 첫날인 5월 4일에는 51900원이다. shift() 함수를 이용하여 한칸씩 뒤로 밀어준다.
# print(sec['Close'].shift(1))

# 2018-05-04일은 전날 데이터가 존재하지 않으므로 NaN(Not a Number)으로 표시되고,
# 5월 8일의 이전 거래일 종가가 5월 4일 종가 51900원으로 표시된다.

# 오늘 일간 변동률을 구한다.
# sec_dpc = (sec['Close'] / sec['Close'].shift(1) - 1) * 100
# print(sec_dpc.head())
sec_dpc = (((sec['Close'] / sec['Close'].shift(1)) - 1)*100)
# print(sec_dpc)
#
# print(sec_dpc.head())

# 첫째날 일간 변동률의 값이 NaN을 0으로 변경해준다.
# 0번째 인덱스의 행을 0으로 바꾸기
sec_dpc.iloc[0] = 0

print('삼성전자 일간 변동률')
print(sec_dpc.head())

print()
print('마이크로소프트 일간 변동률')
msft_dpc = (((msft['Close'] / msft['Close'].shift(1)) - 1)*100)
msft_dpc.iloc[0] = 0
print(msft_dpc.head())



## 주가 일간 변동률 히스토그램
# 삼성전자의 일간 변동률을 18개 구간으로 빈도수를 표시한다.

import matplotlib.pyplot as plt

plt.hist(sec_dpc, bins=18)
plt.grid(True)
plt.show()


# 차트해석 : 삼성전자 일간 변동률 분초가 0 bin을 기준으로 좌우 대칭적이다.
# 정규분포 형태와 비슷하다. 주가 수익률은 정규분포보다 중앙부분이 더 뾰족하고 분포의 양쪽
# 꼬리는 더 두터운 것으로 알려져 있다. 이를 각각 급첨 분포와 팻 테일 이라 부른다.

# 주가 수익률이 급첨 분포를 나타낸다는 것은 정규 분포와 비교했을 때 주가의 움직임이
# 대부분 매우 작은 범위 안에서 발생한다는 것을 의미한다.
# 그리고 두꺼운 꼬리를 가리키는 팻 테일은 그래프의 좌우 극단 부분에 해당하는 아주 큰 가격 변동이
# 정규분포보다 더 많이 발생한다는 의미이다.

# 시리즈의 describe()메서드를 이용하여 평균과 표준편차를 확인할 수 있다.

