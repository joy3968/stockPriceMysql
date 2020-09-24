## 최대손실 낙폭

# MDD(Maximum Drawdown, 최대손실 낙폭)은 특정 기간에 발생한 최고점에서 최저점까지의 가장 큰
# 손실을 의미한다. 퀀트 투자에서 수익률을 높이는 것보다 MDD를 낮추는 것이 더 중요하다.
# 특정 기간 동안 최대한 얼마의 손실이 일어날 수 있는지를 나타낸다.

# MDD = (최저점 - 최고점) / 최저점

from pandas_datareader import data as pdr
import yfinance as yf

yf.pdr_override()
import matplotlib.pyplot as plt

# KOSPI 지수의 심볼은 ^KS11 이다.
kospi = pdr.get_data_yahoo('^KS11', '2004-01-04')

# 산정 기간에 해당하는 window 값은 1년 동안 개장일을 252일로 설정한다.
window = 252
# KOSPI 종가 컬럼에서 1년(거래일 기준) 기간 단위로 최고치 peak를 구한다.
peak = kospi['Adj Close'].rolling(window, min_periods=1).max()

# drawdown 은 최고치(peak) 대비 현재 KOSPI 종가가 얼마나 하락 했는지 구한다.
drawdown = kospi['Adj Close'] / peak - 1.0

# drawdown 에서 1년 기간 단위로 최저치 min_dd 를 구한다.
# 마이너스값이기 때문에 최저치가 바로 최대 손실 낙폭이 된다.

### rolling -> 앞에 있는 숫자 만큼씩 묶어서 계산(7일선, 30일선 등과 같은 걸 계산할 때)
max_dd = drawdown.rolling(window, min_periods=1).min()

print(max_dd.min()) # MDD 의 최저치

plt.figure(figsize=(9, 7))
plt.subplot(211) # 2행 1열 중 1행에 그린다.
kospi['Close'].plot(label='KOSPI', title='KOSPI MDD', grid=True, legend=True)
plt.subplot(212) # 2행 2열 중 2행에 그린다.
drawdown.plot(c='blue', label='KOSPI DD', grid=True, legend=True)
max_dd.plot(c='red', label='KOSPI MDD', grid=True, legend=True)
plt.show()


# 서브프라임 금융위기 당시였던 2008년 10월 24일에 KOSPI 지수가 10.57% 하락하면서
# MDD가 -54.5%를 기록했다 MDD를 min()함수로 구한다.

print(max_dd[max_dd == -0.5453665130144085])

# 2008-10-24 일부터 2009-10-22일까지 1년(252거래일)동안 주어진 max_dd와 일치했다.