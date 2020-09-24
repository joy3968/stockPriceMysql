from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()

sec = pdr.get_data_yahoo('005930.KS', start= '2018-05-04')
msft = pdr.get_data_yahoo('MSFT', start='2018-05-04')

sec_dpc = (((sec['Close'] / sec['Close'].shift(1)) - 1)*100)
sec_dpc.iloc[0] = 0
sec_dpc_cs = sec_dpc.cumsum()
# print(sec_dpc_cs)
# 조회 기간동안에 수익률의 누적합은 19% 이다

sec_dpc2 = (((sec['Close'] - sec['Close'][0]) / sec['Close'][0]) * 100)

# 실제 조회 기간동안에 수익률
print(((sec['Close'][-1] - sec['Close'][0]) / sec['Close'][0])*100)


# 차이가 나는 이유 : 떨어질때의 %가 더 크게 느껴진다.
# 즉 변동률의 누적합은 과대평가 된다.

msft_dpc = (((msft['Close'] / msft['Close'].shift(1)) - 1)*100)
msft_dpc.iloc[0] = 0
msft_dpc_cs = msft_dpc.cumsum()

msft_dpc2 = (((msft['Close'] - msft['Close'][0]) / msft['Close'][0]) * 100)

import matplotlib.pyplot as plt

plt.plot(sec.index, sec_dpc2, 'b', label='Samsung Electronic')
plt.plot(msft.index, msft_dpc2, 'r--', label='Microsoft')
plt.ylabel('Change %')
plt.grid(True)
plt.legend(loc='best')
plt.show()

# print(sec_dpc2)
# print(sec['Close'])


# print(msft['Close'][0], msft['Close'][-1])




