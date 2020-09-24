import pymysql
import pandas as pd
from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
import json
import calendar
from threading import Timer
from datetime import timedelta
import re

class MarketDB:
    def __init__(self):
        '''생성자: MySQL DB 연결 및 종목코드 딕셔너리 생성'''
        self.conn = pymysql.connect(host = 'localhost', user = 'naverfinance', passwd = '1234',
                                    db = 'naverfinance', charset='utf8')
        self.codes = {} # 딕셔너리 생성
        self.get_comp_info()

    def __del__(self):
        '''소멸자: MySQL DB연결 해제'''
        self.conn.close()

    def get_comp_info(self):
        '''company_info 테이블에서 읽어와서 codes 에 저장'''
        sql = "SELECT * FROM company_info"
        krx = pd.read_sql(sql, self.conn)
        for idx in range(len(krx)):
            self.codes[krx['code'].values[idx]] = krx['company'].values[idx]

    def get_daily_price(self, code, start_date=None, end_date=None):
        '''KRX 종목의 일별 시세를 데이터프레임 형태로 반환'''
        # 인수값이 주어지지 않으면 인수=None을 기본값으로 처리한다.
        # 조회 시작일로 넘겨받은 인수가 None 이면, 1년전 오늘 날짜 %Y-%m-%d 형식으로 처리
        if start_date is None:
            one_year_ago = datetime.today() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y-%m-%d')
            print("start_date is initialized to '{}'".format(start_date))
        else:
            start_lst = re.split('\D+', start_date)
            if start_lst[0] == '':
                start_lst = start_lst[1:]
            start_year = int(start_lst[0])
            start_month = int(start_lst[1])
            start_day = int(start_lst[2])
            if start_year < 1900 or start_year > 2200:
                print(f"ValueError: start_year({start_year:d}) is wrong.")
                return
            if start_month < 1 or start_month > 12:
                print(f"ValueError: start_month({start_month:d}) is wrong.")
                return
            if start_day < 1 or start_day > 31:
                print(f"ValueError: start_day({start_day:d}) is wrong.")
                return
            #분리된 연, 월, 일을 다시 다음과 같은 형식으로 구성하면 DB에 저장된 날짜 형식과 같게된다.
            start_date = f"{start_year:04d}-{start_month:02d}-{start_day:02d}"


        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
            print("end_date is initialized to '{}'".format(end_date))
        else:
            end_lst = re.split('\D+', end_date)
            if end_lst[0] == '':
                end_lst = end_lst[1:]
            end_year = int(end_lst[0])
            end_month = int(end_lst[1])
            end_day = int(end_lst[2])
            if end_year < 1800 or end_year > 2200:
                print(f"ValueError: end_year({end_year:d}) is wrong.")
                return
            if end_month < 1 or end_month > 12:
                print(f"ValueError: end_month({end_month:d}) is wrong.")
                return
            if end_day < 1 or end_day > 31:
                print(f"ValueError: end_day({end_day:d}) is wrong.")
                return
            # 분리된 연, 월, 일을 다시 다음과 같은 형식으로 구성하면 DB에 저장된 날짜 형식과 같게된다.
            end_date = f"{end_year:04d}-{end_month:02d}-{end_day:02d}"



        # 딕셔너리에서 값으로 키를 조회
        codes_keys = list(self.codes.keys()) # 딕셔너리에서 키 리스트를 생성
        codes_values = list(self.codes.values()) # 딕셔너리에서 값 리스트를 생성

        # 사용자가 입력한 code 가 키 리스트에 있으면 그대로 사용
        if code in codes_keys:
            pass
        # 사용자가 입력한 code가 값 리스트에 있으면 인덱스를 구한 뒤 키 리스트에서 동일한
        # 인덱스에 위치한 키를 찾는다.
        elif code in codes_values:
            idx = codes_values.index(code)
            code = codes_keys[idx]
        else:
            print(f"ValueError: Code({code}) doesn`t exist.")

        # 판다스의 read_sql() 함수를 이용해 select 결과를 데이터프레임에 가져오면
        # 정수형 인덱스가 별도로 생성된다.
        sql = f"SELECT * FROM daily_price WHERE code = '{code}'" \
        f" and date >= '{start_date}' and date <= '{end_date}'"
        df = pd.read_sql(sql, self.conn)
        # 데이터프레임의 인덱스를 date 컬럼으로 새로 설정해야 한다.
        df.index = df['date']
        return df
