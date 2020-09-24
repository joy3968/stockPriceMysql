import pymysql
import pandas as pd
from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
import json
import calendar
from threading import Timer
import urllib, time

# # 1.mysql에 접속되는지 확인
# connection = pymysql.connect(host='localhost', port=3306, db='naverfinance',
#                                 user = 'naverfinance', passwd='1234', autocommit=True)
#
# cursor = connection.cursor()
# cursor.execute("SELECT VERSION();")
# result = cursor.fetchone()
#
# print("접속 성공 MySQL version : {}".format(result))
#
# connection.close()

# 2. 주식 시세를 매일 DB로 업데이트하기

class DBUpdater:
    def __init__(self):
        '''생성자: DB 연결 및 종목코드 딕셔너리 생성'''
        self.conn = pymysql.connect(host = 'localhost', user = 'naverfinance', passwd = '1234',
                                    db = 'naverfinance', charset='utf8')

        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS company_info (
                code VARCHAR(20),
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (code))
                """
            curs.execute(sql)
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price(
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date))
            """
            curs.execute(sql)
        self.conn.commit()
        self.codes = dict()
        # update_comp_info()메서드로 KRX 주식 코드를 읽어와서 company_info 테이블에
        # 업데이트 한다.
        self.update_comp_info()


    def __del__(self):
        '''소멸자: DB연결 해제'''

    def read_krx_code(self):
        '''KRX 로부터 상장기업 목록 파일을 읽어와서 데이터프로임으로 반환'''
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드': 'code', '회사명': 'company'}) #한글 컬럼명을 영문 컬럼명으로 변경
        krx.code = krx.code.map('{:06d}'.format) # 종목코드 형식을 {:06d} 형식 문자열로 변경(빈칸 0으로 채움)
        return krx

    def update_comp_info(self):
        '''종목코드를 company_info 테이블에 업데이트 한 후 딕셔너리에 저장'''
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            # 종목코드와 회사명으로 codes 딕셔너리를 만든다.
            self.codes[df['code'].values[idx]] = df['company'].values[idx]

        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone() # 가장 최근에 업데이트 된 날짜를 가져온다.
            today = datetime.today().strftime('%Y-%m-%d')
            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code() # KRX 상장기업목록 파일을 krx 데이터프레임에 저장
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    # REPLACE INTO 구문으로 '종목코드, 회사명, 오늘날짜' 행을 DB에 저장
                    sql = f"REPLACE INTO company_info (code, company, last_update) VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    # codes 딕셔너리에 '키-값'으로 종목코드와 회사명을 추가
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M') # 오늘 날짜
                    print(f"[{tmnow}]) #{idx + 1:04d} REPLACE INTO company_info VALUES ({code},{company}, {today})")

                self.conn.commit()
                print('')

    def read_naver(self, code, company, pages_to_fetch):
        ''' 네이버에서 주식 시세를 읽어서 데이터프레임으로 반환'''
        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            with urlopen(url) as doc:
                if doc is None:
                    return None
                html = BeautifulSoup(doc, "lxml")
                pgrr = html.find("td", class_="pgRR")
                if pgrr is None:
                    return None
                s = str(pgrr.a["href"]).split('=')
                lastpage = s[-1]  # 일별 시세의 마지막 페이지를 가져온다.
            df = pd.DataFrame()
            # 설정 파일에 설정된 페이지 수(pages_to_fetch)와 마지막 페이지(lastpage) 수에서 작은 것을 택한다.

            pages = min(int(lastpage), pages_to_fetch)



            for page in range(1, pages + 1):
                pg_url = '{}&page={}'.format(url, page)
                # 일별 시세 페이지를 데이터프레임에 추가한다.
                df = df.append(pd.read_html(pg_url, header=0)[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'
                      .format(tmnow, company, code, page, pages), end="\r")
            df = df.rename(columns={'날짜':'date', '종가':'close', '전일비':'diff',
                                    '시가':'open', '고가':'high', '저가':'low', '거래량':'volume'})
            df['date'] = df['date'].replace('.','-')
            df = df.dropna()
            # MySQL 에서 BIGINT 형으로 지정한 컬럼들의 데이터 형을 int로 변경한다.
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close', 'diff', 'open', 'high', 'low',
                                                                         'volume']].astype(int)
            # 원하는 순서로 다시 조합한다.
            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
        except Exception as e:
            print('Exception occured :', str(e))
            return None
        return df



    def replace_into_db(self, df, num, code, company):
        '''네이버에서 읽어온 주식 시세를 DB에 REPLACE'''
        with self.conn.cursor() as curs:
            # 인수로 넘겨받은 데이터프레임을 튜플로 반복 처리한다.
            for r in df.itertuples():
                # replace into 로 daily_price 테이블을 업데이트한다.
                sql = f"REPLACE INTO daily_price VALUES ('{code}', " \
                f"'{r.date}', {r.open}, {r.high}, {r.low}, {r.close}," \
                f"{r.diff}, {r.volume})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_' \
                  'price [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), num + 1, company,
                                      code, len(df)))

    def update_daily_price(self, pages_to_fetch):
        '''KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트'''
        # self.codes 딕셔너리에 저장된 모든 종목코드에 대해 반복 처리한다.
        for idx, code in enumerate(self.codes):
            # 종목코드에 대한 일별 시세 데이터프레임을 구한다.
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])


    def execute_daily(self):
        '''실행 즉시 or 매일 오후 다섯시에 daily_price 테이블 업데이트'''
        # 상장 법인 목록을 DB에 업데이트 한다.
        self.update_comp_info()

        try:
            # config.json 파일을 읽기 모드로 오픈한다.
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                # config.json 파일이 있다면 pages_to_fetch 값을 읽어온다.
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            # config.json 파일이 없다면
            with open('config.json', 'w') as out_file:
                # pages_to_fetch 값을 100으로 설정한다.
                pages_to_fetch = 100
                config = {'pages_to_fetch': 1} # 이후부터는 1페이지
                json.dump(config, out_file)
        self.update_daily_price(pages_to_fetch)

        tmnow = datetime.now()
        # 이번달의 마지막 날을 구해 다음날 오후 5시를 계산하는데 사용.
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year + 1, month=1, day=1,
                                   hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month + 1, day=1, hour=17,
                                   minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day + 1, hour=17, minute=0,
                                   second=0)
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds

        # 다음날 오후 5시에 execute_daily() 메서드를 실행하는 타이머 객체를 생성
        t = Timer(secs, self.execute_daily)
        print("Wating for next update ({}) ...".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()


if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()

