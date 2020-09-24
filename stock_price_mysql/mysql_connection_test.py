import pymysql
import pandas as pd
from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup

# 1.mysql에 접속되는지 확인
connection = pymysql.connect(host='localhost', port=3306, db='naverfinance',
                                user='naverfinance', passwd='1234', autocommit=True)

cursor = connection.cursor()
cursor.execute("SELECT VERSION();")
result = cursor.fetchone()

print("접속 성공 MySQL version : {}".format(result))

connection.close()

# 2. 주식 시세를 매일 DB로 업데이트하기

class DBUpdater:
    def __init__(self):
        '''생성자: DB 연결 및 종목코드 딕셔너리 생성'''

    def __del__(self):
        '''소멸자: DB연결 해제'''

    def read_krx_code(self):
        '''KRX 로부터 상장기업 목록 파일을 읽어와서 데이터프로임으로 반환'''

    def update_comp_info(self):
        '''종목코드를 company_info 테이블에 업데이트 한 후 딕셔너리에 저장'''


    def replace_into_db(self, df, num, code, company):
        '''네이버에서 읽어온 주식 시세를 DB에 REPLACE'''

    def update_daily_price(self, pages_to_fetch):
        '''KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트'''

    def execute_daily(self):
        '''실행 즉시 or 매일 오후 다섯시에 daily_price 테이블 업데이트'''




if __name__ == '__main__':
    dbu = DBUpdater()