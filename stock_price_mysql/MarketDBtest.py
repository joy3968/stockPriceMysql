import stock_price_mysql.Analyzer

mk = stock_price_mysql.Analyzer.MarketDB()
print(mk.get_daily_price('토탈소프트', '2020-09-20', '2020.09.23'))
