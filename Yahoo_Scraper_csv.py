import pandas as pd
import yfinance as yf 
import os, time
from datetime import date

def Scraping(Ticket):

    for i in Ticket:
        try:
            path = 'Database/' + i
            if not os.path.isdir(path):
                os.mkdir('Database/' + i)

            print(i + ' Downloading')

            data = yf.download(i, start="2015-01-01")                                                    #下載資料, 設定起始及終結時間, end = date.today()
            data.to_csv('Database/' + i + '/' + i + '.csv')
            pd.read_csv('Database/' + i + '/' + i + '.csv', parse_dates = ['Date'], index_col= 'Date')            #轉換日期格式
            data['Open'] = (data['Adj Close']/ data['Close'] * data['Open'])                             #因應派息等因素對Open, High, Low作出調整
            data['High'] = (data['Adj Close']/ data['Close'] * data['High'])
            data['Low'] = (data['Adj Close']/ data['Close'] * data['Low'])
            data['Mid'] = (data['High']+ data['Low'])/2                                                  #計算中位數
            data.to_csv('Database/' + i + '/' + i + '.csv')
            time.sleep(1)

        except:
            print(i + 'Error')

if __name__ == '__main__':
    watchlist = pd.read_csv('watchlist.csv', index_col = 0, encoding = 'Big5')
    Ticket = watchlist.index
    Scraping(Ticket)
