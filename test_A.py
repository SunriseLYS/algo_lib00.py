# -*- coding: utf-8 -*-
from __future__ import print_function
import pandas as pd
import timejob as tj
pd.set_option('display.max_columns', 150)       #pandas setting 顥示列數上限
pd.set_option('display.width', 5000)           #pandas setting 顯示列的闊度
pd.set_option('display.max_colwidth',20)      #pandas setting 每個數據顥示上限
pd.set_option('display.max_rows', 5000)       #pandas setting 顯示行的闊度
pd.options.mode.chained_assignment = None
from futu import *
import mysql.connector
from mysql.connector import Error


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def data_check():
    connection = create_server_connection('103.68.62.116', 'root', '630A78e77?')
    cursor = connection.cursor()

    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()
    symbol = symbol[:1]
    symbol_dict = {i: i.replace('.', '_') for i in symbol}  # 轉變成Dictionary

    for i in symbol_dict:
        try:

            sql = "USE %s" % (symbol_dict[i])
            cursor.execute(sql)

            '''
            sql = "DELETE FROM Day WHERE date='2023-01-16'"
            cursor.execute(sql)
            connection.commit()

            sql = "DELETE FROM Mins WHERE time_key>'2023-01-15'"
            cursor.execute(sql)
            connection.commit()   
            sql = "DROP TABLE IF EXISTS 2023_01_16"
            cursor.execute(sql)
            connection.commit()'''

        except:
            pass

        print(i)
        # df = pd.read_sql("SELECT * FROM Mins", connection)
        # print(df.tail(3))

def recalculate(DB, symbol):
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    symbol_dict = {i: i.replace('.', '_') for i in symbol}   # 轉變成Dictionary
    for stock_i in symbol_dict:
        ret, rehab_data = quote_ctx.get_rehab(stock_i)
        if ret == RET_OK:
            pass
        else:
            print('error:', rehab_data)

        if len(rehab_data) > 1:
            rehab_date = rehab_data['ex_div_date'][len(rehab_data) - 2]
            rehab_a = rehab_data['forward_adj_factorA'][len(rehab_data) - 2]
            rehab_b = rehab_data['forward_adj_factorB'][len(rehab_data) - 2]
            print(stock_i)
            print(rehab_date)

            sql = "CREATE TABLE Ex(rehab_date DATE, factor_a FLOAT, factor_b FLOAT, UNIQUE INDEX date(rehab_date))"
            DB.creat_table(symbol_dict[stock_i], sql)

            DB.data_input_tailor(symbol_dict[stock_i], 'Ex', '("%s", factor_a, factor_b)' % (rehab_date))

            DB.date_update(symbol_dict[stock_i], 'Day', 'open = open * %s + %s' % (rehab_a, rehab_b),
                           'date < "%s"' % (rehab_date))
            DB.date_update(symbol_dict[stock_i], 'Day', 'close = close * %s + %s' % (rehab_a, rehab_b),
                           'date < "%s"' % (rehab_date))
            DB.date_update(symbol_dict[stock_i], 'Day', 'high = high * %s + %s' % (rehab_a, rehab_b),
                           'date < "%s"' % (rehab_date))
            DB.date_update(symbol_dict[stock_i], 'Day', 'low = low * %s + %s' % (rehab_a, rehab_b),
                           'date < "%s"' % (rehab_date))
            DB.date_update(symbol_dict[stock_i], 'Day', 'mid = mid * %s + %s' % (rehab_a, rehab_b),
                           'date < "%s"' % (rehab_date))

            df_d = DB.data_request(symbol_dict[stock_i], 'Day')

            time.sleep(0.5)

    quote_ctx.close()




if __name__ == '__main__':

    df = pd.read_csv('700.csv', index_col=0)


    '''
    df['high_5'] = df['high'].shift(1).rolling(5).max()
    df['high_5_dif'] = df['high_5'] - df['high']
    df['low_5'] = df['low'].shift(1).rolling(5).min()
    df['low_5_dif'] = df['low'] - df['low_5']
    '''

    print(df)

















