# -*- coding: utf-8 -*-
import datetime
import sys, time, os
import pandas as pd
import Techanalysis as ts
from futu import *
from datetime import time
from time import sleep
import numpy as np
import mysql.connector
from mysql.connector import Error

pd.set_option('display.max_columns', 150)  # pandas setting 顥示列數上限
pd.set_option('display.width', 5000)  # pandas setting 顯示列的闊度
# pd.set_option('display.max_colwidth',20)      #pandas setting 每個數據顥示上限
pd.set_option('display.max_rows', 5000)  # pandas setting 顯示行的闊度
pd.options.mode.chained_assignment = None


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
    sql = "USE HK_00011"
    cursor.execute(sql)

    sql = "DELETE FROM Day WHERE date='2022-08-29'"
    #sql = "DELETE FROM Mins WHERE time_key>'2022-08-28'"
    cursor.execute(sql)
    connection.commit()
    #df = pd.read_sql("SELECT * FROM Day", connection)
    #df = pd.read_sql("SELECT * FROM Mins", connection)
    #print(df.tail(50))


if __name__ == "__main__":
    data_check()
    '''
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()
    symbol = symbol[:3]

    for i in symbol:
        i = str(i).replace('.', '_')
        exec('df_{} = {}'.format(i, 'pd.DataFrame()'))


    ret_sub, err_message = quote_ctx.subscribe(symbol, [SubType.TICKER], subscribe_push=False)
    if ret_sub == RET_OK:
        for stock_i in symbol:
            stock_i_ = stock_i.replace('.', '_')
            for x in range(20):
                ret, data = quote_ctx.get_rt_ticker(stock_i, 1000)
                if ret == RET_OK:
                    exec('df_{stock_i_} = pd.concat([df_{stock_i_}, data])'.format(stock_i_=stock_i_))
                    #exec('df_{stock_i_} = data'.format(stock_i_=stock_i_))
                else:
                    print('error:', data)
    else:
        print('subscription failed', err_message)

    for stock_i in symbol:
        stock_i_ = stock_i.replace('.', '_')
        exec('df_{}.to_csv("Analysis/" + stock_i + ".csv")'.format(stock_i_))

    quote_ctx.close()'''










































































