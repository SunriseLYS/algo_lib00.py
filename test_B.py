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


def executed_deal(quote_ctx):
    ret_sub, err_message = quote_ctx.subscribe(['HK.00700'], [SubType.TICKER], subscribe_push=False)
    if ret_sub == RET_OK:
        ret, data = quote_ctx.get_rt_ticker('HK.00700', 2)
        if ret == RET_OK:
            print(data)
        else:
            print('error:', data)
    else:
        print('subscription failed', err_message)


def matching(quote_ctx):
    ret_sub = quote_ctx.subscribe(['HK.00700'], [SubType.ORDER_BOOK], subscribe_push=False)[0]
    if ret_sub == RET_OK:  # 订阅成功
        ret, data = quote_ctx.get_order_book('HK.00700', num=3)  # 获取一次 3 档实时摆盘数据
        if ret == RET_OK:
            print(data)
        else:
            print('error:', data)
    else:
        print('subscription failed')


class Database:
    def __init__(self, host_name, user_name, user_password):
        self.connection = None
        try:
            self.connection = mysql.connector.connect(host=host_name,
                                                      user=user_name,
                                                      passwd=user_password
                                                      )
            self.cursor = self.connection.cursor()
        except Error as err:
            print(f"Error: '{err}'")

    def data_request(self, stock_i_, table):
        self.cursor.execute("USE %s" % (stock_i_))
        self.cursor.execute("SHOW columns FROM %s" % (table))
        column_list= [i[0] for i in self.cursor.fetchall()]

        self.cursor.execute("SELECT * FROM %s" % (table))  # Day, Mins, YYYY-MM-DD
        df = pd.DataFrame(self.cursor.fetchall(), columns=column_list)
        return df

    def creat_database(self, DB_name):
        self.cursor.execute("CREATE DATABASE %s" % (DB_name))

    def creat_table(self, DB_name, sql):
        self.cursor.execute("USE %s" % (DB_name))
        self.cursor.execute(sql)



if __name__ == "__main__":
    '''
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()'''
    DB = Database('103.68.62.116', 'root', '630A78e77?')
    df = DB.data_request('HK_00005', '2022_09_02')

    print(df.head(10))











