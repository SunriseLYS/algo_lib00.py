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


class Database:   # 需增加檢驗資料完整性功能, 如每隔15分鐘有超過1000筆交易便有遺漏的機會
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

    def table_list(self, stock_i_):
        self.cursor.execute("USE %s" % (stock_i_))
        self.cursor.execute("SHOW TABLEs")
        table_list = [i[0] for i in self.cursor.fetchall()]
        return table_list

    def data_request(self, stock_i_, table):
        try:
            self.cursor.execute("USE %s" % (stock_i_))
            self.cursor.execute("SHOW columns FROM %s" % (table))
            column_list = [i[0] for i in self.cursor.fetchall()]

            self.cursor.execute("SELECT * FROM %s" % (table))  # Day, Mins, YYYY_MM_DD
            df = pd.DataFrame(self.cursor.fetchall(), columns=column_list)
            return df
        except:
            print('Dose not exist')

    def creat_database(self, DB_name):
        self.cursor.execute("CREATE DATABASE %s" % (DB_name))

    def creat_table(self, DB_name, sql):
        self.cursor.execute("USE %s" % (DB_name))
        self.cursor.execute(sql)

def model3_2_4(df, P_level = None):   # P_level應是現價
    print(df[df.ticker_direction == 'NEUTRAL']['turnover'].sum())
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)

    distribution_B = df[df.ticker_direction == 'BUY']
    distribution_S = df[df.ticker_direction == 'SELL']

    distribution_Buy_T = distribution_B.groupby('price')['turnover'].sum()
    distribution_Sell_T = distribution_S.groupby('price')['turnover'].sum()

    if P_level is None:
        import statistics
        list_P = df.drop_duplicates(subset=['price'], keep='first')['price']
        P_level = statistics.median(list_P)
        del statistics
    elif P_level == 'last':
        P_level = df['price'][len(df) - 1]   # 現價

    distribution_Buy_T = pd.DataFrame(distribution_Buy_T)
    distribution_Buy_T['index'] = distribution_Buy_T.index.map(lambda x: x - P_level)
    distribution_Buy_T['distribution'] = distribution_Buy_T['turnover'] * distribution_Buy_T['index']

    distribution_Sell_T = pd.DataFrame(distribution_Sell_T)
    distribution_Sell_T['index'] = distribution_Sell_T.index.map(lambda x: x - P_level)
    distribution_Sell_T['distribution'] = distribution_Sell_T['turnover'] * distribution_Sell_T['index']

    quadrant0 = distribution_Buy_T[distribution_Buy_T.index > P_level]['distribution'].sum()
    quadrant1 = distribution_Sell_T[distribution_Sell_T.index > P_level]['distribution'].sum()
    quadrant2 = distribution_Buy_T[distribution_Buy_T.index < P_level]['distribution'].sum()
    quadrant3 = distribution_Sell_T[distribution_Sell_T.index < P_level]['distribution'].sum()

    print(f'Q0: {round(quadrant0, 4)}')
    print(f'Q1: {round(quadrant1, 4)}')
    print(f'Q2: {round(quadrant2, 4)}')
    print(f'Q3: {round(quadrant3, 4)}')


if __name__ == "__main__":
    '''
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()'''
    T_List = ['2022_08_31', '2022_09_01', '2022_09_02', '2022_09_05', '2022_09_06', '2022_09_07', '2022_09_08', '2022_09_13', '2022_09_14', '2022_09_15', '2022_09_16', '2022_09_19', '2022_09_20', '2022_09_21', '2022_09_22', '2022_09_23', '2022_09_26', '2022_09_27', '2022_09_28', '2022_09_29', '2022_10_14', '2022_10_17', '2022_10_18', '2022_10_19']
    DB = Database('103.68.62.116', 'root', '630A78e77?')

    for i in T_List:
        print(i)
        df = DB.data_request('HK_00005', i)
        print(len(df))
        df_re = model3_2_4(df)

    #print(DB.table_list('HK_00005'))

    #df.to_csv('HK_00005_2022_09_14.csv')











