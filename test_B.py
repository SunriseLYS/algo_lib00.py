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

    def data_request(self, stock_i_, table, instruct=None):
        try:
            self.cursor.execute("USE %s" % (stock_i_))
            self.cursor.execute("SHOW columns FROM %s" % (table))
            column_list = [i[0] for i in self.cursor.fetchall()]

            order_col = 'time'
            if table == 'Day':
                order_col = 'date'
            elif table == 'Mins':
                order_col = 'time_key'

            self.cursor.execute("SELECT * FROM %s ORDER BY %s" % (table, order_col))
            df = pd.DataFrame(self.cursor.fetchall(), columns=column_list)

            if instruct is None:
                return df
            else:
                self.cursor.execute("ORDER BY code")
                column_list = [i[0] for i in self.cursor.fetchall()]
                df = pd.DataFrame(self.cursor.fetchall(), columns=column_list)

        except:
            print('Dose not exist')

    def creat_database(self, DB_name):
        self.cursor.execute("CREATE DATABASE %s" % (DB_name))

    def creat_table(self, DB_name, sql):
        self.cursor.execute("USE %s" % (DB_name))
        self.cursor.execute(sql)


def model3_2_40(df, P_level = None):   # P_level應是現價
    #print(df[df.ticker_direction == 'NEUTRAL']['turnover'].sum())
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    df.drop(['code', 'sequence'], axis=1, inplace=True)
    df.reset_index(inplace=True, drop=True)

    df_B = df.drop(df[df['ticker_direction'] == 'SELL'].index)
    df_B.set_index('price', inplace=True)
    df_B.index = df_B.index.astype(str, copy = False)
    df_B.drop(['time'], axis=1, inplace=True)

    df_S = df.drop(df[df['ticker_direction'] == 'BUY'].index)
    df_S.set_index('price', inplace=True)
    df_S.index = df_S.index.astype(str, copy=False)
    df_S.drop(['time'], axis=1, inplace=True)

    df_index = sorted(set(df['price'].to_list()))
    df_index = [str(x) for x in df_index]
    df_re = pd.DataFrame(columns={'Buy', 'Sell', 'Ratio'}, index=df_index)

    for i in df_index:
        if i in df_B.index:
            df_re['Buy'][i] = df_B.loc[i]['turnover'].sum()   # 相同索引總和
        if i in df_S.index:
            df_re['Sell'][i] = df_S.loc[i]['turnover'].sum()

    '''
    df_re['Ratio'] = df_re.apply(lambda x: x['Buy'] / x['Sell'] if x['Buy'] > x['Sell'] else x['Sell'] / x['Buy'] * -1,
                                 axis=1)
    '''
    # 空值轉變成0, 再比較底部購買力和頂部沽空
    df_re.fillna(0, inplace=True)

    df_re['Sum'] = df_re['Buy'] + df_re['Sell']
    df_re['Power'] = df_re['Buy'] - df_re['Sell']

    bottomBuy = df_re.loc[df_re.index[0]]
    support = bottomBuy['Buy'] - bottomBuy['Sell']
    topSell = df_re.loc[df_re.index[len(df_re.index) - 1]]
    pressure = topSell['Buy'] - topSell['Sell']

    df_re = df_re[['Buy', 'Sell', 'Sum', 'Power']]

    df_re2 = pd.concat([df_re.loc[[df_re.index[0]]],
                        df_re.sort_values(by=['Sum'], ascending=False)[:3],
                        df_re.loc[[df_re.index[len(df_re.index) - 1]]]])
    print(df_re2)

    print(df_re2['Sum'].idxmax(), df_re2['Power'].sum())
    print(df_re.index[0])
    print(df_re.index[len(df_re.index) - 1])
    return df_re


def model9(df):
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    df.drop(['code', 'sequence', 'type'], axis=1, inplace=True)
    df.reset_index(inplace=True, drop=True)

    df_B = df.drop(df[df['ticker_direction'] == 'BUY'].index)

    df_S = df.drop(df[df['ticker_direction'] == 'SELL'].index)
    df['Roll'] = df['turnover'].rolling(10).sum()

    df_T = df[['time', 'turnover']]

    print(df)



def model3_reflection(df):
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    df.reset_index(inplace=True, drop=True)
    if isinstance(df['time'][0], datetime):   #檢查datetime 類型
        pass
    else: df['time'] = pd.to_datetime(df['time'])



if __name__ == "__main__":
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()
    symbol = symbol[:1]
    symbol_dict = {i: i.replace('.', '_') for i in symbol}

    DB = Database('103.68.62.116', 'root', '630A78e77?')

    for stock_i in symbol_dict:
        T_List = DB.table_list(symbol_dict[stock_i])
        T_List.remove('Day')
        T_List.remove('Mins')

        T_List = T_List[:5]

        dfResult = pd.DataFrame(columns={'Positive', 'Negative', 'Ratio', 'Adjusted'}, index= [j.replace('_', '-') for j in T_List])
        for list_i in T_List:
            print(list_i)
            df = DB.data_request(symbol_dict[stock_i], list_i)
            #df.to_csv('%s.csv' %(list_i))
            df_re = model3_2_40(df)
    #print(DB.table_list('HK_00005'))
    #df.to_csv('HK_00005_2022_09_14.csv')














