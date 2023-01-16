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
    df.reset_index(inplace=True, drop=True)

    if isinstance(df['time'][0], datetime):   #datetime 類型
        pass
    else: df['time'] = pd.to_datetime(df['time'])

    if P_level is None:
        import statistics
        list_P = df.drop_duplicates(subset=['price'], keep='first')['price']
        P_level = statistics.median(list_P)
        del statistics
    elif P_level == 'last':
        P_level = df['price'][len(df) - 1]   # 現價

    #print(P_level)

    i = 0
    dict_u, dict_d = {}, {}
    while i < len(df) - 1:   # 需要優化
        if df['price'][i] >= P_level:
            first_i_u: int = i
            while df['price'][i] >= P_level and i < len(df) - 1:
                i += 1
            else:
                last_i_u: int = i
                dict_u[first_i_u] = last_i_u
        else:
            first_i_d: int = i
            while df['price'][i] < P_level and i < len(df) - 1:
                i += 1
            else:
                last_i_d: int = i
                dict_d[first_i_d] = last_i_d

    upper_time = df['time'][0]
    for j, jj in dict_u.items():
        upper_time += df['time'][jj] - df['time'][j]
    upper_time -= df['time'][0]

    lower_time = df['time'][0]
    for k, kk in dict_d.items():
        lower_time += df['time'][kk] - df['time'][k]
    lower_time -= df['time'][0]
    # 得出高(低)於P_Level時，平均每秒的成交

    #print(upper_time.seconds, lower_time.seconds)

    distribution_B = df[df.ticker_direction == 'BUY']
    distribution_S = df[df.ticker_direction == 'SELL']

    distribution_Buy_T = distribution_B.groupby('price')['turnover'].sum()
    distribution_Sell_T = distribution_S.groupby('price')['turnover'].sum()

    distribution_Buy_T = pd.DataFrame(distribution_Buy_T)
    distribution_Buy_T['index'] = distribution_Buy_T.index.map(lambda x: x - P_level)
    distribution_Buy_T['distribution'] = distribution_Buy_T['turnover'] * distribution_Buy_T['index']

    distribution_Sell_T = pd.DataFrame(distribution_Sell_T)
    distribution_Sell_T['index'] = distribution_Sell_T.index.map(lambda x: x - P_level)
    distribution_Sell_T['distribution'] = distribution_Sell_T['turnover'] * distribution_Sell_T['index']

    quadrant0 = distribution_Buy_T[distribution_Buy_T.index > P_level]['distribution'].sum() / int(upper_time.seconds)
    quadrant1 = distribution_Sell_T[distribution_Sell_T.index > P_level]['distribution'].sum() / int(upper_time.seconds)
    quadrant2 = distribution_Buy_T[distribution_Buy_T.index < P_level]['distribution'].sum() / int(lower_time.seconds)
    quadrant3 = distribution_Sell_T[distribution_Sell_T.index < P_level]['distribution'].sum() / int(lower_time.seconds)

    TQ = quadrant0 + quadrant1 + abs(quadrant2) + abs(quadrant3)
    '''
    print(f'Q0: {round(quadrant0, 4)}, {round(quadrant0/TQ * 100, 2)}')
    print(f'Q1: {round(quadrant1, 4)}, {round(quadrant1/TQ * 100, 2)}')
    print(f'Q2: {round(quadrant2, 4)}, {round(quadrant2/TQ * 100, 2)}')
    print(f'Q3: {round(quadrant3, 4)}, {round(quadrant3/TQ * 100, 2)}')
    '''
    Q_result = {'Q0': round(quadrant0/1000, 2), 'Q1': round(quadrant1/1000, 2), 'Q2': round(quadrant2/1000, 2),
                'Q3': round(quadrant3/1000, 2)}

    return Q_result


def model3_reflection(df):
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    df.reset_index(inplace=True, drop=True)
    if isinstance(df['time'][0], datetime):   #datetime 類型
        pass
    else: df['time'] = pd.to_datetime(df['time'])



if __name__ == "__main__":
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()
    symbol = symbol[:1]
    symbol_dict = {i: i.replace('.', '_') for i in symbol}

    DB = Database('103.68.62.116', 'root', '630A78e77?')
    dfDay = DB.data_request('HK_01548', 'Mins')
    print(dfDay.tail(1))
    dfDay = DB.data_request('HK_01798', 'Mins')
    print(dfDay.tail(1))


    '''
    for stock_i in symbol_dict:
        T_List = DB.table_list(symbol_dict[stock_i])
        T_List.remove('Day')
        T_List.remove('Mins')


        dfResult = pd.DataFrame(columns={'Positive', 'Negative', 'Ratio', 'Adjusted'}, index= [j.replace('_', '-') for j in T_List])
        for list_i in T_List:
            df = DB.data_request(symbol_dict[stock_i], list_i)
            list_i = list_i.replace('_', '-')
            dfResult.loc[list_i] = ts.model3_2_4(df)

        dfDay = DB.data_request(symbol_dict[stock_i], 'Day')
        dfDay = dfDay.set_index('date', drop=True)

        dfResult['Ratio'] = dfResult.apply(lambda x:
                                                     x['Positive'] / x['Negative'] * -1
                                                     if x['Positive'] > x['Negative'] * -1
                                                     else x['Negative'] / x['Positive'],
                                                     axis=1)
        dfResult['Ratio'] = dfResult['Ratio'].astype('float')
        dfResult['Ratio'] = dfResult['Ratio'].round(2)

        maket_trend = dfResult['Positive'].sum() / dfResult['Negative'].sum()
        dfResult['Adjusted'] = dfResult['Ratio'] * maket_trend

        dfResult = dfResult[['Positive', 'Negative', 'Ratio', 'Adjusted']]

        dfResult.index = pd.to_datetime(dfResult.index)
        dfDay.index = pd.to_datetime(dfDay.index)

        dfDay = pd.concat([dfDay, dfResult], axis=1, sort=False)

        dfDay.to_csv('%s.csv' %(stock_i))'''

    '''
    stock = 'HK_00005'
    T_List = DB.table_list(stock)
    T_List.remove('Day')
    T_List.remove('Mins')
    T_List = T_List[20:]
    
    for i in T_List[len(T_List) - 20:]:
        df = DB.data_request(stock, i)
        df_re.loc[i] = model3_2_4(df)

    df_re = df_re[['Q0', 'Q1', 'Q2', 'Q3']]
    print(df_re)   # 注意有較大比率出現'''
    #print(DB.table_list('HK_00005'))
    #df.to_csv('HK_00005_2022_09_14.csv')














