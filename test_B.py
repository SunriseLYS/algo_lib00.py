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


def model6_2_4(df, P_level = None):   # P_level應是現價
    #print(df[df.ticker_direction == 'NEUTRAL']['turnover'].sum())
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    df.drop(['code', 'sequence'], axis=1, inplace=True)
    df.reset_index(inplace=True, drop=True)

    if isinstance(df['time'][0], datetime):
        pass
    else: df['time'] = pd.to_datetime(df['time'])

    df_B = df.drop(df[df['ticker_direction'] == 'SELL'].index)
    df_S = df.drop(df[df['ticker_direction'] == 'BUY'].index)

    df_count_B = pd.DataFrame()
    df_count_B['count_B'] = df_B['price'].value_counts()
    df_count_S = pd.DataFrame()
    df_count_S['count_S'] = df_S['price'].value_counts()
    df_count = pd.concat([df_count_B, df_count_S], axis=1)
    df_count.index = [str(x) for x in df_count.index]
    df_count.fillna(0, inplace=True)
    df_count = df_count.astype({'count_B': 'int', 'count_S': 'int'})

    df_B.set_index('price', inplace=True)
    df_B.index = df_B.index.astype(str, copy = False)
    df_B.drop(['time'], axis=1, inplace=True)

    df_S.set_index('price', inplace=True)
    df_S.index = df_S.index.astype(str, copy=False)
    df_S.drop(['time'], axis=1, inplace=True)

    df_index = sorted(set(df['price'].to_list()))
    df_index = [str(x) for x in df_index]
    df_re = pd.DataFrame(columns={'Buy', 'Sell'}, index=df_index)

    for i in df_index:
        if i in df_B.index:
            df_re['Buy'][i] = df_B.loc[i]['turnover'].sum()   # 相同索引總和
        if i in df_S.index:
            df_re['Sell'][i] = df_S.loc[i]['turnover'].sum()

    df_re = pd.concat([df_re, df_count], axis=1)
    df_re.fillna(0, inplace=True)
    df_re = df_re.astype({'count_B': 'int', 'count_S': 'int'})

    df_re['avg_B'] = df_re['Buy'] / df_re['count_B']
    df_re['avg_B'] = df_re['avg_B'].round(0)
    df_re['avg_S'] = df_re['Sell'] / df_re['count_S']
    df_re['avg_S'] = df_re['avg_S'].round(0)

    df_re = df_re[['Buy', 'count_B', 'avg_B', 'Sell', 'count_S', 'avg_S']]
    df_re.fillna(0, inplace=True)

    if P_level is None:
        P_level = df_re.index[len(df_re.index)//2]
    elif P_level == 'last':
        P_level = df['price'][len(df) - 1]   # 現價

    # 找出位置
    P_level_loc: int = 0
    for i in range(len(df_re.index)):
        if df_re.index[i] == P_level:
            P_level_loc = i

    df_re['feature'] = [float(x) - float(P_level) for x in df_re.index]

    df_re['B_power'] = df_re['Buy'] * df_re['feature']
    df_re['S_power'] = df_re['Sell'] * df_re['feature']
    df_re['N_power'] = df_re['B_power'] - df_re['S_power']

    upper_power =df_re[P_level_loc:]['N_power'].sum()
    lower_power = df_re[:P_level_loc]['N_power'].sum()
    total_power = abs(upper_power) + abs(lower_power)
    '''
    print('lower: %s; upper: %s' %(round(lower_power / total_power, 2), round(upper_power / total_power, 2)))
    print(round(total_power))'''

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

    print(df_re)

    df_re2 = pd.concat([df_re.loc[[df_re.index[0]]],
                        df_re.sort_values(by=['Sum'], ascending=False)[:3],
                        df_re.loc[[df_re.index[len(df_re.index) - 1]]]])

    moder_result = {'Price': df_re2['Sum'].idxmax(),   # 成交最高的價位
                    'Power': df_re2['Power'].sum(),
                    'Support': df_re.index[0],
                    'Celling': df_re.index[len(df_re.index) - 1]}
    return moder_result'''

    # lower高負數, upper正數, confid >= 0.9, 沽出
    # lower但正數, upper高正數, confid >= 0.9, 買入

    result_dict = {'lower': round(lower_power / total_power, 2),
                   'upper': round(upper_power / total_power, 2),
                   'turnover': round(total_power, 0)}
    return result_dict


def model9_2_4(df):
    #print(df[df.ticker_direction == 'NEUTRAL']['turnover'].sum())
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    df.drop(['code', 'sequence', 'type'], axis=1, inplace=True)
    df.reset_index(inplace=True, drop=True)

    if isinstance(df['time'][0], datetime):
        pass
    else: df['time'] = pd.to_datetime(df['time'])

    df['avg'] = round(df['volume'].mean(), 0) * 3

    accumulate: int = 0
    df['accumulate'] = 0
    df['instruct'] = ' '
    for i in range(len(df)):
        if df['ticker_direction'][i] == 'BUY':
            accumulate += df['volume'][i]
            df['accumulate'][i] = accumulate

            if df['volume'][i] > df['avg'][i]:
                df['instruct'][i] = 'B'

        else:
            accumulate -= df['volume'][i]
            df['accumulate'][i] = accumulate

            if df['volume'][i] > df['avg'][i]:
                df['instruct'][i] = 'S'


    df['fu_p'] = df['price'].rolling(10).mean()

    # 出現大手買入的 BUY 則跟隨買入, 直到出現多次 大手買入或 大手賣出
    print(df)



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
    symbol = symbol[7:8]
    symbol_dict = {i: i.replace('.', '_') for i in symbol}

    DB = Database('103.68.62.116', 'root', '630A78e77?')

    for stock_i in symbol_dict:
        T_List = DB.table_list(symbol_dict[stock_i])
        T_List.remove('Day')
        T_List.remove('Mins')

        T_List = T_List[20:21]

        dfResult = pd.DataFrame(columns={'lower', 'upper', 'turnover'})
        for list_i in T_List:
            df = DB.data_request(symbol_dict[stock_i], list_i)
            print(model9_2_4(df))
















