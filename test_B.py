# -*- coding: utf-8 -*-
import datetime
import sys, os
import pandas as pd
import Techanalysis as ts
from futu import *
from datetime import time
from time import sleep
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import mplfinance as mpf
import mysql.connector
from mysql.connector import Error

pd.set_option('display.max_columns', 150)  # pandas setting 顥示列數上限
pd.set_option('display.width', 5000)  # pandas setting 顯示列的闊度
# pd.set_option('display.max_colwidth',20)      #pandas setting 每個數據顥示上限
pd.set_option('display.max_rows', 5000)  # pandas setting 顯示行的闊度
pd.options.mode.chained_assignment = None


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

    def show_db(self, table):
        sql = "SHOW DATABASES LIKE %s" % ('"' + table + '"')
        self.cursor.execute(sql)
        showHall = self.cursor.fetchall()
        return showHall

    def data_input(self, DB_name, table, values_num: int, row: tuple):
        sqlTo = "INSERT INTO %s.%s" % (DB_name, table)
        value = "VALUES" + "(" + "%s" * values_num + ")"
        sql = sqlTo + value
        self.cursor.execute(sql, tuple(row))
        self.connection.commit()

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

    df['time1'] = df['time'].shift(1)
    df['time_dif'] = df['time'] - df['time1']
    df.drop('time1', axis=1, inplace=True)
    df['time_dif'] = df['time_dif'] / pd.Timedelta(seconds=1)


    accumulate: int = 0
    df['accumulate'] = 0
    df['instruct'] = ' '
    for i in range(3, len(df)):
        if df['ticker_direction'][i] == 'BUY':
            df['volume'][i:i].sum()
            accumulate += df['volume'][i]
            df['accumulate'][i] = accumulate

            if accumulate > 180000:
                df['instruct'][i] = 'B'

        else:
            accumulate -= df['volume'][i]
            df['accumulate'][i] = accumulate

            if df['volume'][i] > 180000:
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



class TickerTest(TickerHandlerBase):
    def __init__(self):
        self.ana_dict = {'US.TSLA': 3600, 'US.DIS': 6000, 'US.AAPL': 3000}
        self.T_book = pd.DataFrame()
    def on_recv_rsp(self, rsp_pb):
        ret_code, data = super(TickerTest,self).on_recv_rsp(rsp_pb)
        if ret_code != RET_OK:
            print("StockQuoteTest: error, msg: %s" % data)
            return RET_ERROR, data
        self.T_book = pd.concat([self.T_book, data])
        self.dealScreening(data)
        self.T_book.to_csv('US_Test.csv')
        return RET_OK, data

    def dealScreening(self, data):
        data.to_csv('US_Test.csv')
        data.drop('push_data_type', axis=1, inplace=True)

        volume_TH = self.ana_dict['%s' %(data['code'][0])]

        data = data[data['volume'] > volume_TH]
        data.drop(data[data['ticker_direction'] == 'NEUTRAL'].index, inplace=True)

        if len(data) > 0:
            print(data[data['volume'] > volume_TH])

def tip_seeing(value, related_value, unit):
    if all(value >= x for x in related_value) and value > min(related_value) + unit:
        return value, value + unit, value - unit*2
    else:
        return '', '', ''


def single_order(trd_ctx, ticket: str, pr: float, qt: int, adj_unit: float, order_side: str):
    if order_side == 'BUY' and adj_unit <= 0:
        print('Type 1 error')
        return None
    elif order_side == 'SELL' and adj_unit >= 0:
        print('Type 1 error')
        return None

    o_id: str
    ret, data = trd_ctx.place_order(price=pr, qty=qt, code=ticket, trd_side='TrdSide.%s' %(order_side),
                                    trd_env=TrdEnv.REAL)
    if ret == RET_OK:
        print(data)
        o_id = data['order_id'][0]
    else:
        print('place_order error: ', data)

    sleep(5)

    remain_qt: float = qt
    ret, order_list = trd_ctx.order_list_query(order_id=o_id)
    if ret == RET_OK:
        print(order_list)
        remain_qt = order_list['qty'][0] - order_list['dealt_qty'][0]

        mod_t: int = 0
        while remain_qt != 0:
            ret, data = trd_ctx.modify_order(ModifyOrderOp.NORMAL, o_id, price= pr + adj_unit)
            sleep(5)
            ret, order_list = trd_ctx.order_list_query(order_id=o_id)
            remain_qt = order_list['qty'][0] - order_list['dealt_qty'][0]
            mod_t += 1

            if mod_t > 6:
                break

    else:
        print('order_list_query error: ', order_list)



if __name__ == "__main__":
    std_d = 3

    DB = Database('103.68.62.116', 'root', '630A78e77?')
    df = DB.data_request('US_TSLA', 'Day')
    df['date'] = df['time_key'].apply(lambda y: y.strftime('%Y-%m-%d'))
    date_target = sorted(set(df['date']))
    date_target = date_target[len(date_target) - std_d]
    df = df[df['time_key'] >= pd.to_datetime(date_target)]
    std_unit = df['change_rate'].std()

    df = DB.data_request('US_TSLA', 'Day')
    df = df[len(df) - 99:]
    avg_100 = df['close'].to_list()
    avg_50 = df['close'][len(df)-49:].to_list()
    avg_20 = df['close'][len(df)-19:].to_list()
    avg_10 = df['close'][len(df)-9:].to_list()
    avg_5 = df['close'][len(df)-4:].to_list()

    '''
    df = pd.read_csv('TSLA_M.csv', index_col=0)
    df_pastday = pd.read_csv('TSLA_D.csv', index_col=0)
    df.drop(['pre_close', 'change_rate', 'volume', 'turnover'], axis=1, inplace=True)
    date_list = sorted(set(df['time_key'].apply(lambda x: x[:10])))

    df['time_key'] = pd.to_datetime(df['time_key'])

    df_day = df[(df['time_key'] > date_list[10]) & (df['time_key'] < date_list[11])]
    df_pastday = df_pastday[(df_pastday['date'] > date_list[5]) & (df_pastday['date'] < date_list[10])]

    for row in range(1, len(df_day) - 380):
        df_result = ts.model12(df_day[:row], df_pastday)

    print(df_result)

    df_day.set_index('time_key', inplace=True, drop=True)

    ap = mpf.make_addplot()
    mpf.plot(df_day, type='candle')
    plt.show()
    
    df_result = ts.model12(df_day, df_pastday)
    print(df_result)

    fig = plt.figure()
    ax = plt.subplot()
    ax.plot(df_result['time_key'], df_result['high'])
    ax.plot(df_result['time_key'], df_result['upper'], color='red')

    tick_spacing = df_result.index.size / 10
    ax.xaxis.set_major_locator(mticker.MultipleLocator(tick_spacing))
    plt.show()


    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()
    symbol_dict = {i: i.replace('.', '_') for i in symbol}'''


    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    Ticker_US = ['US.TSLA']
    tip_list = []
    std = 0.3
    t = 0
    ret_sub, err_message = quote_ctx.subscribe(['US.TSLA'], [SubType.K_1M], subscribe_push=False)
    while t <= 180:
        if ret_sub == RET_OK:
            ret, data = quote_ctx.get_cur_kline('US.TSLA', 30, KLType.K_1M, AuType.QFQ)
            if ret == RET_OK:
                data['tip'] = ''
                data['target_U'] = ''
                data['target_D'] = ''
                for i in range(2, len(data) - 2):
                    data['tip'][i], data['target_U'][i], data['target_D'][i] = tip_seeing(data['high'][i],
                                                                                          data['high'][i - 2: i + 3].to_list(),
                                                                                          std
                                                                                          )
                    if data['tip'][i] != '':
                        tip_list.append(data['tip'][i])
                        if len(tip_list) > 1:
                            tipGap = tip_list[1] - tip_list[0]

                print(data)
            else:
                print('error:', data)
        else:
            print('subscription failed', err_message)
        sleep(60)
        t+=1

    quote_ctx.close()