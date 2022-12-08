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

            self.cursor.execute("SELECT * FROM %s ORDER BY time" % (table))
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


def model3_2_4(df, P_level = None):   # P_level應是現價
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
    '''
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()
    #symbol = symbol[:1]
    symbol_dict = {i: i.replace('.', '_') for i in symbol}

    DB = Database('103.68.62.116', 'root', '630A78e77?')

    for stock_i in symbol_dict:
        T_List = DB.table_list(symbol_dict[stock_i])
        T_List.remove('Day')
        T_List.remove('Mins')
        df = DB.data_request(symbol_dict[stock_i], '2022_11_03')
        print(symbol_dict[stock_i])
        print(model3_2_4(df))'''


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

    import smtplib
    from email.mime.text import MIMEText
    from email.header import Header

    mail_host = "smtp.abchk.com"
    mail_user = "sam.liu@nvcapitals.com"
    mail_pass = "630A78e77"

    sender = 'sam.liu@nvcapitals.com'
    receivers = ['sam.liu@nvcapitals.com']

    message = MIMEText("his is the mail system at host smtp45.abchk.net. \nI'm sorry to have to inform you that your message could not "
                       "be delivered to one or more recipients. \nIt's attached below. For further assistance, "
                       "please send mail to postmaster. \n\nIf you do so, please include this problem report. \n\nYou can"
                       "delete your own text from the attached returned message.The mail system", 'plain', 'utf-8')

    message['From'] = Header("MAILER-DAEMON@smtp45.abchk.net (Mail Delivery System)", 'utf-8')
    message['To'] = Header("sam.liu@nvcapitals.com", 'utf-8')

    subject = 'Undelivered Mail Returned to Sender'
    message['Subject'] = Header(subject, 'utf-8')
    message['Date'] = '2022-12-8 18:37:32'

    smtpObj = smtplib.SMTP()
    smtpObj.connect(mail_host, 465)
    smtpObj.login(mail_user, mail_pass)
    print('login')
    smtpObj.sendmail(sender, receivers, message.as_string())
    print('sent')










