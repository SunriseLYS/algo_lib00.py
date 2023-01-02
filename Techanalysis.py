import numpy as np
import datetime
import pandas as pd
import talib as ta
import os
import time
import itertools
import mysql.connector
from mysql.connector import Error
pd.options.mode.chained_assignment = None
pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 10)       #pandas setting 顥示列數上限
pd.set_option('display.width', 5000)           #pandas setting 顯示列的闊度


Hit_Rate = lambda a, b: a / b if b > 0 else 0

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

        self.cursor.execute("SELECT * FROM %s" % (table))  # Day, Mins, YYYY_MM_DD
        df = pd.DataFrame(self.cursor.fetchall(), columns=column_list)
        return df

    def creat_database(self, DB_name):
        self.cursor.execute("CREATE DATABASE %s" % (DB_name))

    def creat_table(self, DB_name, sql):
        self.cursor.execute("USE %s" % (DB_name))
        self.cursor.execute(sql)

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

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

def data_request(connection, stock_i_, table = 'Day'):
    cursor = connection.cursor()
    sql = "USE %s" % (stock_i_)
    cursor.execute(sql)
    sql = "SELECT * FROM %s" % (table)
    cursor.execute(sql)

    result = cursor.fetchall()
    col_result = cursor.description

    columns = []
    for i in range(len(col_result)):
        columns.append(col_result[i][0])

    df = pd.DataFrame(result, columns=columns)
    return df

class portfolio:
    def __init__(self, holding = 0, portfolio = 100, tradeTime = 0, tradeTimeS = 0):
        self.holding, self.portfolio, self.tradeTime, self.tradeTimeS = holding, portfolio, tradeTime, tradeTimeS
        self.buyP, self.buyD, self.holdingPeriod = 0, 0, 0

    def buy(self, buyP, buyD):
        self.holding = 1
        self.buyP = buyP
        self.buyD = buyD

    def sell(self, sellP, sellD):
        self.holding = 0
        self.sellP = sellP
        self.sellD = sellD
        self.tradeTime += 1
        self.portfolio *= self.sellP/ self.buyP
        self.portfolioReturn = ((self.portfolio / 100) - 1) * 100
        self.holdingPeriod = self.sellD - self.buyD
        if self.sellP > self.buyP:
            self.tradeTimeS += 1

def bestAnalysisBase(stock_i):
    dfSum = pd.read_csv('Database/' + stock_i + '/Method Summary.csv')
    dfSum = dfSum.sort_values(['Hit Rate'], ascending=False)
    topLoc = dfSum.index[0]

    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
    df = df.drop(['Unnamed: 0'], axis = 1)

    if dfSum['Tech Method'][topLoc] == 'MFI':
        techCon0 = int(dfSum['Method Condition0'][topLoc])
        techCon1 = int(dfSum['Method Condition1'][topLoc])
        techCon2 = int(dfSum['Method Condition2'][topLoc])
        df['MFI - Base'] = ta.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod = techCon0)
        df['result Base'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        for i in range(len(df)):
            if holding == 0 and df['MFI - Base'][i] <= techCon1:
                holding = 1
                buyPrice = df['mid'][i]
                buyDay = i
            elif holding == 1 and df['MFI - Base'][i] >= techCon2:
                holding = 0
                if df['mid'][i] > buyPrice:
                    df['result Base'][buyDay:i + 1] = 'Win'
                elif df['mid'][i] < buyPrice:
                    df['result Base'][buyDay:i + 1] = 'Lose'
    elif dfSum['Tech Method'][topLoc] == 'RSI':
        techCon0 = int(dfSum['Method Condition0'][topLoc])
        techCon1 = int(dfSum['Method Condition1'][topLoc])
        techCon2 = int(dfSum['Method Condition2'][topLoc])
        techCon3 = str(dfSum['Method Condition3'][topLoc])
        df['RSI - Base'] = ta.RSI(df['close'], timeperiod=techCon0)
        df['result Base'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        if techCon3 == 'Negative':
            for i in range(len(df)):
                if holding == 0 and df['RSI - Base'][i] < techCon1:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['RSI - Base'][i] > techCon2:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Lose'
        elif techCon3 == 'Positive':
            for i in range(len(df)):
                if holding == 0 and df['RSI - Base'][i] > techCon1:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['RSI - Base'][i] < techCon2:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Lose'
    elif dfSum['Tech Method'][topLoc] == 'BB':
        techCon0 = int(dfSum['Method Condition0'][topLoc])
        techCon1 = int(dfSum['Method Condition1'][topLoc])
        techCon2 = int(dfSum['Method Condition2'][topLoc])
        techCon3 = str(dfSum['Method Condition3'][topLoc])
        df['BB Upper - Base'], df['BB Middle - Base'], df['BB Lower - Base'] = ta.BBANDS(df['close'], techCon0, techCon1, techCon2)
        df['result Base'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        if techCon3 == 'Bottom To Middle':
            for i in range(len(df)):
                if holding == 0 and df['BB Lower - Base'][i] >= df['low'][i]:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['BB Middle - Base'][i] <= df['high'][i]:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Lose'
        if techCon3 == 'Middle To Top':
            for i in range(len(df)):
                if holding == 0 and df['BB Middle - Base'][i] >= df['low'][i]:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['BB Upper - Base'][i] <= df['high'][i]:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Lose'
        if techCon3 == 'Bottom To Top':
            for i in range(len(df)):
                if holding == 0 and df['BB Lower - Base'][i] >= df['low'][i]:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['BB Upper - Base'][i] <= df['high'][i]:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Base'][buyDay:i + 1] = 'Lose'
    elif dfSum['Tech Method'][topLoc] == 'SAR':
        techCon0 = int(dfSum['Method Condition0'][topLoc])
        techCon1 = int(dfSum['Method Condition1'][topLoc])
        techCon2 = int(dfSum['Method Condition2'][topLoc])
        df['SAR - Base'] = ta.SAR(df['high'], df['low'], acceleration=techCon0, maximum=techCon1)
        df['result Base'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        for i in range(len(df) - 5):
            if holding == 0 and df['SAR - Base'][i] < df['close'][i] and df['SAR - Base'][i + techCon2] < df['close'][i + techCon2]:
                holding = 1
                buyPrice = df['mid'][i]
                buyDay = i
            elif holding == 1 and df['SAR - Base'][i] > df['close'][i]:
                holding = 0
                if df['mid'][i] > buyPrice:
                    df['result Base'][buyDay:i + 1] = 'Win'
                elif df['mid'][i] < buyPrice:
                    df['result Base'][buyDay:i + 1] = 'Lose'

    elif dfSum['Tech Method'][topLoc] == 'MACD':
        techCon0 = int(dfSum['Method Condition0'][topLoc])
        techCon1 = int(dfSum['Method Condition1'][topLoc])
        techCon2 = int(dfSum['Method Condition2'][topLoc])
        techCon3 = dfSum['Method Condition3'][topLoc]
        techCon4 = dfSum['Method Condition4'][topLoc]
        df['MACD'], df['MACD signal'], df['MACD hist'] = ta.MACD(df['close'], fastperiod = techCon0, slowperiod = techCon1, signalperiod = techCon2)
        df['result Base'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        for i in range(len(df)):
            if holding == 0 and MACD_BuyDecision(techCon3, df, i) is True:
                holding = 1
                buyPrice = df['mid'][i]
                buyDay = i
            elif holding == 1 and MACD_SellDecision(techCon4, df, i) is True:
                holding = 0
                if df['mid'][i] > buyPrice:
                    df['result Base'][buyDay:i + 1] = 'Win'
                elif df['mid'][i] < buyPrice:
                    df['result Base'][buyDay:i + 1] = 'Lose'
        df = df.rename(columns = {'MACD':'MACD - Base',
                                  'MACD signal':'MACD signal - Base',
                                  'MACD hist':'MACD hist - Base'
                                  }
                       )

    df.to_csv('Database/' + stock_i + '/Method Summary1.csv')

def MFI_tradeBeta(df, techCon0, techCon1, techCon2):
    df['MFI '] = ta.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod=techCon0)
    df['result'] = None

    holding, buyPrice, buyDay = 0, 0, 0
    for i in range(len(df)):
        if holding == 0 and df['MFI'][i] <= techCon1:
            holding = 1
            buyPrice = df['mid'][i]
            buyDay = i
        elif holding == 1 and df['MFI'][i] >= techCon2:
            holding = 0
            if df['mid'][i] > buyPrice:
                df['result'][buyDay:i + 1] = 'Win'
            elif df['mid'][i] < buyPrice:
                df['result'][buyDay:i + 1] = 'Lose'

def RSI_tradeBeta(df, techCon0, techCon1, techCon2, techCon3):
    df['RSI - Second'] = ta.RSI(df['close'], timeperiod=techCon0)
    df['result Second'] = None

    holding, buyPrice, buyDay = 0, 0, 0
    if techCon3 == 'Negative':
        for i in range(len(df)):
            if holding == 0 and df['RSI - Second'][i] < techCon1:
                holding = 1
                buyPrice = df['mid'][i]
                buyDay = i
            elif holding == 1 and df['RSI - Second'][i] > techCon2:
                holding = 0
                if df['mid'][i] > buyPrice:
                    df['result Second'][buyDay:i + 1] = 'Win'
                elif df['mid'][i] < buyPrice:
                    df['result Second'][buyDay:i + 1] = 'Lose'
    elif techCon3 == 'Positive':
        for i in range(len(df)):
            if holding == 0 and df['RSI - Second'][i] > techCon1:
                holding = 1
                buyPrice = df['mid'][i]
                buyDay = i
            elif holding == 1 and df['RSI - Second'][i] < techCon2:
                holding = 0
                if df['mid'][i] > buyPrice:
                    df['result Second'][buyDay:i + 1] = 'Win'
                elif df['mid'][i] < buyPrice:
                    df['result Second'][buyDay:i + 1] = 'Lose'

def tradeBeta(df, dfSum, tarLoc):
    discrete = int(0)
    if dfSum['Tech Method'][tarLoc] == 'MFI':
        techCon0 = int(dfSum['Method Condition0'][tarLoc])
        techCon1 = int(dfSum['Method Condition1'][tarLoc])
        techCon2 = int(dfSum['Method Condition2'][tarLoc])
        df['MFI - Second'] = ta.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod = techCon0)
        df['result Second'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        for i in range(len(df)):
            if holding == 0 and df['MFI - Second'][i] <= techCon1:
                holding = 1
                buyPrice = df['mid'][i]
                buyDay = i
            elif holding == 1 and df['MFI - Second'][i] >= techCon2:
                holding = 0
                if df['mid'][i] > buyPrice:
                    df['result Second'][buyDay:i + 1] = 'Win'
                elif df['mid'][i] < buyPrice:
                    df['result Second'][buyDay:i + 1] = 'Lose'
    elif dfSum['Tech Method'][tarLoc] == 'RSI':
        techCon0 = int(dfSum['Method Condition0'][tarLoc])
        techCon1 = int(dfSum['Method Condition1'][tarLoc])
        techCon2 = int(dfSum['Method Condition2'][tarLoc])
        techCon3 = str(dfSum['Method Condition3'][tarLoc])
        df['RSI - Second'] = ta.RSI(df['close'], timeperiod=techCon0)
        df['result Second'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        if techCon3 == 'Negative':
            for i in range(len(df)):
                if holding == 0 and df['RSI - Second'][i] < techCon1:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['RSI - Second'][i] > techCon2:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Lose'
        elif techCon3 == 'Positive':
            for i in range(len(df)):
                if holding == 0 and df['RSI - Second'][i] > techCon1:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['RSI - Second'][i] < techCon2:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Lose'

    elif dfSum['Tech Method'][tarLoc] == 'BB':
        techCon0 = int(dfSum['Method Condition0'][tarLoc])
        techCon1 = int(dfSum['Method Condition1'][tarLoc])
        techCon2 = int(dfSum['Method Condition2'][tarLoc])
        techCon3 = str(dfSum['Method Condition2'][tarLoc])
        df['BB Upper - Second'], df['BB Middle - Second'], df['BB Lower - Second'] = ta.BBANDS(df['close'], techCon0, techCon1, techCon2)
        df['result Base'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        if techCon3 == 'Bottom To Middle':
            for i in range(len(df)):
                if holding == 0 and df['BB Lower - Second'][i] >= df['low'][i]:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['BB Middle - Second'][i] <= df['high'][i]:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Lose'
        if techCon3 == 'Middle To Top':
            for i in range(len(df)):
                if holding == 0 and df['BB Middle - Second'][i] >= df['low'][i]:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['BB Upper - Second'][i] <= df['high'][i]:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Lose'
        if techCon3 == 'Bottom To Top':
            for i in range(len(df)):
                if holding == 0 and df['BB Lower - Second'][i] >= df['low'][i]:
                    holding = 1
                    buyPrice = df['mid'][i]
                    buyDay = i
                elif holding == 1 and df['BB Upper - Second'][i] <= df['high'][i]:
                    holding = 0
                    if df['mid'][i] > buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Win'
                    elif df['mid'][i] < buyPrice:
                        df['result Second'][buyDay:i + 1] = 'Lose'

    elif dfSum['Tech Method'][tarLoc] == 'SAR':
        techCon0 = int(dfSum['Method Condition0'][tarLoc])
        techCon1 = int(dfSum['Method Condition1'][tarLoc])
        techCon2 = int(dfSum['Method Condition2'][tarLoc])
        df['SAR - Second'] = ta.SAR(df['high'], df['low'], acceleration=techCon0, maximum=techCon1)
        df['result Base'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        for i in range(len(df) - 5):
            if holding == 0 and df['SAR - Second'][i] < df['close'][i] and df['SAR - Second'][i + techCon2] < df['close'][i + techCon2]:
                holding = 1
                buyPrice = df['mid'][i]
                buyDay = i
            elif holding == 1 and df['SAR - Second'][i] > df['close'][i]:
                holding = 0
                if df['mid'][i] > buyPrice:
                    df['result Second'][buyDay:i + 1] = 'Win'
                elif df['mid'][i] < buyPrice:
                    df['result Second'][buyDay:i + 1] = 'Lose'

    elif dfSum['Tech Method'][tarLoc] == 'MACD':
        techCon0 = int(dfSum['Method Condition0'][tarLoc])
        techCon1 = int(dfSum['Method Condition1'][tarLoc])
        techCon2 = int(dfSum['Method Condition2'][tarLoc])
        techCon3 = dfSum['Method Condition3'][tarLoc]
        techCon4 = dfSum['Method Condition4'][tarLoc]
        df['MACD'], df['MACD signal'], df['MACD hist'] = ta.MACD(df['close'], fastperiod=techCon0, slowperiod=techCon1, signalperiod=techCon2)
        df['result Base'] = None

        holding, buyPrice, buyDay = 0, 0, 0
        for i in range(len(df)):
            if holding == 0 and MACD_BuyDecision(techCon3, df, i) is True:
                holding = 1
                buyPrice = df['mid'][i]
                buyDay = i
            elif holding == 1 and MACD_SellDecision(techCon4, df, i) is True:
                holding = 0
                if df['mid'][i] > buyPrice:
                    df['result Base'][buyDay:i + 1] = 'Win'
                elif df['mid'][i] < buyPrice:
                    df['result Base'][buyDay:i + 1] = 'Lose'
        df = df.rename(columns = {'MACD':'MACD - Second',
                                  'MACD signal':'MACD signal - Second',
                                  'MACD hist':'MACD hist - Second'
                                  }
                       )

    for j in range(len(df)):
        if df['result Base'][j] != 'Win' and df['result Second'][j] == 'Win':
            discrete += 1

    return df, discrete



def RSI_Beta(stock_i,
             timePeriodStart = 11,
             timePeriodEnd = 18,
             buyConditionStart = 25,
             buyConditionEnd = 76,
             sellConditionStart = 25,
             sellConditionEnd = 76,
             saveReport = 1
             ):
    try:
        print('RSI Beta ' + stock_i + ' starting now...')
        RSI_df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
        RSI_Return, RSI_Return60, RSI_Rate, RSI_Trade, RSI_Holding_Period, RSI_period, RSI_Buy_level, RSI_Sell_level, \
        Method = [], [], [], [], [], [], [], [], []
        RSI_Condit_S, RSI_Condit_E = 25, 76

        for RSI_Timeperiod in range(timePeriodStart, timePeriodEnd):
            RSI_df['RSI'] = ta.RSI(RSI_df['close'], timeperiod = RSI_Timeperiod)

            for RSI_Sell_Condition in range(sellConditionStart, sellConditionEnd, 3):

                for RSI_Buy_Condition in range(buyConditionStart, buyConditionEnd, 3):
                    RSI_df['Buy'] = None
                    RSI_df['Sell'] = None
                    RSI_df['Perf'] = None
                    RSI_Holding, RSI_Portfolio, RSI_Count, RSI_Count_S, RSIHoldPeriod, RSI_Buy_Price, RSI_Buy_Day = 0, 100, 0, 0, 0, 0, 0
                    if RSI_Buy_Condition <= RSI_Sell_Condition:
                        continue
                    for RSI_i in range(1, len(RSI_df)):         # 開始迴圈測試

                        if RSI_i > 365 and RSI_Count <= 6:             #長期沒有達成交易條件則跳出該次迴圈
                            break
                        if RSI_Holding == 0 and RSI_df['RSI'][RSI_i] < RSI_Buy_Condition:             #RSI低於買入條件時買入(反向策略)
                            RSI_Buy_Price = RSI_df['mid'][RSI_i]
                            RSI_Holding = 1
                            RSI_df['Buy'][RSI_i] = RSI_Buy_Price
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio
                            RSI_Buy_Day = RSI_i
                        elif RSI_Holding == 1 and RSI_df['RSI'][RSI_i] > RSI_Sell_Condition:            #RSI高於賣出條件時賣出(反向策略)
                            RSI_Sell_Price = RSI_df['mid'][RSI_i]
                            RSI_Holding = 0
                            RSI_Portfolio *= RSI_Sell_Price / RSI_Buy_Price
                            RSI_Count += 1
                            RSIHoldPeriod += RSI_i - RSI_Buy_Day
                            RSI_df['Sell'][RSI_i] = RSI_Sell_Price
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio
                            if RSI_Sell_Price > RSI_Buy_Price:
                                RSI_Count_S += 1
                        else:
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio

                    if RSI_Count > 12:
                        RSI_Return.append(((RSI_Portfolio / 100) - 1) * 100)
                        if len(RSI_df) > 61:
                            RSI_Return60.append(((RSI_df['Perf'][len(RSI_df) - 1] / RSI_df['Perf'][len(RSI_df) - 61]) - 1) * 100)
                        else:
                            RSI_Return60.append('N/A')
                        RSI_Rate.append(Hit_Rate(RSI_Count_S, RSI_Count))
                        RSI_Trade.append(RSI_Count)
                        RSI_Holding_Period.append(RSIHoldPeriod/RSI_Count)
                        RSI_period.append(RSI_Timeperiod)
                        RSI_Buy_level.append(RSI_Buy_Condition)
                        RSI_Sell_level.append(RSI_Sell_Condition)
                        Method.append('Negative')
                    #RSI_df.to_csv('Analysis/RSI' + str(RSI_Timeperiod) + ' ' + str(RSI_Buy_Condition) + ' ' + str(RSI_Sell_Condition) + '.csv')
                    #print(RSI_Timeperiod, ' ', RSI_Buy_Condition, ' ', RSI_Sell_Condition, ' ', RSI_Portfolio)
        print(stock_i, ' RSI negative completed')

        for RSI_Timeperiod in range(11, 18):  # RSI參數, 取多少天的價格
            RSI_df['RSI'] = ta.RSI(RSI_df[('close')], timeperiod=RSI_Timeperiod)

            for RSI_Sell_Condition in range(RSI_Condit_S, RSI_Condit_E, 3):  # RSI賣出條件

                for RSI_Buy_Condition in range(RSI_Condit_S, RSI_Condit_E, 3):  # RSI買入條件
                    RSI_df['Buy'] = None
                    RSI_df['Sell'] = None
                    RSI_df['Perf'] = None
                    RSI_Holding, RSI_Portfolio, RSI_Count, RSI_Count_S, RSIHoldPeriod = 0, 100, 0, 0, 0

                    if RSI_Sell_Condition <= RSI_Buy_Condition:
                        continue

                    for RSI_i in range(1, len(RSI_df)):  # 開始迴圈測試

                        if RSI_i > 365 and RSI_Count <= 6:  # 長期沒有達成交易條件則跳出該次迴圈
                            break

                        if RSI_Holding == 0 and RSI_df['RSI'][RSI_i] > RSI_Buy_Condition:         #將上面的買入, 賣出條件倒轉
                            RSI_Buy_Price = RSI_df['mid'][RSI_i]
                            RSI_Holding = 1
                            RSI_df['Buy'][RSI_i] = RSI_Buy_Price
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio
                            RSI_Buy_Day = RSI_i

                        elif RSI_Holding == 1 and RSI_df['RSI'][RSI_i] < RSI_Sell_Condition:
                            RSI_Sell_Price = RSI_df['mid'][RSI_i]
                            RSI_Holding = 0
                            RSI_Portfolio *= RSI_Sell_Price / RSI_Buy_Price
                            RSI_Count += 1
                            RSIHoldPeriod += RSI_i - RSI_Buy_Day
                            RSI_df['Sell'][RSI_i] = RSI_Sell_Price
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio

                            if RSI_Sell_Price > RSI_Buy_Price:
                                RSI_Count_S += 1

                        else:
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio

                    if RSI_Count > 12:
                        RSI_Return.append(RSI_Portfolio)

                        if len(RSI_df) > 61:
                            RSI_Return60.append(((RSI_df['Perf'][len(RSI_df) - 1] / RSI_df['Perf'][len(RSI_df) - 61]) - 1) * 100)
                        else:
                            RSI_Return60.append('N/A')

                        RSI_Rate.append(Hit_Rate(RSI_Count_S, RSI_Count))
                        RSI_Trade.append(RSI_Count)
                        RSI_Holding_Period.append(RSIHoldPeriod/RSI_Count)
                        RSI_period.append(RSI_Timeperiod)
                        RSI_Buy_level.append(RSI_Buy_Condition)
                        RSI_Sell_level.append(RSI_Sell_Condition)
                        Method.append('Positive')

        RSI_Report = pd.DataFrame({'RSI Return':RSI_Return,
                                   'RSI Return60':RSI_Return60,
                                   'RSI Hit Rate': RSI_Rate,
                                   'RSI Trade Time': RSI_Trade,
                                   'RSI Holding Period':RSI_Holding_Period,
                                   'RSI Time Period':RSI_period,
                                   'RSI Buy Condition':RSI_Buy_level,
                                   'RSI Sell Condition':RSI_Sell_level,
                                   'RSI Method':Method
                                   })
        RSI_Report.to_csv('Database/' + stock_i + '/RSI Summary.csv')
        print(stock_i, ' RSI positive completed')
    except:
        print(stock_i, ' RSI Error')



def BB_Beta(stock_i):
    print('BB Beta ' + stock_i + ' starting now...')
    BB_df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
    BB_Return, BB_Return60, BB_Rate, BB_Trade, BB_Holding_Period, BB_Time_Period, BB_Upper, BB_Lower, BB_Middle, BB_Method = [], [], [], [], [], [], [], [], [], []

    for BB_TPeriod in range(3, 8):

        for BB_Up in np.arange(1, 2.1, 0.1):

            for BB_Low in np.arange(1, 2.1, 0.1):

                BB_df['BB Upper'], BB_df['BB Middle'], BB_df['BB Lower'] = ta.BBANDS(BB_df['close'], BB_TPeriod, BB_Up, BB_Low)
                BB_df['Perf'] = None
                BB_Holding, BB_Portfolio, BB_Count, BB_Count_S, BBHoldPeriod, BB_Buy_Price, BB_Buy_Day = 0, 100, 0, 0, 0, 0, 0
                for BB_i in range(1, len(BB_df)):

                    if BB_df['BB Lower'][BB_i] >= BB_df['low'][BB_i] and BB_Holding == 0:                            #低於下行通道便買入
                        BB_Buy_Price = BB_df['mid'][BB_i]
                        BB_Holding = 1
                        BB_Buy_Day = BB_i
                        BB_df['Perf'] = BB_Portfolio
                    elif BB_df['BB Middle'][BB_i] <= BB_df['high'][BB_i] and BB_Holding == 1:                         #超過中軸賣出
                        BB_Sell_Price = BB_df['mid'][BB_i]
                        BB_Holding = 0
                        BB_Portfolio *= BB_Sell_Price/BB_Buy_Price
                        BBHoldPeriod = BB_i - BB_Buy_Day
                        BB_Count += 1
                        BB_df['Perf'] = BB_Portfolio
                        if BB_Sell_Price > BB_Buy_Price:
                            BB_Count_S += 1

                BB_Return.append(((BB_Portfolio / 100) - 1) * 100)
                if len(BB_df) > 61:
                    BB_Return60.append(((BB_df['Perf'][len(BB_df) - 1] / BB_df['Perf'][len(BB_df) - 61]) - 1) * 100)
                else:
                    BB_Return60.append('N/A')
                BB_Rate.append(Hit_Rate(BB_Count_S, BB_Count))
                BB_Trade.append(BB_Count)
                BB_Holding_Period.append(Hit_Rate(BBHoldPeriod, BB_Count))
                BB_Time_Period.append(BB_TPeriod)
                BB_Upper.append(BB_Up)
                BB_Lower.append(BB_Low)
                BB_Method.append('Bottom To Middle')

                BB_Holding, BB_Portfolio, BB_Count, BB_Count_S, BBHoldPeriod, BB_Buy_Price, BB_Buy_Day = 0, 100, 0, 0, 0, 0, 0
                for BB_j in range(1, len(BB_df)):
                    if BB_df['BB Middle'][BB_j] >= BB_df['low'][BB_j] and BB_Holding == 0:                            #低於下行通道便買入
                        BB_Buy_Price = BB_df['mid'][BB_j]
                        BB_Holding = 1
                        BB_Buy_Day = BB_j
                        BB_df['Perf'] = BB_Portfolio
                    elif BB_df['BB Upper'][BB_j] <= BB_df['high'][BB_j] and BB_Holding == 1:                         #超過中軸賣出
                        BB_Sell_Price = BB_df['mid'][BB_j]
                        BB_Holding = 0
                        BB_Portfolio *= BB_Sell_Price/BB_Buy_Price
                        BBHoldPeriod = BB_j - BB_Buy_Day
                        BB_Count += 1
                        BB_df['Perf'] = BB_Portfolio
                        if BB_Sell_Price > BB_Buy_Price:
                            BB_Count_S += 1

                BB_Return.append(((BB_Portfolio / 100) - 1) * 100)
                if len(BB_df) > 61:
                    BB_Return60.append(((BB_df['Perf'][len(BB_df) - 1] / BB_df['Perf'][len(BB_df) - 61]) - 1) * 100)
                else:
                    BB_Return60.append('N/A')
                BB_Rate.append(Hit_Rate(BB_Count_S, BB_Count))
                BB_Trade.append(BB_Count)
                BB_Holding_Period.append(Hit_Rate(BBHoldPeriod, BB_Count))
                BB_Time_Period.append(BB_TPeriod)
                BB_Upper.append(BB_Up)
                BB_Lower.append(BB_Low)
                BB_Method.append('Middle To Top')

                BB_Holding, BB_Portfolio, BB_Count, BB_Count_S, BBHoldPeriod, BB_Buy_Price, BB_Buy_Day = 0, 100, 0, 0, 0, 0, 0
                for BB_k in range(1, len(BB_df)):
                    if BB_df['BB Lower'][BB_k] >= BB_df['low'][BB_k] and BB_Holding == 0:  # 低於下行通道便買入
                        BB_Buy_Price = BB_df['mid'][BB_k]
                        BB_Holding = 1
                        BB_Buy_Day = BB_k
                        BB_df['Perf'] = BB_Portfolio
                    elif BB_df['BB Upper'][BB_k] <= BB_df['high'][BB_k] and BB_Holding == 1:  # 超過中軸賣出
                        BB_Sell_Price = BB_df['mid'][BB_k]
                        BB_Holding = 0
                        BB_Portfolio *= BB_Sell_Price / BB_Buy_Price
                        BBHoldPeriod = BB_k - BB_Buy_Day
                        BB_Count += 1
                        BB_df['Perf'] = BB_Portfolio
                        if BB_Sell_Price > BB_Buy_Price:
                            BB_Count_S += 1

                BB_Return.append(((BB_Portfolio / 100) - 1) * 100)
                if len(BB_df) > 61:
                    BB_Return60.append(((BB_df['Perf'][len(BB_df) - 1] / BB_df['Perf'][len(BB_df) - 61]) - 1) * 100)
                else:
                    BB_Return60.append('N/A')
                BB_Rate.append(Hit_Rate(BB_Count_S, BB_Count))
                BB_Trade.append(BB_Count)
                BB_Holding_Period.append(Hit_Rate(BBHoldPeriod, BB_Count))
                BB_Time_Period.append(BB_TPeriod)
                BB_Upper.append(BB_Up)
                BB_Lower.append(BB_Low)
                BB_Method.append('Bottom To Top')

    BB_Report = pd.DataFrame({'BB Return' : BB_Return,
                              'BB Return60': BB_Return60,
                              'BB Hit Rate' : BB_Rate,
                              'BB Trade Time' : BB_Trade,
                              'BB Holding Period' : BB_Holding_Period,
                              'BB Time period' : BB_Time_Period,
                              'BB Upper' : BB_Upper,
                              'BB Lower' : BB_Lower,
                              'BB Method' : BB_Method
                              })
    BB_Report.to_csv('Database/' + stock_i + '/BB Summary.csv')


def MACD_BuyDecision(method, df, loc_i, bottomLine = -0.5):
    if method == 'negativeToPositive' and df['MACD hist'][loc_i] > 0 and df['MACD hist'][loc_i - 1] <= 0:  # 柱狀圖由負變正為買入
        return True
    elif method == 'nearToZero' and df['MACD hist'][loc_i] > df['MACD hist'][loc_i - 1] > df['MACD hist'][loc_i - 2] and df['MACD hist'][loc_i] > bottomLine:
        return True
    elif method == 'beyondSignal' and df['MACD'][loc_i] > df['MACD signal'][loc_i] and df['MACD'][loc_i - 1] < df['MACD signal'][loc_i - 1]:
        return True
    else:
        return False

def MACD_SellDecision(method, df, loc_i, histLevel=0.3):
    if method == 'histBelowPreHist' and df['MACD hist'][loc_i] < df['MACD hist'][loc_i - 1]:
        return True
    elif method == 'histLevel' and df['MACD hist'][loc_i] > histLevel and df['MACD hist'][loc_i - 1] > histLevel:
        return True
    elif method == 'beyondMACD' and df['MACD signal'][loc_i] > df['MACD'][loc_i] and df['MACD signal'][loc_i - 1] < df['MACD'][loc_i - 1]:
        return True
    else:
        return False

def MACD_Beta(stock_i):
    print('MACD Beta ' + stock_i + ' starting now...')
    MACD_df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
    MACD_df = MACD_df.drop(['Unnamed: 0'], axis = 1)
    MACD_Return, MACD_Return60, MACD_Rate, MACD_Trade, MACD_HoldingPeriod, MACD_Fast, MACD_Slow, MACD_Signal, methodBuy, methodSell = [], [], [], [], [], [], [], [], [], []

    for MACD_F in range(9, 16):

        for MACD_S in range(23, 30):

            for MACD_Sig in range(6, 12):
                MACD_df['MACD'], MACD_df['MACD signal'], MACD_df['MACD hist'] = ta.MACD(MACD_df['close'], fastperiod = MACD_F, slowperiod = MACD_S, signalperiod = MACD_Sig)

                for buy_i in ['negativeToPositive', 'nearToZero', 'beyondSignal']:

                    for sell_i in ['histBelowPreHist', 'histLevel', 'beyondMACD']:
                        MACD_BuyPrice, MACD_SellPrice, MACD_BuyDay, MACD_Portfolio = 0, 0, 0, 100
                        MACD_Holding, MACD_Count, MACD_CountS, MACD_HoldPeriod = 0, 0, 0, 0
                        MACD_df['Perf'] = None

                        for MACD_i in range(6, len(MACD_df)):
                            if MACD_Holding == 0 and MACD_BuyDecision(buy_i, MACD_df, MACD_i) is True:
                                MACD_Holding = 1
                                MACD_BuyPrice = MACD_df['mid'][MACD_i]
                                MACD_BuyDay = MACD_i
                            elif MACD_Holding == 1 and MACD_SellDecision(sell_i, MACD_df, MACD_i) is True:
                                MACD_Holding = 0
                                MACD_HoldPeriod += MACD_i - MACD_BuyDay
                                MACD_SellPrice = MACD_df['mid'][MACD_i]
                                MACD_Portfolio *= MACD_SellPrice / MACD_BuyPrice
                                if MACD_SellPrice > MACD_BuyPrice:
                                    MACD_CountS += 1
                            MACD_df['Perf'][MACD_i] = MACD_Portfolio

                        MACD_Return.append(MACD_Portfolio)
                        if len(MACD_df) > 61:
                            MACD_Return60.append(((MACD_df['Perf'][len(MACD_df) - 1] / MACD_df['Perf'][len(MACD_df) - 61]) - 1) * 100)
                        MACD_Rate.append(Hit_Rate(MACD_CountS, MACD_Count))
                        MACD_Trade.append(MACD_Count)
                        MACD_HoldingPeriod.append(Hit_Rate(MACD_HoldPeriod, MACD_Count))
                        MACD_Fast.append(MACD_F)
                        MACD_Slow.append(MACD_S)
                        MACD_Signal.append(MACD_Sig)
                        methodBuy.append(buy_i)
                        methodSell.append(sell_i)
                        #MACD_df.to_csv('Analysis/MACD F%s S%s Sig%s buy%s sell%s.csv' %(MACD_F, MACD_S, MACD_Sig, buy_i, sell_i))

    MACD_Report = pd.DataFrame({
        'MACD Return' : MACD_Return,
        'MACD Return60' : MACD_Return60,
        'MACD Hit Rate' : MACD_Rate,
        'MACD Trade Time' : MACD_Trade,
        'MACD Fast' : MACD_Fast,
        'MACD Slow' : MACD_Slow,
        'MACD Signal' : MACD_Signal,
        'MACD Buy method': methodBuy,
        'MACD Sell method': methodSell,
    })
    MACD_Report.to_csv('Database/' + stock_i + '/MACD Summary.csv')



def MFI_Beta(stock_i,
             timePeriodStart = 14, 
             timePeriodEnd = 15, 
             buyConditionStart = 20, 
             buyConditionEnd = 41, 
             sellConditionStart = 60, 
             sellConditionEnd = 81, 
             saveReport = 1):

    try:
        print('MFI_Beta ' + stock_i + ' starting now...')
        MFI_df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv', index_col=0)
        MFI_Return, MFI_Return60, MFI_Rate, MFI_Trade, MFI_Holding_Period, MFI_Time_Period, MFI_Buy_Level, MFI_Sell_Level = [], [], [] ,[] ,[], [], [], []

        for MFI_TPeriod in range(timePeriodStart, timePeriodEnd):

            for MFI_Buy_Condition in range(buyConditionStart, buyConditionEnd, 2):

                for MFI_Sell_Condition in range(sellConditionStart, sellConditionEnd, 2):
                    MFI_df['MFI'] = ta.MFI(MFI_df['high'], MFI_df['low'], MFI_df['close'], MFI_df['volume'], timeperiod = MFI_TPeriod)
                    MFI_df['Perf'] = None
                    MFI_df['resultMFI'] = None
                    MFI_Holding, MFI_Portfolio, MFI_Count, MFI_Count_S, MFIHoldPeriod, MFI_Buy_Day, MFI_Buy_Price = 0, 100, 0, 0, 0, 0, 0

                    for MFI_i in range(1, len(MFI_df)):

                        if MFI_Holding == 0 and MFI_df['MFI'][MFI_i] <= MFI_Buy_Condition:
                            MFI_Holding = 1
                            MFI_Buy_Price = MFI_df['mid'][MFI_i]
                            MFI_df['Perf'][MFI_i] = MFI_Portfolio
                            MFI_Buy_Day = MFI_i
                        elif MFI_Holding == 1 and MFI_df['MFI'][MFI_i] >= MFI_Sell_Condition:
                            MFI_Holding = 0
                            MFI_Sell_Price = MFI_df['mid'][MFI_i]
                            MFI_Portfolio *= MFI_Sell_Price / MFI_Buy_Price
                            MFI_df['Perf'][MFI_i] = MFI_Portfolio
                            MFIHoldPeriod += MFI_i - MFI_Buy_Day
                            MFI_Count += 1
                            if MFI_Sell_Price > MFI_Buy_Price:
                                MFI_Count_S += 1
                                MFI_df['resultMFI'][MFI_Buy_Day: MFI_i + 1] = 'Win'
                            elif MFI_Sell_Price < MFI_Buy_Price:
                                MFI_df['resultMFI'][MFI_Buy_Day: MFI_i + 1] = 'Lose'
                        else:
                            MFI_df['Perf'][MFI_i] = MFI_Portfolio
                    if saveReport == 0:
                        #MFI_df.to_csv('Analysis/' + stock_i + '.csv')
                        return MFI_df

                    if saveReport == 1:
                        MFI_Return.append(((MFI_Portfolio / 100) - 1) * 100)
                        if len(MFI_df) > 61:
                            MFI_Return60.append(((MFI_df['Perf'][len(MFI_df) - 1] / MFI_df['Perf'][len(MFI_df) - 61]) - 1) * 100)
                        else:
                            MFI_Return60.append('N/A')
                        MFI_Rate.append(Hit_Rate(MFI_Count_S, MFI_Count))
                        MFI_Trade.append(MFI_Count)
                        MFI_Holding_Period.append(Hit_Rate(MFIHoldPeriod, MFI_Count))
                        MFI_Time_Period.append(MFI_TPeriod)
                        MFI_Buy_Level.append(MFI_Buy_Condition)
                        MFI_Sell_Level.append(MFI_Sell_Condition)

        if saveReport == 1:
            MFI_Report = pd.DataFrame({'MFI Return': MFI_Return,
                                       'MFI Return60': MFI_Return60,
                                       'MFI Hit Rate': MFI_Rate,
                                       'MFI Trade Time': MFI_Trade,
                                       'MFI Holding Period': MFI_Holding_Period,
                                       'MFI Time Period': MFI_Time_Period,
                                       'MFI Buy Condition': MFI_Buy_Level,
                                       'MFI Sell Condition': MFI_Sell_Level
                                       }
                                      )
            MFI_Report.to_csv('Database/' + stock_i + '/MFI Summary.csv')

    except:
        print(stock_i, ' MFI Error')


def model_1(p_list, index_dec = 0.15, timeperiod = 3):

    change_list = p_list.pct_change()
    P1 = change_list.apply(lambda a: a * -100 if a <0 else 0)           #只取下趺幅度, 把其正負倒轉, 大於或等於0的話返回零
    P2 = []

    for i in range(timeperiod):
        P2.append(0)

    for j in range(timeperiod, len(P1), 1):
        pi = 0
        for k in range(timeperiod):
            pi += P1[j - k] * (1 - index_dec * k)
        P2.append(pi)

    return P2

def model_1_Beta(Ticket):

    for stock_i in Ticket:

        try:
            print('Model_1_Beta ' + stock_i + ' starting now...')
            Mod1_df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
            Mod1_Return, Mod1_Return60, Mod1_Rate, Mod1_Trade, Mod1_Holding_Period, Mod1_Time_Period, Mod1_Buy_Level = [], [], [] ,[] ,[], [], []

            for Mod1_Timeperiod in range(3, 7):

                for Mod1_Buy_Condition in range(5, 12):
                    Mod1_df['PI'] = model_1(Mod1_df['close'], timeperiod = Mod1_Timeperiod)
                    Mod1_df['Buy'] = None
                    Mod1_df['Sell'] = None
                    Mod1_df['Perf'] = None
                    Mod1_Holding, Mod1_Portfolio, Mod1_Count, Mod1_Count_S, Mod1HoldPeriod = False, 100, 0, 0, 0

                    for Mod1_i in range(1, len(Mod1_df)):

                        if Mod1_Holding == False and Mod1_df['PI'][Mod1_i] > Mod1_Buy_Condition:
                            Mod1_Holding = True
                            Mod1_df['Buy'][Mod1_i] = Mod1_Buy_Price = Mod1_df['Mid'][Mod1_i]
                            Mod1_df['Perf'][Mod1_i] = Mod1_Portfolio
                            Mod1_Buy_Day = Mod1_i

                        elif Mod1_Holding == True:
                            Mod1_Holding = False
                            Mod1_df['Sell'][Mod1_i] = Mod1_Sell_Price = Mod1_df['Mid'][Mod1_i]
                            Mod1_Portfolio = Mod1_Sell_Price / Mod1_Buy_Price
                            Mod1_df['Perf'][Mod1_i] = Mod1_Portfolio
                            Mod1HoldPeriod += Mod1_i - Mod1_Buy_Day
                            Mod1_Count += 1

                            if Mod1_Sell_Price > Mod1_Buy_Price:
                                Mod1_Count_S += 1

                        else:
                            Mod1_df['Perf'][Mod1_i] = Mod1_Portfolio
                    Mod1_Return.append(((Mod1_Portfolio / 100) -1) * 100)

                    if len(Mod1_df) > 61:
                        Mod1_Return60.append(((Mod1_df['Perf'][len(Mod1_df) - 1] / Mod1_df['Perf'][len(Mod1_df) - 61]) - 1) *100)
                    else:
                        Mod1_Return60.append('N/A')

                    Mod1_Rate.append(Hit_Rate(Mod1_Count_S, Mod1_Count))
                    Mod1_Trade.append(Mod1_Count)
                    Mod1_Holding_Period.append(Hit_Rate(Mod1HoldPeriod, Mod1_Count))
                    Mod1_Time_Period.append(Mod1_Timeperiod)
                    Mod1_Buy_Level.append(Mod1_Buy_Condition)

            Mod1_Report = pd.DataFrame({'Mod1 Return': Mod1_Return, 'Mod1 Return60': Mod1_Return60, 'Mod1 Hit Rate': Mod1_Rate, 'Mod1 Trade Time': Mod1_Trade, \
                'Mod1 Holding Period': Mod1_Holding_Period, 'Mod1 Time Period': Mod1_Time_Period, 'Mod1 Buy Condition': Mod1_Buy_Level})
            Mod1_Report.to_csv('Database/' + stock_i +  '/Mod1 Summary.csv')
            

        except:
            print('Model 1 Error ' + stock_i)



#def model_2(p_list): #趨勢判斷

#def model_3():



def dailize(Ticket):
    df = pd.read_csv('Database/' + Ticket + '/' + Ticket +'_min.csv')
    df = df.drop(['Unnamed: 0'], axis=1)
    stockMarket = df['code'][0]
    dfDaily = pd.DataFrame(
        columns=[
        'Date',
        'Open',
        'High',
        'Low',
        'Close',
        'Volume',
        'Mid'
        ]
    )
    da, op, hi, lo, cl, vo = [], [], [], [], [], []

    if stockMarket[:2] == 'US':
        i, largest, lowest = 0, 0, 0
        while i < len(df):
            today = df['time'][i]
            today = datetime.datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
            da.append(today.date())
            op.append(df['cur_price'][i])
            cl.append(df['cur_price'][i + 389])

            largest = df['cur_price'][i]
            for j in range(i, i + 390):
                if df['cur_price'][j] > largest:
                    largest = df['cur_price'][j]
            hi.append(largest)

            lowest = df['cur_price'][i]
            for k in range(i, i + 390):
                if df['cur_price'][k] < lowest:
                    lowest = df['cur_price'][k]
            lo.append(lowest)

            tur = 0
            for l in range(i, i + 390):
                tur += df['turnover'][l]
            vo.append(tur)
            
            i += 390
    elif stockMarket[:2] == 'HK':
        print('HK Market')

    dfDaily['Date'], dfDaily['Open'], dfDaily['Close'], dfDaily['High'], dfDaily['Low'], dfDaily['Volume'] = da, op, cl, hi, lo, vo
    dfDaily['Mid'] = (dfDaily['High'] + dfDaily['Low']) / 2
    print(dfDaily)
    dfDaily.to_csv('Database/' + Ticket + '/' + Ticket +'.csv')


def SAR_Beta(df, SAR_Acc_List, SAR_Max_List, SAR_Delay_List):

    for sarAcc, sarMax, sarDelay in itertools.product(SAR_Acc_List, SAR_Max_List, SAR_Delay_List):
        df['SAR'] = ta.SAR(df['high'], df['low'], acceleration=sarAcc, maximum=sarMax)


def beta(stock_i, connection):

    print('SAR Beta ' + stock_i + ' starting now...')
    stock_i_ = stock_i.replace('.', '_')
    SAR_df = data_request(connection, stock_i_)
    SAR_Return, SAR_Return60, SAR_Rate, SAR_Trade, SAR_Holding_Period, SAR_Acceleration, SAR_MaxValue, SAR_Postpone = [], [], [], [], [], [], [], []


    for SAR_Acc in np.arange(0.03, 0.06, 0.01):              # SAR變速因子

        for SAR_Max in np.arange(0.3, 0.6, 0.1):              # SAR起始及上限

            for SAR_Delay in range(0, 3):                       # 加入延後買入條件

                try:

                    SAR_df['SAR'] = ta.SAR(SAR_df['high'], SAR_df['low'], acceleration=SAR_Acc, maximum=SAR_Max)       #計算SAR於暫存的dataframe
                    SAR_df['Buy'] = None
                    SAR_df['Sell'] = None
                    SAR_df['Perf'] = None
                    SAR_Holding, SAR_Portfolio, SAR_Count, SAR_Count_S, SARHoldPeriod, SAR_Buy_Price, SAR_Buy_Day, SAR_Break = 0, 100, 0, 0, 0, 0, 0, 0       #迴圈開始前將數據回復預設

                    SAR_i = 0  # for SAR_i in range(0, len(SAR_df)):
                    while SAR_i < len(SAR_df):

                        SAR_df['Perf'][SAR_i] = SAR_Portfolio
                        for x in range(0, SAR_Delay + 1):                                  #因為延後因素關係會跳過一些行數, 所以先行補充了下面的數值, 避免None出現
                            SAR_df['Perf'][SAR_i + x] = SAR_Portfolio

                        if SAR_i > 365 and SAR_Count <= 1:  # 長期沒有達成交易條件則跳出該次迴圈
                            SAR_Break = 1
                            break

                        elif SAR_Holding == 0:

                            if SAR_i + SAR_Delay >= len(SAR_df):             #防止因為延後買入而可以會溢出
                                break

                            elif SAR_df['SAR'][SAR_i] < SAR_df['close'][SAR_i] and SAR_df['SAR'][SAR_i + SAR_Delay] < SAR_df['close'][SAR_i + SAR_Delay]:
                                SAR_Buy_Price = SAR_df['mid'][SAR_i + SAR_Delay]
                                SAR_Holding = 1
                                SAR_i += SAR_Delay
                                SAR_df['Buy'][SAR_i] = SAR_Buy_Price
                                SAR_df['Perf'][SAR_i] = SAR_Portfolio
                                SAR_Buy_Day = SAR_i

                        elif SAR_Holding == 1 and SAR_df['SAR'][SAR_i] > SAR_df['close'][SAR_i]:
                            SAR_Sell_Price = SAR_df['mid'][SAR_i]
                            SAR_Holding = 0
                            SAR_Portfolio *= SAR_Sell_Price / SAR_Buy_Price
                            SAR_Count += 1
                            SARHoldPeriod += SAR_i - SAR_Buy_Day
                            SAR_df['Sell'][SAR_i] = SAR_Sell_Price
                            SAR_df['Perf'][SAR_i] = SAR_Portfolio

                            if SAR_Sell_Price > SAR_Buy_Price:
                                SAR_Count_S += 1

                        SAR_i += 1

                    if SAR_Count > 1 and SAR_Break == 0:
                        SAR_Return.append(((SAR_Portfolio / 100) - 1) * 100)

                        if len(SAR_df) > 61:
                            SAR_Return60.append(((SAR_df['Perf'][len(SAR_df) - 1] / SAR_df['Perf'][len(SAR_df) - 61]) - 1) * 100)
                        else:
                            SAR_Return60.append('N/A')

                        SAR_Rate.append(Hit_Rate(SAR_Count_S, SAR_Count))
                        SAR_Trade.append(SAR_Count)
                        SAR_Holding_Period.append(SARHoldPeriod/SAR_Count)
                        SAR_Acceleration.append(SAR_Acc)
                        SAR_MaxValue.append(SAR_Max)
                        SAR_Postpone.append(SAR_Delay)

                except:
                    print(stock_i, ' _ ' + str(SAR_Acc), ' _ ' + str(SAR_Max), ' _ ' + str(SAR_Delay), ' Error')

    SAR_Report = pd.DataFrame({'SAR Return' : SAR_Return, 'SAR Return60' : SAR_Return60, 'SAR Hit Rate' : SAR_Rate, 'SAR Trade Time' : SAR_Trade, \
                               'SAR Holding Period' : SAR_Holding_Period, 'SAR Acceleration' : SAR_Acceleration, \
                               'SAR Max Value' : SAR_MaxValue, 'SAR Postpone' : SAR_Postpone})
    SAR_Report.to_csv('Database/' + stock_i + '/SAR Summary.csv')
    '''
    try:
        print('RSI Beta ' + stock_i + ' starting now...')
        RSI_df = data_request(connection, stock_i_)
        RSI_Return, RSI_Return60, RSI_Rate, RSI_Trade, RSI_Holding_Period, RSI_period, RSI_Buy_level, RSI_Sell_level, \
        Method = [], [], [], [], [], [], [], [], []
        RSI_Condit_S, RSI_Condit_E = 25, 76

        for RSI_Timeperiod in range(11, 18):
            RSI_df['RSI'] = ta.RSI(RSI_df['close'], timeperiod=RSI_Timeperiod)

            for RSI_Sell_Condition in range(RSI_Condit_S, RSI_Condit_E, 3):

                for RSI_Buy_Condition in range(RSI_Condit_S, RSI_Condit_E, 3):
                    RSI_df['Buy'] = None
                    RSI_df['Sell'] = None
                    RSI_df['Perf'] = None
                    RSI_Holding, RSI_Portfolio, RSI_Count, RSI_Count_S, RSIHoldPeriod = 0, 100, 0, 0, 0

                    if RSI_Buy_Condition <= RSI_Sell_Condition:
                        continue

                    for RSI_i in range(1, len(RSI_df)):  # 開始迴圈測試

                        if RSI_i > 365 and RSI_Count <= 6:  # 長期沒有達成交易條件則跳出該次迴圈
                            break

                        if RSI_Holding == 0 and RSI_df['RSI'][RSI_i] < RSI_Buy_Condition:  # RSI低於買入條件時買入(反向策略)
                            RSI_Buy_Price = RSI_df['mid'][RSI_i]
                            RSI_Holding = 1
                            RSI_df['Buy'][RSI_i] = RSI_Buy_Price
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio
                            RSI_Buy_Day = RSI_i
                        elif RSI_Holding == 1 and RSI_df['RSI'][RSI_i] > RSI_Sell_Condition:  # RSI高於賣出條件時賣出(反向策略)
                            RSI_Sell_Price = RSI_df['mid'][RSI_i]
                            RSI_Holding = 0
                            RSI_Portfolio *= RSI_Sell_Price / RSI_Buy_Price
                            RSI_Count += 1
                            RSIHoldPeriod += RSI_i - RSI_Buy_Day
                            RSI_df['Sell'][RSI_i] = RSI_Sell_Price
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio
                            if RSI_Sell_Price > RSI_Buy_Price:
                                RSI_Count_S += 1
                        else:
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio

                    if RSI_Count > 12:
                        RSI_Return.append(((RSI_Portfolio / 100) - 1) * 100)
                        if len(RSI_df) > 61:
                            RSI_Return60.append(((RSI_df['Perf'][len(RSI_df) - 1] / RSI_df['Perf'][len(RSI_df) - 61]) - 1) * 100)
                        else:
                            RSI_Return60.append('N/A')
                        RSI_Rate.append(Hit_Rate(RSI_Count_S, RSI_Count))
                        RSI_Trade.append(RSI_Count)
                        RSI_Holding_Period.append(RSIHoldPeriod / RSI_Count)
                        RSI_period.append(RSI_Timeperiod)
                        RSI_Buy_level.append(RSI_Buy_Condition)
                        RSI_Sell_level.append(RSI_Sell_Condition)
                        Method.append('Negative')
                    # RSI_df.to_csv('Analysis/RSI' + str(RSI_Timeperiod) + ' ' + str(RSI_Buy_Condition) + ' ' + str(RSI_Sell_Condition) + '.csv')
                    # print(RSI_Timeperiod, ' ', RSI_Buy_Condition, ' ', RSI_Sell_Condition, ' ', RSI_Portfolio)
        print(stock_i, ' RSI negative completed')

        for RSI_Timeperiod in range(11, 18):  # RSI參數, 取多少天的價格
            RSI_df['RSI'] = ta.RSI(RSI_df[('close')], timeperiod=RSI_Timeperiod)

            for RSI_Sell_Condition in range(RSI_Condit_S, RSI_Condit_E, 3):  # RSI賣出條件

                for RSI_Buy_Condition in range(RSI_Condit_S, RSI_Condit_E, 3):  # RSI買入條件
                    RSI_df['Buy'] = None
                    RSI_df['Sell'] = None
                    RSI_df['Perf'] = None
                    RSI_Holding, RSI_Portfolio, RSI_Count, RSI_Count_S, RSIHoldPeriod = 0, 100, 0, 0, 0

                    if RSI_Sell_Condition <= RSI_Buy_Condition:
                        continue

                    for RSI_i in range(1, len(RSI_df)):  # 開始迴圈測試

                        if RSI_i > 365 and RSI_Count <= 6:  # 長期沒有達成交易條件則跳出該次迴圈
                            break

                        if RSI_Holding == 0 and RSI_df['RSI'][RSI_i] > RSI_Buy_Condition:  # 將上面的買入, 賣出條件倒轉
                            RSI_Buy_Price = RSI_df['mid'][RSI_i]
                            RSI_Holding = 1
                            RSI_df['Buy'][RSI_i] = RSI_Buy_Price
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio
                            RSI_Buy_Day = RSI_i

                        elif RSI_Holding == 1 and RSI_df['RSI'][RSI_i] < RSI_Sell_Condition:
                            RSI_Sell_Price = RSI_df['mid'][RSI_i]
                            RSI_Holding = 0
                            RSI_Portfolio *= RSI_Sell_Price / RSI_Buy_Price
                            RSI_Count += 1
                            RSIHoldPeriod += RSI_i - RSI_Buy_Day
                            RSI_df['Sell'][RSI_i] = RSI_Sell_Price
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio

                            if RSI_Sell_Price > RSI_Buy_Price:
                                RSI_Count_S += 1

                        else:
                            RSI_df['Perf'][RSI_i] = RSI_Portfolio

                    if RSI_Count > 12:
                        RSI_Return.append(RSI_Portfolio)

                        if len(RSI_df) > 61:
                            RSI_Return60.append(((RSI_df['Perf'][len(RSI_df) - 1] / RSI_df['Perf'][len(RSI_df) - 61]) - 1) * 100)
                        else:
                            RSI_Return60.append('N/A')

                        RSI_Rate.append(Hit_Rate(RSI_Count_S, RSI_Count))
                        RSI_Trade.append(RSI_Count)
                        RSI_Holding_Period.append(RSIHoldPeriod / RSI_Count)
                        RSI_period.append(RSI_Timeperiod)
                        RSI_Buy_level.append(RSI_Buy_Condition)
                        RSI_Sell_level.append(RSI_Sell_Condition)
                        Method.append('Positive')

        RSI_Report = pd.DataFrame({'RSI Return': RSI_Return,
                                   'RSI Return60': RSI_Return60,
                                   'RSI Hit Rate': RSI_Rate,
                                   'RSI Trade Time': RSI_Trade,
                                   'RSI Holding Period': RSI_Holding_Period,
                                   'RSI Time Period': RSI_period,
                                   'RSI Buy Condition': RSI_Buy_level,
                                   'RSI Sell Condition': RSI_Sell_level,
                                   'RSI Method': Method
                                   })
        RSI_Report.to_csv('Database/' + stock_i + '/RSI Summary.csv')
        print(stock_i, ' RSI negative completed')
    except:
        print(stock_i, ' RSI Error')

    try:
        print('BB Beta ' + stock_i + ' starting now...')
        BB_df = data_request(connection, stock_i_)
        BB_Return, BB_Return60, BB_Rate, BB_Trade, BB_Holding_Period, BB_Time_Period, BB_Upper, BB_Lower, BB_Middle = [], [], [], [], [], [], [], [], []

        for BB_TPeriod in range(3, 8):

            for BB_Up in np.arange(1, 2.1, 0.2):

                for BB_Low in np.arange(1, 2.1, 0.2):

                    BB_df['BB Upper'], BB_df['BB Middle'], BB_df['BB Lower'] = ta.BBANDS(BB_df['close'], BB_TPeriod, BB_Up, BB_Low)
                    BB_df['Buy'] = None
                    BB_df['Sell'] = None
                    BB_df['Perf'] = None
                    BB_Holding, BB_Portfolio, BB_Count, BB_Count_S, BBHoldPeriod, BB_Buy_Price, BB_Buy_Day = 0, 100, 0, 0, 0, 0, 0

                    for BB_i in range(1, len(BB_df)):

                        if BB_df['BB Lower'][BB_i] > BB_df['close'][BB_i] and BB_Holding == 0:  # 低於下行通道便買入
                            BB_df['Buy'][BB_i] = BB_Buy_Price = BB_df['mid'][BB_i]
                            BB_Holding = 1
                            BB_Buy_Day = BB_i
                            BB_df['Perf'] = BB_Portfolio
                        elif BB_df['BB Middle'][BB_i] < BB_df['close'][BB_i] and BB_Holding == 1:  # 超過中軸賣出
                            BB_df['Sell'][BB_i] = BB_Sell_Price = BB_df['mid'][BB_i]
                            BB_Holding = 0
                            BB_Portfolio *= BB_Sell_Price / BB_Buy_Price
                            BBHoldPeriod = BB_i - BB_Buy_Day
                            BB_Count += 1
                            BB_df['Perf'] = BB_Portfolio
                            if BB_Sell_Price > BB_Buy_Price:
                                BB_Count_S += 1
                        else:
                            BB_df['Perf'] = BB_Portfolio

                    BB_Return.append(((BB_Portfolio / 100) - 1) * 100)
                    if len(BB_df) > 61:
                        BB_Return60.append(((BB_df['Perf'][len(BB_df) - 1] / BB_df['Perf'][len(BB_df) - 61]) - 1) * 100)
                    else:
                        BB_Return60.append('N/A')
                    BB_Rate.append(Hit_Rate(BB_Count_S, BB_Count))
                    BB_Trade.append(BB_Count)
                    BB_Holding_Period.append(Hit_Rate(BBHoldPeriod, BB_Count))
                    BB_Time_Period.append(BB_TPeriod)
                    BB_Upper.append(BB_Up)
                    BB_Lower.append(BB_Low)

        BB_Report = pd.DataFrame({'BB Return': BB_Return, 'BB Return60': BB_Return60, 'BB Hit Rate': BB_Rate, 'BB Trade Time': BB_Trade, 'BB Holding Period': BB_Holding_Period,
                                  'BB Time period': BB_Time_Period, 'BB Upper': BB_Upper, 'BB Lower': BB_Lower})
        BB_Report.to_csv('Database/' + stock_i + '/BB Summary.csv')

    except:
        print(stock_i, 'BB Error')

    try:
        print('MACD Beta ' + stock_i + ' starting now...')
        MACD_df = data_request(connection, stock_i_)
        MACD_Return, MACD_Return60, MACD_Rate, MACD_Trade, MACD_HoldingPeriod, MACD_Fast, MACD_Slow, MACD_Signal, methodBuy, methodSell = [], [], [], [], [], [], [], [], [], []

        for MACD_F in range(9, 16):

            for MACD_S in range(23, 30):

                for MACD_Sig in range(6, 12):
                    MACD_df['MACD'], MACD_df['MACD signal'], MACD_df['MACD hist'] = ta.MACD(MACD_df['close'], fastperiod=MACD_F, slowperiod=MACD_S, signalperiod=MACD_Sig)

                    for buy_i in ['negativeToPositive', 'nearToZero', 'beyondSignal']:

                        for sell_i in ['histBelowPreHist', 'histLevel', 'beyondMACD']:
                            MACD_BuyPrice, MACD_SellPrice, MACD_BuyDay, MACD_Portfolio = 0, 0, 0, 100
                            MACD_Holding, MACD_Count, MACD_CountS, MACD_HoldPeriod = 0, 0, 0, 0
                            MACD_df['Perf'] = None

                            for MACD_i in range(6, len(MACD_df)):
                                if MACD_Holding == 0 and MACD_BuyDecision(buy_i, MACD_df, MACD_i) is True:
                                    MACD_Holding = 1
                                    MACD_BuyPrice = MACD_df['mid'][MACD_i]
                                    MACD_BuyDay = MACD_i
                                elif MACD_Holding == 1 and MACD_SellDecision(sell_i, MACD_df, MACD_i) is True:
                                    MACD_Holding = 0
                                    MACD_HoldPeriod += MACD_i - MACD_BuyDay
                                    MACD_SellPrice = MACD_df['mid'][MACD_i]
                                    MACD_Portfolio *= MACD_SellPrice / MACD_BuyPrice
                                    if MACD_SellPrice > MACD_BuyPrice:
                                        MACD_CountS += 1
                                MACD_df['Perf'][MACD_i] = MACD_Portfolio

                            MACD_Return.append(MACD_Portfolio)
                            if len(MACD_df) > 61:
                                MACD_Return60.append(((MACD_df['Perf'][len(MACD_df) - 1] / MACD_df['Perf'][len(MACD_df) - 61]) - 1) * 100)
                            MACD_Rate.append(Hit_Rate(MACD_CountS, MACD_Count))
                            MACD_Trade.append(MACD_Count)
                            MACD_HoldingPeriod.append(Hit_Rate(MACD_HoldPeriod, MACD_Count))
                            MACD_Fast.append(MACD_F)
                            MACD_Slow.append(MACD_S)
                            MACD_Signal.append(MACD_Sig)
                            methodBuy.append(buy_i)
                            methodSell.append(sell_i)
                            #MACD_df.to_csv('Analysis/MACD F%s S%s Sig%s buy%s sell%s.csv' %(MACD_F, MACD_S, MACD_Sig, buy_i, sell_i))

                MACD_Report = pd.DataFrame({'MACD Return': MACD_Return,
                                            'MACD Return60': MACD_Return60,
                                            'MACD Hit Rate': MACD_Rate,
                                            'MACD Trade Time': MACD_Trade,
                                            'MACD Fast': MACD_Fast,
                                            'MACD Slow': MACD_Slow,
                                            'MACD Signal': MACD_Signal,
                                            'MACD Buy method': methodBuy,
                                            'MACD Sell method': methodSell,
                })
                MACD_Report.to_csv('Database/' + stock_i + '/MACD Summary.csv')

    except:
        print(stock_i, ' MACD Error')

    try:
        print('MFI_Beta ' + stock_i + ' starting now...')
        MFI_df = data_request(connection, stock_i_)
        MFI_Return, MFI_Return60, MFI_Rate, MFI_Trade, MFI_Holding_Period, MFI_Time_Period, MFI_Buy_Level, MFI_Sell_Level = [], [], [], [], [], [], [], []

        for MFI_TPeriod in range(14, 15):

            for MFI_Buy_Condition in range(20, 41, 2):

                for MFI_Sell_Condition in range(60, 81, 2):
                    MFI_df['MFI'] = ta.MFI(MFI_df['high'], MFI_df['low'], MFI_df['close'], MFI_df['volume'], timeperiod=MFI_TPeriod)
                    MFI_df['Buy'] = None
                    MFI_df['Sell'] = None
                    MFI_df['Perf'] = None
                    MFI_Holding, MFI_Portfolio, MFI_Count, MFI_Count_S, MFIHoldPeriod = 0, 100, 0, 0, 0

                    for MFI_i in range(1, len(MFI_df)):

                        if MFI_Holding == 0 and MFI_df['MFI'][MFI_i] <= MFI_Buy_Condition:
                            MFI_Holding = 1
                            MFI_df['Buy'][MFI_i] = MFI_Buy_Price = MFI_df['mid'][MFI_i]
                            MFI_df['Perf'][MFI_i] = MFI_Portfolio
                            MFI_Buy_Day = MFI_i

                        elif MFI_Holding == 1 and MFI_df['MFI'][MFI_i] >= MFI_Sell_Condition:
                            MFI_Holding = 0
                            MFI_df['Sell'][MFI_i] = MFI_Sell_Price = MFI_df['mid'][MFI_i]
                            MFI_Portfolio *= MFI_Sell_Price / MFI_Buy_Price
                            MFI_df['Perf'][MFI_i] = MFI_Portfolio
                            MFIHoldPeriod += MFI_i - MFI_Buy_Day
                            MFI_Count += 1

                            if MFI_Sell_Price > MFI_Buy_Price:
                                MFI_Count_S += 1

                        else:
                            MFI_df['Perf'][MFI_i] = MFI_Portfolio

                    MFI_Return.append(((MFI_Portfolio / 100) - 1) * 100)

                    if len(MFI_df) > 61:
                        MFI_Return60.append(((MFI_df['Perf'][len(MFI_df) - 1] / MFI_df['Perf'][len(MFI_df) - 61]) - 1) * 100)
                    else:
                        MFI_Return60.append('N/A')

                    MFI_Rate.append(Hit_Rate(MFI_Count_S, MFI_Count))
                    MFI_Trade.append(MFI_Count)
                    MFI_Holding_Period.append(Hit_Rate(MFIHoldPeriod, MFI_Count))
                    MFI_Time_Period.append(MFI_TPeriod)
                    MFI_Buy_Level.append(MFI_Buy_Condition)
                    MFI_Sell_Level.append(MFI_Sell_Condition)

        MFI_Report = pd.DataFrame(
            {
            'MFI Return': MFI_Return,
            'MFI Return60': MFI_Return60,
            'MFI Hit Rate': MFI_Rate,
            'MFI Trade Time': MFI_Trade,
            'MFI Holding Period': MFI_Holding_Period,
            'MFI Time Period': MFI_Time_Period,
            'MFI Buy Condition': MFI_Buy_Level,
           'MFI Sell Condition': MFI_Sell_Level
            }
        )
        MFI_Report.to_csv('Database/' + stock_i + '/MFI Summary.csv')

    except:
        print(stock_i, ' MFI Error')'''

def doubleTop(stock_i, screenRange = 6):
    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
    df = df.drop(['Unnamed: 0'], axis = 1)
    df['firstTop'] = None
    firstTopLoc, T1, T2, T3, T4, T5, T6 = [], [], [], [], [], [], []

    for i in range(9, len(df)-9):   #確定第一次頂部
        secondTop, frontTail, backTail, frontLow = 0, 0, 0, 0
        frontLow = min(df['high'][i - screenRange:i])
        backLow = min(df['high'][i:i + screenRange])
        for j in range(1, screenRange):
            if df['high'][i] > df['high'][i-j]:
                frontTail = 1
            else:
                frontTail = 0
                break
        if frontTail == 1:
            for k in range(1, 6):
                if df['high'][i] > df['high'][i+k]:
                    backTail = 1
                else:
                    backTail = 0
                    break
            if backTail == 1:
                df['firstTop'][i] = 'T'
                firstTopLoc.append(i)

def supportLevel(stock_i):
    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
    df = df.drop(['Unnamed: 0'], axis = 1)

def bands(stock_i):
    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
    df = df.drop(['Unnamed: 0'], axis = 1)
    df['top len'] = None
    df['low len'] = None

    for i in range(len(df) - 1):
        bandsTopLen, bandsLowLen = 0, 0
        j = 1
        while df['high'][i + j] <= df['high'][i] and i + j < len(df) - 1:
            bandsTopLen += 1
            j += 1
        k = 1
        while df['low'][i + k] >= df['low'][i] and i + k < len(df) - 1:
            bandsLowLen += 1
            k += 1
        df['top len'][i] = bandsTopLen
        df['low len'][i] = bandsLowLen

    print(df)


def trydown(stock_i):
    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
    df = df.drop(['Unnamed: 0'], axis = 1)
    df['sup'] = None
    holding, buyDate = 0, 0
    T0, T1, T2, TA, TB = 0, 0, 0, 0, 0

    for i in range(30, len(df)):
        support = min(df['low'][i - 10:i])
        supportS = 0
        for j in range(10):
            if df['low'][i - j] * 0.995 <= df['low'][i] <= df['low'][i - j] * 1.005:
                supportS += 1
        df['sup'][i] = supportS

    print(df)

def dayrange(stock_i):
    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '_min.csv')
    df = df.drop(['Unnamed: 0'], axis = 1)
    df = df.set_index('time_key')
    df['check'] = None
    df['check1'] = None
    df['check2'] = None
    df['trend'] = None
    df['action'] = None
    holding, buyP, sellP, investCount, investCountS, prof = 0, 0, 0, 0, 0, 100
    j = 0
    k = 1

    for i in range(5):
        dayOpenTime = df.index[j]
        dayEndTime = df.index[k]
        while df.index[j][:10] == df.index[k][:10]:
            k += 1
        #dfthatday = df[df.index[j]:df.index[k - 1]]

        m = j + 30
        while m < k - 30:
            

            m += 1
        print(df[j:k])
        print(investCount, investCountS, prof)

        j = k + 1

def model3pre(stock_i):
    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv', index_col=0)
    df.drop(['pe_ratio', 'turnover_rate'], axis=1, inplace=True)
    df['date'] = df['date'].apply(lambda a: a[:10])
    df.reset_index(inplace=True, drop=True)

    dfM = pd.read_csv('Database/' + stock_i + '/' + stock_i + '_min.csv', index_col=0)
    dfM.drop(['pe_ratio', 'turnover_rate'], axis=1, inplace=True)
    dfM['mid'] = (dfM['high'] + dfM['low']) / 2
    dfM.reset_index(inplace=True, drop=True)

    dfM3Path = 'Database/' + stock_i + '/' + stock_i + '_M3.csv'
    if not os.path.exists(dfM3Path):
        model3_Data(stock_i, df, dfM)
    else:
        dfM3 = pd.read_csv(dfM3Path, index_col=0)
        dfM3.reset_index(inplace=True, drop=True)
        if dfM3['time_key'][len(dfM3)-1] == dfM['time_key'][len(dfM)-1]:
            pass
        else: model3_Data(stock_i, df, dfM, dfM3)


def model3(df, dfM): #以成交量計算資金流入/出
    df['flow'] = None

    dfM['change'] = dfM['mid'].diff().round(2)
    dfM['change'].fillna(0, inplace=True)
    dfM['flow'] = dfM['change'] * dfM['turnover'] / 1000
    dfM['flow'].fillna(0, inplace=True)
    dfM['mid'] = (dfM['high'] + dfM['low']) / 2

    firstDate = dfM['time_key'][0][:10]
    firstDateLoc = df.index[df['date'] == firstDate].tolist()

    ii = firstDateLoc[0]
    for i in df['date'][ii:len(df) + 1]:
        flow = 0
        for j in range(len(dfM)):
            if dfM['time_key'][j][:10] == i:
                flow += dfM['flow'][j]
        df['flow'][ii] = flow
        ii += 1

    df.to_csv('test.csv')
    dfM.to_csv('test_v.csv')

    return df

def model3_Data(stock_i, df, dfM, dfM3 = None): #分出高/低於界線的成交量
    if dfM3 is None:
        print(stock_i + ' start m3 (new)')
        firstD = df['date'][0]
        firstM = dfM['time_key'][0][:10]

        if firstD > firstM:
            dfM.drop(dfM[dfM['time_key'] < firstD].index, inplace=True)
            dfM.reset_index(drop=True, inplace=True)
        elif firstD < firstM:
            df.drop(df[df['date'] < firstM].index, inplace=True)
            df.reset_index(drop=True, inplace=True)

        dfM['pre_close'] = 0
        firstDate = dfM['time_key'][0][:10]
        firstDateLoc = df.index[df['date'] == firstDate].tolist()

        ii = firstDateLoc[0] #找出每分鐘資料表的第一個有數據的日子, 配對到每天資料表

        dfM_point = pd.DataFrame(columns={'start_point', 'end_point'}, index=df['date'][ii:len(df) + 1])

        for i in df['date'][ii:len(df) + 1]:
            stPoint, endPoint = 0, 0
            j = 0
            while j < len(dfM): #找出每天第一及最後一筆資料
                if dfM['time_key'][j][:10] == i:
                    stPoint = j
                    while j < len(dfM) and dfM['time_key'][j][:10] == i:
                        endPoint = j
                        j += 1
                    else:
                        dfM_point['start_point'][i] = stPoint
                        dfM_point['end_point'][i] = endPoint
                j += 1

        for k in range(1, len(dfM_point)):
            st_Loc = dfM_point['start_point'][k]
            end_Loc = dfM_point['end_point'][k]
            #print(k, st_Loc)
            pre_Close = dfM['close'][st_Loc - 1]
            dfM['pre_close'][st_Loc:end_Loc + 1] = pre_Close

        print(stock_i + 'model3 data completed')
        dfM.to_csv('Database/' + stock_i + '/' + stock_i + '_M3.csv')
    elif dfM3.empty == False:
        print(stock_i + ' start m3 (add)')

        lastM = dfM3['time_key'][len(dfM3) - 1]
        firstData = dfM3['close'][len(dfM3) - 1]

        df.drop(df[df['date'] <= lastM].index, inplace=True)
        df.set_index('date', drop=True, inplace=True)

        dfM.drop(dfM[dfM['time_key'] <= lastM].index, inplace=True)
        dfM.reset_index(drop=True, inplace=True)
        dfM['pre_close'] = 0

        dateList = df.index
        dfM.pre_close[dfM.time_key.str.contains(dateList[0]) == True] = firstData
        for i in range(len(dateList)):
            day_i = dateList[i]
            closeP = df['close'][day_i]
            try:
                iNext = dateList[i+1]
                dfM.pre_close[dfM.time_key.str.contains(iNext) == True] = closeP
            except: pass

        dfM3 = pd.concat([dfM3, dfM])

        print(stock_i + 'model3 data completed')
        dfM3.to_csv('Database/' + stock_i + '/' + stock_i + '_M3.csv')



def model3EX(dfD, dfM):  #之後加入近期最高, 最低位為支持阻力
    dfM.reset_index(drop=True, inplace=True)
    dfM.drop(dfM[dfM['pre_close'] == 0].index, inplace=True)
    dfD.reset_index(inplace=True, drop=True)

    dfM = model_target_in_out_flow(dfM, 100)

    dfM['date'] = dfM.apply(lambda x: x['time_key'][:10], axis=1)
    dfM.set_index('date', inplace=True, drop=True)

    dfD.drop(dfD[dfD['date'] < dfM.index[0]].index, inplace=True)
    dfD['date'] = dfD.apply(lambda x: x['date'][:10], axis=1)
    dfD.set_index('date', inplace=True, drop=True)

    cashFlowLevel = 0
    sRate, fRate, tRate = 0, 0, 0
    dfD['flow'] = None
    dfD['action'] = None
    dfD['remark'] = None
    dfD['N1'] = dfD['mid'].shift(-1)
    dfD['N2'] = dfD['mid'].shift(-2)
    dfD['N3'] = dfD['mid'].shift(-3)
    dfD['N4'] = dfD['mid'].shift(-4)
    dfD['N5'] = dfD['mid'].shift(-5)

    for i in range(1, len(dfD.index)):
        dfD['flow'][dfD.index[i]] = sum(dfM['flow'][dfD.index[i]])

    for j in range(1, len(dfD.index) - 5):
        if dfD['flow'][dfD.index[j]] > cashFlowLevel:
            dfD['action'][dfD.index[j]] = 'Buy'
            tRate += 1
            comPrice = dfD.loc[dfD.index[j]]
            comPrice = comPrice[['N1', 'N2', 'N3', 'N4', 'N5']].mean()
            if dfD['mid'][dfD.index[j]] < dfD['N1'][dfD.index[j]]:
                sRate += 1
                dfD['remark'][dfD.index[j]] = 'S'
            elif dfD['mid'][dfD.index[j]] > dfD['N1'][dfD.index[j]]:
                fRate += 1
                dfD['remark'][dfD.index[j]] = 'F'
    print(sRate, fRate, tRate)

    return dfD, [sRate, fRate, tRate]



def model3V(dfD, dfM):   #model3 cash flow的延伸, 以投票方式決定當日支持阻力
    dfM.reset_index(drop=True, inplace=True)
    dfM.drop(dfM[dfM['pre_close'] == 0].index, inplace=True)

    dfD.reset_index(inplace=True, drop=True)
    dfD['max_inflow_price'] = None
    dfD['max_inflow_volume'] = None
    dfD['total_inflow_volume'] = None
    dfD['max_outflow_price'] = None
    dfD['max_outflow_volume'] = None
    dfD['total_outflow_volume'] = None
    dfD['inflow_vote'] = None
    dfD['outflow_vote'] = None
    dfD['flow_vote_ratio'] = None

    dfM = model_target_in_out_flow(dfM, 100)

    dfM['date'] = dfM.apply(lambda x: x['time_key'][:10], axis=1)
    dfM.set_index('date', inplace=True, drop=True)

    dfD.drop(dfD[dfD['date'] < dfM.index[0]].index, inplace=True)
    dfD['date'] = dfD.apply(lambda x: x['date'][:10], axis=1)
    dfD.set_index('date', inplace=True, drop=True)

    dfM['close'] = dfM['close'].round(2)
    dfD = dfD.round(2)
    for i in range(1, len(dfD.index)):
        dfCD = dfM.loc[dfD.index[i]]
        priceList = sorted(set(dfCD['close']))
        dfC = pd.DataFrame(index = priceList, columns = {'flow', 'remark'})
        dfC['remark'] = ' '

        maxV, minV = None, None
        maxLoc, minLoc = float(), float()
        for j in dfC.index:
            dfCD1 = dfCD[dfCD['close'] == j]
            dfC['flow'][j] = dfCD1['flow'].sum()

            if maxV is None:
                maxV = dfC['flow'][j]
                maxLoc = j
            elif dfC['flow'][j] > maxV:
                maxV = dfC['flow'][j]
                maxLoc = j

            if minV is None:
                minV = dfC['flow'][j]
                minLoc = j
            elif dfC['flow'][j] < minV:
                minV = dfC['flow'][j]
                minLoc = j


        dfC['remark'][maxLoc] = 'Max'
        dfC['remark'][minLoc] = 'Min'

        dfD['max_inflow_price'][dfD.index[i]] = maxLoc
        dfD['max_inflow_volume'][dfD.index[i]] = dfC['flow'][maxLoc]
        dfD['total_inflow_volume'][dfD.index[i]] = dfC[dfC['flow'] > 0].sum()['flow']
        dfD['max_outflow_price'][dfD.index[i]] = minLoc
        dfD['max_outflow_volume'][dfD.index[i]] = dfC['flow'][minLoc]
        dfD['total_outflow_volume'][dfD.index[i]] = dfC[dfC['flow'] < 0].sum()['flow']

        po = sum(dfC.flow > 0)
        ne = sum(dfC.flow < 0)
        pnRatio = round((po / (po + ne)) * 100, 2)
        dfD['inflow_vote'][dfD.index[i]] = po
        dfD['outflow_vote'][dfD.index[i]] = ne
        dfD['flow_vote_ratio'][dfD.index[i]] = pnRatio

        '''
        po = sum(dfC.flow > 0)
        ne = sum(dfC.flow < 0)
        print(po, ne, po / (po + ne))
        dif0 = round(dfD['open'][i+1] - dfD['open'][i], 2)
        dif1 = round(dfD['mid'][i+1] - dfD['mid'][i], 2)
        dif2 = round(dfD['close'][i+1] - dfD['close'][i], 2)
        print(dif0, dif1, dif2)
        print('\n')'''
        #dfC.to_csv('Analysis/' + str(dfD.index[i]) + '.csv')

    #dfD.to_csv('Analysis/M3V.csv')
    return dfD

def model3V_RT(dfM):   #3V簡化
    dfM.reset_index(drop=True, inplace=True)
    #dfM.drop(dfM[dfM['last_close'] == 0].index, inplace=True)

    dfM = model_target_in_out_flow(dfM, 100)

    dfM['date'] = dfM.apply(lambda x: x['time_key'][:10], axis=1)
    dfM.set_index('date', inplace=True, drop=True)

    priceList = sorted(set(dfM['close']))
    dfC = pd.DataFrame(index=priceList, columns={'flow', 'remark'})

    maxV, minV = None, None
    maxLoc, minLoc = float(), float()
    for j in dfC.index:
        dfCD1 = dfM[dfM['close'] == j]
        dfC['flow'][j] = dfCD1['flow'].sum()

        if maxV is None:
            maxV = dfC['flow'][j]
            maxLoc = j
        elif dfC['flow'][j] > maxV:
            maxV = dfC['flow'][j]
            maxLoc = j

        if minV is None:
            minV = dfC['flow'][j]
            minLoc = j
        elif dfC['flow'][j] < minV:
            minV = dfC['flow'][j]
            minLoc = j

    dfC['remark'][maxLoc] = 'Max'
    dfC['remark'][minLoc] = 'Min'
    po = sum(dfC.flow > 0)
    ne = sum(dfC.flow < 0)
    pnRatio = round((po / (po + ne)) * 100, 2)

    print(dfC)
    #print(pnRatio)
    return pnRatio


def model_target_fu_avg_price(df, ro):
    df['FP_avg'] = df.mid.values[::-1]
    df['FP_avg'] = df['FP_avg'].rolling(ro).mean()  # 未來平均價格
    df['FP_avg'] = df.FP_avg.values[::-1]

    return df



def model_target_in_out_flow(df, ro, comPrice = 'mid', stLine = 'pre_close_min'):
    df['pre_close_min'] = df['close'].shift(1)
    df['flow'] = df.apply(lambda x: x['turnover'] if x[comPrice] > x[stLine] else x['turnover'] * -1, axis=1)
    df['flow_avg'] = df['flow'].rolling(ro).mean()

    return df



def model3EX_C1(df):
    for j in range(10, 200, 10):

        df['FP'] = df.mid.values[::-1]
        df['FP'] = df['FP'].rolling(200).mean()      #未來平均價格
        df['FP'] = df.FP.values[::-1]




def model3ex2(df, roll = 5):
    df['chg'] = df['mid'].diff().round(2)
    df['flow'] = df.apply(lambda x: x['turnover'] if x['chg'] > 0 else x['turnover'] * -1 if x['chg'] < 0 else 0, axis=1)
    df.drop(range(331), inplace=True)
    df.reset_index(inplace=True, drop=True)
    df['flow'] = df['flow'] / 1000000

    #for i in range(roll, len(df)): #如果價格不變, 則向前滾動睇下係咪處於升勢

    return df



def model6():
    pass


def model_testing(symbol):
    pass


def model3_2_4(df, P_level = None):   # P_level應是現價
    #print(df[df.ticker_direction == 'NEUTRAL']['turnover'].sum())
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    df.reset_index(inplace=True, drop=True)

    if isinstance(df['time'][0], datetime):
        pass
    else: df['time'] = pd.to_datetime(df['time'])

    if P_level is None:
        import statistics
        list_P = df.drop_duplicates(subset=['price'], keep='first')['price']
        P_level = statistics.median(list_P)
        del statistics
    elif P_level == 'last':
        P_level = df['price'][len(df) - 1]   # 現價

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
    Q_result = {'Positive': round(quadrant0 + quadrant1, 2), 'Negative': round(quadrant2 + quadrant3, 2)}

    return Q_result



if __name__ == '__main__':
    df = pd.read_csv('HK_00005_2022_09_01.csv', index_col=0)
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    a = df.groupby('price')['turnover'].sum()

    df1 = pd.read_csv('HK_00005_2022_09_02.csv', index_col=0)
    df1.drop(df1[df1['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    b = df1.groupby('price')['turnover'].sum()

    print(len(a))
    print(a)
    print(df['volume'].mean())
    print(a.nlargest(1))
    '''
    from timejob import gmail_create_draft
    gmail_create_draft('alphax.lys@gmail.com', 'collection', a)'''

















