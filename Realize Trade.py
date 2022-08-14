import datetime as dt
import matplotlib.pyplot as plt
import pandas as pd
import pandas_datareader.data as web
import numpy as np
import plotly.graph_objects as go
import talib as ta
import mplfinance as mpf
pd.set_option('display.max_columns', 30)       #pandas setting 顥示列數上限
pd.set_option('display.width', 1000)           #pandas setting 顯示列的闊度
#pd.set_option('display.max_colwidth',20)      #pandas setting 每個數據顥示上限
pd.set_option('display.max_rows', 2000)
pd.options.mode.chained_assignment = None
from matplotlib.pyplot import MultipleLocator
from line_profiler import LineProfiler
from memory_profiler import profile


def combineAnalysis(stock_i):
    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
    ps = pd.read_csv('Database/' + stock_i + '/MFI Summary.csv')

    TA_Row_H = ps['MFI Hit Rate'].idxmax()
    methCon0 = ps['MFI Time Period'][TA_Row_H]
    methCon1 = ps['MFI Buy Condition'][TA_Row_H]
    methCon2 = ps['MFI Sell Condition'][TA_Row_H]

    df['MFI'] = ta.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod = methCon0)
    holding, buyPrice, buyDate = 0, 0, 0
    #df['buyMFI'] = None
    #df['sellMFI'] = None
    df['resultMFI'] = None

    for i in range(len(df)):
        if df['MFI'][i] <= methCon1 and holding == 0:
            holding = 1
            buyDate = i
            buyPrice = df['mid'][i]
        elif df['MFI'][i] >= methCon2 and holding == 1:
            holding = 0
            sellPrice = df['mid'][i]
            if sellPrice > buyPrice:
                df['resultMFI'][buyDate : i + 1] = 'Win'
            elif sellPrice <= buyPrice:
                df['resultMFI'][buyDate : i + 1] = 'Lose'

    ps = pd.read_csv('Database/' + stock_i + '/SAR Summary.csv')
    TA_Row_H = ps['SAR Hit Rate'].idxmax()
    methCon0 = ps['SAR Acceleration'][TA_Row_H]
    methCon1 = ps['SAR Max Value'][TA_Row_H]
    methCon2 = ps['SAR Postpone'][TA_Row_H]

    df['SAR'] = ta.SAR(df['high'], df['low'], acceleration=methCon0, maximum=methCon1)
    holding, buyPrice, buyDate = 0, 0, 0
    #df['buySAR'] = None
    #df['sellSAR'] = None
    df['resultSAR'] = None
    for j in range(len(df)):
        if holding == 0 and df['SAR'][j] < df['close'][j] and df['SAR'][j + methCon2] < df['close'][j + methCon2]:
            holding = 1
            buyDate = j
            buyPrice = df['mid'][j]
        elif holding == 1 and df['SAR'][j] > df['close'][j]:
            holding = 0
            sellPrice = df['mid'][j]
            if sellPrice > buyPrice:
                df['resultSAR'][buyDate : j + 1] = 'Win'
            elif sellPrice <= buyPrice:
                df['resultSAR'][buyDate: j + 1] = 'Lose'

    ps = pd.read_csv('Database/' + stock_i + '/RSI Summary.csv')
    TA_Row_H = ps['RSI Hit Rate'].idxmax()
    methCon0 = ps['RSI Time Period'][TA_Row_H]
    methCon1 = ps['RSI Buy Condition'][TA_Row_H]
    methCon2 = ps['RSI Sell Condition'][TA_Row_H]
    methCon3 = ps['RSI Method'][TA_Row_H]

    df['RSI'] = ta.RSI(df['close'], timeperiod=methCon0)
    holding, buyPrice, buyDate = 0, 0, 0
    df['resultRSI'] = None
    if methCon3 == 'Negative':
        for k in range(len(df)):
            if holding == 0 and df['RSI'][k] < methCon1:
                holding = 1
                buyDate = k
                buyPrice = df['mid'][k]
            elif holding == 1 and df['RSI'][k] > methCon2:
                holding = 0
                sellPrice = df['mid'][k]
                if sellPrice > buyPrice:
                    df['resultRSI'][buyDate: k + 1] = 'Win'
                elif sellPrice <= buyPrice:
                    df['resultRSI'][buyDate: k + 1] = 'Lose'
    elif methCon3 == 'Positive':
        for k in range(len(df)):
            if holding == 0 and df['RSI'][k] > methCon1:
                holding = 1
                buyDate = k
                buyPrice = df['mid'][k]
            elif holding == 1 and df['RSI'][k] < methCon2:
                holding = 0
                sellPrice = df['mid'][k]
                if sellPrice > buyPrice:
                    df['resultRSI'][buyDate: k + 1] = 'Win'
                elif sellPrice <= buyPrice:
                    df['resultRSI'][buyDate: k + 1] = 'Lose'

    ps = pd.read_csv('Database/' + stock_i + '/BB Summary.csv')
    TA_Row_H = ps['BB Hit Rate'].idxmax()
    methCon0 = ps['BB Time Period'][TA_Row_H]
    methCon1 = ps['BB Upper'][TA_Row_H]
    methCon2 = ps['BB Lower'][TA_Row_H]
    methCon3 = ps['BB Method'][TA_Row_H]

    df['BB Upper'], df['BB Middle'], df['BB Lower'] = ta.BBANDS(df['close'], methCon0, methCon1, methCon2)
    holding, buyPrice, buyDate = 0, 0, 0
    #df['buySAR'] = None
    #df['sellSAR'] = None
    df['resultBB'] = None
    if methCon3 == 'Bottom To Middle':
        for l in range(len(df)):
            if holding == 0 and df:
                holding = 1
                buyDate = l
                buyPrice = df['mid'][l]
    elif methCon3 == 'Middle To Top':
        for l in range(len(df)):
            if holding == 0 and df:
                holding = 1
                buyDate = l
                buyPrice = df['mid'][l]
    elif methCon3 == 'Bottom To Top':
        for l in range(len(df)):
            if holding == 0 and df:
                holding = 1
                buyDate = l
                buyPrice = df['mid'][l]


    df.to_csv('Analysis/' + stock_i + '.csv')

def rel(stock_i):
    df = pd.read_csv('Analysis/' + stock_i + '.csv')
    df = df.drop(['Unnamed: 0'],
                 axis = 1)

    df['result Base'] = df['result Base'].apply(lambda x: df['high'].max() if x == 'Win' else (df['high'].max() + 0.1 if x == 'Lose' else None))
    #df['resultSAR'] = df['resultSAR'].apply(lambda x: df['high'].max() if x == 'Win' else (df['high'].max() + 0.1 if x == 'Lose' else None))

    df['date'] = df['date'].apply(lambda x: x[:10])
    axisX = df['date']
    stockP = df['close']

    #plt.subplot(211)
    plt.bar(axisX,
            df['result Base'],
            color = ['blue' if k == df['high'].max() else 'red' for k in df['result Base']],
            alpha = 0.3,
            width = 1)
    plt.plot(axisX,
             stockP)
    ax=plt.gca()
    ax.xaxis.set_major_locator(MultipleLocator(100))
    '''
    plt.subplot(212)
    plt.bar(axisX,
            df['resultSAR'],
            color = ['blue' if k == df['high'].max() else 'red' for k in df['result Base']],
            alpha = 0.3,
            width = 1)
    plt.plot(axisX,
             stockP)
    ax=plt.gca()
    ax.xaxis.set_major_locator(MultipleLocator(100))
    '''
    plt.show()


if __name__ == '__main__':
    stock_i = 'HK.00027'
    rel(stock_i)

