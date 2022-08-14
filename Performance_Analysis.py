import numpy, datetime, Techanalysis
import pandas as pd 
import talib as ta
from pandas import DataFrame
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 3000)
from memory_profiler import profile

@profile
def bestAnalysis(stock_i):
    techMethod = ['BB', 'MACD', 'SAR', 'RSI', 'MFI']

    dfBB = pd.read_csv('Database/' + stock_i + '/BB Summary.csv')
    dfBB['Tech Method'] = 'BB'
    dfBB = dfBB.rename(columns = {'BB Return': 'Return',
                                  'BB Return60': 'Return60',
                                  'BB Hit Rate': 'Hit Rate',
                                  'BB Trade Time': 'Trade Time',
                                  'BB Holding Period': 'Holding Period',
                                  'BB Time period': 'Method Condition0',
                                  'BB Upper': 'Method Condition1',
                                  'BB Lower': 'Method Condition2',
                                  'BB Method': 'Method Condition3'
                                  }
                       )

    dfMACD = pd.read_csv('Database/' + stock_i + '/MACD Summary.csv')
    dfMACD['Tech Method'] = 'MACD'
    dfMACD = dfMACD.rename(columns = {'MACD Return': 'Return',
                                      'MACD Return60': 'Return60',
                                      'MACD Hit Rate': 'Hit Rate',
                                      'MACD Trade Time': 'Trade Time',
                                      'MACD Holding Period': 'Holding Period',
                                      'MACD Fast': 'Method Condition0',
                                      'MACD Slow': 'Method Condition1',
                                      'MACD Signal': 'Method Condition2',
                                      'MACD Buy method': 'Method Condition3',
                                      'MACD Sell method': 'Method Condition4'
                                      }
                           )

    dfMFI = pd.read_csv('Database/' + stock_i + '/MFI Summary.csv')
    dfMFI['Tech Method'] = 'MFI'
    dfMFI = dfMFI.rename(columns = {'MFI Return': 'Return',
                                    'MFI Return60': 'Return60',
                                    'MFI Hit Rate': 'Hit Rate',
                                    'MFI Trade Time': 'Trade Time',
                                    'MFI Holding Period': 'Holding Period',
                                    'MFI Time Period': 'Method Condition0',
                                    'MFI Buy Condition': 'Method Condition1',
                                    'MFI Sell Condition': 'Method Condition2'
                                    }
                         )

    dfRSI = pd.read_csv('Database/' + stock_i + '/RSI Summary.csv')
    dfRSI['Tech Method'] = 'RSI'
    dfRSI = dfRSI.rename(columns = {'RSI Return': 'Return',
                                    'RSI Return60': 'Return60',
                                    'RSI Hit Rate': 'Hit Rate',
                                    'RSI Trade Time': 'Trade Time',
                                    'RSI Holding Period': 'Holding Period',
                                    'RSI Time Period': 'Method Condition0',
                                    'RSI Buy Condition': 'Method Condition1',
                                    'RSI Sell Condition': 'Method Condition2',
                                    'RSI Method': 'Method Condition3'
                                    }
                         )

    dfSAR = pd.read_csv('Database/' + stock_i + '/SAR Summary.csv')
    dfSAR['Tech Method'] = 'SAR'
    dfSAR = dfSAR.rename(columns = {'SAR Return': 'Return',
                                    'SAR Return60': 'Return60',
                                    'SAR Hit Rate': 'Hit Rate',
                                    'SAR Trade Time': 'Trade Time',
                                    'SAR Holding Period': 'Holding Period',
                                    'SAR Acceleration': 'Method Condition0',
                                    'SAR Max Value': 'Method Condition1',
                                    'SAR Postpone': 'Method Condition2'
                                    }
                         )

    dfSum = pd.concat([dfBB, dfMACD, dfMFI, dfRSI, dfSAR])
    dfSum = dfSum.reset_index()
    dfSum = dfSum.drop(['Unnamed: 0', 'index'], axis = 1)
    dfSum = dfSum.sort_values(['Hit Rate'], ascending=False)
    dfSum['discrete rate'] = None
    topMethodSort = dfSum.index

    dfBase = Techanalysis.bestAnalysisBase(dfSum, stock_i, topMethodSort[0])
    dfBase.to_csv('Analysis/' + stock_i + '.csv')
    discreteHigh, discreteHighLoc = secondLevelAnalysis(dfBase, dfSum, topMethodSort[1:])
    print(dfSum[discreteHighLoc:discreteHighLoc+1])

    dfBase = pd.read_csv('Analysis/' + stock_i + '.csv', index_col=0)
    dfBase, discrete = Techanalysis.tradeBeta(dfBase, dfSum, discreteHighLoc)
    dfBase.to_csv('Analysis/' + stock_i + '.csv')
    print(discrete)

def secondLevelAnalysis(df, dfSum, topLocSort):
    discrete, discreteHigh, discreteHighLoc = 0, 0, 0
    for j in topLocSort[0:100]:
        techIndex = dfSum['Tech Method'][j]
        print(j, techIndex)

        df, discrete = Techanalysis.tradeBeta(df, dfSum, j)
        if discrete > discreteHigh:
            discreteHigh = discrete
            discreteHighLoc = j

    return discreteHigh, discreteHighLoc


def Performing(stock_i):
    print('Performance Analysis of ' + stock_i + ' starting now...')
    techMethod = ['BB', 'MACD', 'SAR', 'RSI', 'MFI']

    dfBB = pd.read_csv('Database/' + stock_i + '/BB Summary.csv')
    dfBB['Tech Method'] = 'BB'
    dfBB = dfBB.rename(columns = {'BB Return': 'Return',
                                  'BB Return60': 'Return60',
                                  'BB Hit Rate': 'Hit Rate',
                                  'BB Trade Time': 'Trade Time',
                                  'BB Holding Period': 'Holding Period',
                                  'BB Time period': 'Method Condition0',
                                  'BB Upper': 'Method Condition1',
                                  'BB Lower': 'Method Condition2',
                                  'BB Method': 'Method Condition3'
                                  }
                       )

    dfMACD = pd.read_csv('Database/' + stock_i + '/MACD Summary.csv')
    dfMACD['Tech Method'] = 'MACD'
    dfMACD = dfMACD.rename(columns = {'MACD Return': 'Return',
                                      'MACD Return60': 'Return60',
                                      'MACD Hit Rate': 'Hit Rate',
                                      'MACD Trade Time': 'Trade Time',
                                      'MACD Holding Period': 'Holding Period',
                                      'MACD Fast': 'Method Condition0',
                                      'MACD Slow': 'Method Condition1',
                                      'MACD Signal': 'Method Condition2',
                                      'MACD Buy method': 'Method Condition3',
                                      'MACD Sell method': 'Method Condition4'
                                      }
                           )

    dfMFI = pd.read_csv('Database/' + stock_i + '/MFI Summary.csv')
    dfMFI['Tech Method'] = 'MFI'
    dfMFI = dfMFI.rename(columns = {'MFI Return': 'Return',
                                    'MFI Return60': 'Return60',
                                    'MFI Hit Rate': 'Hit Rate',
                                    'MFI Trade Time': 'Trade Time',
                                    'MFI Holding Period': 'Holding Period',
                                    'MFI Time Period': 'Method Condition0',
                                    'MFI Buy Condition': 'Method Condition1',
                                    'MFI Sell Condition': 'Method Condition2'
                                    }
                         )

    dfRSI = pd.read_csv('Database/' + stock_i + '/RSI Summary.csv')
    dfRSI['Tech Method'] = 'RSI'
    dfRSI = dfRSI.rename(columns = {'RSI Return': 'Return',
                                    'RSI Return60': 'Return60',
                                    'RSI Hit Rate': 'Hit Rate',
                                    'RSI Trade Time': 'Trade Time',
                                    'RSI Holding Period': 'Holding Period',
                                    'RSI Time Period': 'Method Condition0',
                                    'RSI Buy Condition': 'Method Condition1',
                                    'RSI Sell Condition': 'Method Condition2',
                                    'RSI Method': 'Method Condition3'
                                    }
                         )

    dfSAR = pd.read_csv('Database/' + stock_i + '/SAR Summary.csv')
    dfSAR['Tech Method'] = 'SAR'
    dfSAR = dfSAR.rename(columns = {'SAR Return': 'Return',
                                    'SAR Return60': 'Return60',
                                    'SAR Hit Rate': 'Hit Rate',
                                    'SAR Trade Time': 'Trade Time',
                                    'SAR Holding Period': 'Holding Period',
                                    'SAR Acceleration': 'Method Condition0',
                                    'SAR Max Value': 'Method Condition1',
                                    'SAR Postpone': 'Method Condition2'
                                    }
                         )
    Method_Summary = pd.concat([dfBB, dfMACD, dfMFI, dfRSI, dfSAR], axis=0)

    Method_Summary.to_csv('Database/' + stock_i + '/Method Summary.csv')
    print(stock_i + 'Performance report completed')

    Method_Summary = pd.read_csv('Database/' + stock_i + '/Method Summary.csv')


def SAR_Performing(Fsymbol):

    SAR_Result = pd.read_csv('Database/' + Fsymbol + '/SAR Summary.csv', index_col = 0)
    SAR_Sum = pd.DataFrame(columns = SAR_Result.columns)

    try:
        SAR_Return_Loc = Top_Loc(SAR_Result['SAR Return'])                                     #找出最好的3個的行位置

        for addi in SAR_Return_Loc:
            SAR_Sum = pd.concat([SAR_Sum, SAR_Result.loc[[addi]]], axis = 0)

        SAR_Return60_Loc = Top_Loc(SAR_Result['SAR Return60'])

        for addi in SAR_Return60_Loc:
            SAR_Sum = pd.concat([SAR_Sum, SAR_Result.loc[[addi]]], axis = 0)

        SAR_Rate_Loc = Top_Loc(SAR_Result['SAR Hit Rate'])

        for addi in SAR_Rate_Loc:
            SAR_Sum = pd.concat([SAR_Sum, SAR_Result.loc[[addi]]], axis = 0)

    except:
        print(Fsymbol, ' Error on SAR Best rate stage')

    SAR_Sum['Method'] = 'SAR'

    return SAR_Sum



def RSI_Performing(Fsymbol):

    RSI_Result = pd.read_csv('Database/' + Fsymbol + '/RSI Summary.csv', index_col = 0)
    RSI_Sum = pd.DataFrame(columns = RSI_Result.columns)

    try:
        RSI_Return_Loc = Top_Loc(RSI_Result['RSI Return'])

        for addi in RSI_Return_Loc:
            RSI_Sum = pd.concat([RSI_Sum, RSI_Result.loc[[addi]]], axis = 0)

        SAR_Return60_Loc = Top_Loc(RSI_Result['RSI Return60'])

        for addi in SAR_Return60_Loc:
            RSI_Sum = pd.concat([RSI_Sum, RSI_Result.loc[[addi]]], axis = 0)

        SAR_Rate_Loc = Top_Loc(RSI_Result['RSI Hit Rate'])

        for addi in SAR_Rate_Loc:
            RSI_Sum = pd.concat([RSI_Sum, RSI_Result.loc[[addi]]], axis = 0)

    except:
        print(Fsymbol, ' Error on RSI Best rate stage')

    RSI_Sum['Method'] = 'RSI'

    return RSI_Sum



def MACD_Performing(Fsymbol):

    MACD_Result = pd.read_csv('Database/' + Fsymbol + '/MACD Summary.csv', index_col = 0)
    MACD_Sum = pd.DataFrame(columns = MACD_Result.columns)

    try:
        MACD_Return_Loc = Top_Loc(MACD_Result['MACD Return'])

        for addi in MACD_Return_Loc:
            MACD_Sum = pd.concat([MACD_Sum, MACD_Result.loc[[addi]]], axis = 0)

        MACD_Return60_Loc = Top_Loc(MACD_Result['MACD Return60'])

        for addi in MACD_Return60_Loc:
            MACD_Sum = pd.concat([MACD_Sum, MACD_Result.loc[[addi]]], axis = 0)

        MACD_Rate_Loc = Top_Loc(MACD_Result['MACD Hit Rate'])

        for addi in MACD_Rate_Loc:
            MACD_Sum = pd.concat([MACD_Sum, MACD_Result.loc[[addi]]], axis = 0)

    except:
        print(Fsymbol, ' Error on MACD Best rate stage')

    MACD_Sum['Method'] = 'MACD'

    return MACD_Sum


def BB_Performing(Fsymbol):

    BB_Result = pd.read_csv('Database/' + Fsymbol + '/BB Summary.csv', index_col = 0)
    BB_Sum = pd.DataFrame(columns = BB_Result.columns)

    try:
        BB_Return_Loc = Top_Loc(BB_Result['BB Return'])

        for addi in BB_Return_Loc:
            BB_Sum = pd.concat([BB_Sum, BB_Result.loc[[addi]]], axis = 0)

        BB_Return60_Loc = Top_Loc(BB_Result['BB Return60'])

        for addi in BB_Return60_Loc:
            BB_Sum = pd.concat([BB_Sum, BB_Result.loc[[addi]]], axis = 0)

        BB_Rate_Loc = Top_Loc(BB_Result['BB Hit Rate'])

        for addi in BB_Rate_Loc:
            BB_Sum = pd.concat([BB_Sum, BB_Result.loc[[addi]]], axis = 0)

    except:
        print(Fsymbol, ' Error on BB Best rate stage')

    BB_Sum['Method'] = 'BB'

    return BB_Sum


def MFI_Performing(Fsymbol):

    MFI_Result = pd.read_csv('Database/' + Fsymbol + '/MFI Summary.csv', index_col = 0)
    MFI_Sum = pd.DataFrame(columns = MFI_Result.columns)

    try:
        MFI_Return_Loc = Top_Loc(MFI_Result['MFI Return'])

        for addi in MFI_Return_Loc:
            MFI_Sum = pd.concat([MFI_Sum, MFI_Result.loc[[addi]]], axis = 0)

        MFI_Return60_Loc = Top_Loc(MFI_Result['MFI Return60'])

        for addi in MFI_Return60_Loc:
            MFI_Sum = pd.concat([MFI_Sum, MFI_Result.loc[[addi]]], axis = 0)

        MFI_Rate_Loc = Top_Loc(MFI_Result['MFI Hit Rate'])

        for addi in MFI_Rate_Loc:
            MFI_Sum = pd.concat([MFI_Sum, MFI_Result.loc[[addi]]], axis = 0)

    except:
        print(Fsymbol, ' Error on MFI Best rate stage')

    MFI_Sum['Method'] = 'MFI'

    return MFI_Sum


def Top_Loc(look_column, topi = 9):

    look_column = look_column.sort_values(ascending=False)            #sort_values按數值掛序, ascending = False由大至小
    Top_Loc = []
    Top_Ascend = look_column.index                                    #抽出索引
    Top_Loc.append(Top_Ascend[0])                                     #因為已經按大小排序, 第一行必然是最大

    TL_i = 1
    while len(Top_Loc) < topi:

        if look_column.iloc[TL_i] == look_column.iloc[TL_i - 1]:        #如果和上一行相同, 就跳到下一行直到存放位置的Top_Loc多於topi個項目
            TL_i += 1
        else:
            Top_Loc.append(Top_Ascend[TL_i])
            TL_i += 1
    return Top_Loc

#def Model_1_Performing(Fsymbol):

def combineAnalysis(stock_i):
    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv')
    df = df.drop(['Unnamed: 0'], axis=1)

    df = combineAnalysisCalculation(stock_i, df, 'MFI')
    df = combineAnalysisCalculation(stock_i, df, 'SAR')
    df = combineAnalysisCalculation(stock_i, df, 'RSI')

    print(df)

def combineAnalysisCalculation(stock_i, df, techIndex):
    conditionSum = pd.read_csv('Database/' + stock_i + '/%s Summary.csv' %(techIndex))
    TA_Row_H = conditionSum['%s Hit Rate' % (techIndex)].idxmax()  # 找出準確率最高的一行參數

    if techIndex == 'MFI':
        methCon0 = conditionSum['MFI Time Period'][TA_Row_H]
        methCon1 = conditionSum['MFI Buy Condition'][TA_Row_H]
        methCon2 = conditionSum['MFI Sell Condition'][TA_Row_H]

        df['MFI'] = ta.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod = methCon0)
        holding, buyPrice, buyDate, sellPrice = 0, 0, 0, 0
        df['buyMFI'] = None
        df['sellMFI'] = None
        df['resultMFI'] = None

        for i in range(len(df)):
            if df['MFI'][i] <= methCon1 and holding == 0:
                holding = 1
                buyDate = i
                buyPrice= df['buyMFI'][i] = df['mid'][i]
            elif df['MFI'][i] >= methCon2 and holding == 1:
                holding = 0
                sellDate = i
                sellPrice = df['sellMFI'][i] = df['mid'][i]
                if sellPrice > buyPrice:
                    df['resultMFI'][buyDate : i + 1] = 'Win'
                elif sellPrice <= buyPrice:
                    df['resultMFI'][buyDate : i + 1] = 'Lose'
    elif techIndex == 'SAR':
        methCon0 = conditionSum['SAR Acceleration'][TA_Row_H]
        methCon1 = conditionSum['SAR Max Value'][TA_Row_H]
        methCon2 = conditionSum['SAR Postpone'][TA_Row_H]

        df['SAR'] = ta.SAR(df['high'], df['low'], acceleration=methCon0, maximum=methCon1)
        holding, buyPrice, buyDate, sellPrice = 0, 0, 0, 0
        df['buySAR'] = None
        df['sellSAR'] = None
        df['resultSAR'] = None

        for j in range(len(df)-5):
            if holding == 0 and df['SAR'][j] < df['close'][j]:
                j += methCon2
                holding = 1
                buyDate = j
                buyPrice = df['buySAR'][j] = df['mid'][j]
            elif holding == 1 and df['SAR'][j] > df['close'][j]:
                holding = 0
                sellPrice = df['sellSAR'][j] = df['mid'][j]
                if sellPrice > buyPrice:
                    df['resultSAR'][buyDate: j + 1] = 'Win'
                elif sellPrice <= buyPrice:
                    df['resultSAR'][buyDate: j + 1] = 'Lose'
    elif techIndex == 'RSI':
        methCon0 = conditionSum['RSI Time Period'][TA_Row_H]
        methCon1 = conditionSum['RSI Buy Condition'][TA_Row_H]
        methCon2 = conditionSum['RSI Sell Condition'][TA_Row_H]
        methCon3 = conditionSum['RSI Method'][TA_Row_H]

        df['RSI'] = ta.RSI(df['close'], timeperiod=methCon0)
        holding, buyPrice, buyDate, sellPrice = 0, 0, 0, 0
        df['buyRSI'] = None
        df['sellRSI'] = None
        df['resultRSI'] = None

        if methCon3 == 'Negative':
            for k in range(len(df)):
                if holding == 0 and df['RSI'][k] < methCon1:
                    holding = 1
                    buyDate = k
                    buyPrice = df['buyRSI'][k] = df['mid'][k]
                elif holding == 1 and df['RSI'][k] > methCon2:
                    holding = 0
                    sellPrice = df['sellRSI'][k] = df['mid'][k]
                    if sellPrice > buyPrice:
                        df['resultRSI'][buyDate: k + 1] = 'Win'
                    elif sellPrice <= buyPrice:
                        df['resultRSI'][buyDate: k + 1] = 'Lose'

        elif methCon3 == 'Positive':
            for k in range(len(df)):
                if holding == 0 and df['RSI'][k] > methCon1:
                    holding = 1
                    buyDate = k
                    buyPrice = df['buyRSI'][k] = df['mid'][k]
                elif holding == 1 and df['RSI'][k] < methCon2:
                    holding = 0
                    sellPrice = df['sellRSI'][k] = df['mid'][k]
                    if sellPrice > buyPrice:
                        df['resultRSI'][buyDate: k + 1] = 'Win'
                    elif sellPrice <= buyPrice:
                        df['resultRSI'][buyDate: k + 1] = 'Lose'

    return(df)



if __name__ == '__main__':
    watchlist = pd.read_csv('watchlist.csv', index_col=0)
    Ticket = list(watchlist['Futu symbol'])

    for stock_i in Ticket:
        Performing(stock_i)
        df = Techanalysis.bestAnalysisBase(stock_i)
        df.to_csv('Analysis/' + stock_i + '.csv')
