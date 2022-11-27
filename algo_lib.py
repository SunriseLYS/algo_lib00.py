import numpy as np
import pandas as pd
import tensorflow as tf
import multiprocessing as mp
from tensorflow import keras
from futu import *
from time import sleep
from datetime import datetime, time


def dataPrepare(df=pd.DataFrame(), nextCH=1, minCH=30):
    sortList = ['time_key', 'open', 'close', 'high', 'low', 'turnover', 'mid', 'next 1 ch', 'open dif', 'close dif',
                'high dif', 'low dif']
    delList = ['Unnamed: 0', 'turnover_rate', 'volume', 'pe_ratio', 'change_rate']
    for i in range(len(delList)):  # 按deList刪除列
        if delList[i] in df.columns:
            df.drop(delList[i], axis=1, inplace=True)

    df['mid'] = (df['high'] + df['low']) / 2  # 中位數

    for m in range(1, nextCH + 1):  # 下一分鐘的變動
        df['next %s mid' % (m)] = df['mid'].shift(-m)
        df['next %s ch' % (m)] = df['next %s mid' % (m)] - df['mid']
        df.drop(['next %s mid' % (m)], axis=1, inplace=True)
        # dropList.append('next %s mid' %(m))

    df['open dif'] = df['open'] - df['mid']
    df['close dif'] = df['close'] - df['mid']
    df['high dif'] = df['high'] - df['mid']
    df['low dif'] = df['low'] - df['mid']

    for j in range(1, minCH + 1):  # 比較N分鐘前的變動
        df['p%sb' % (j)] = df['mid'].shift(j)
        df['ch%sb' % (j)] = df['mid'] - df['p%sb' % (j)]
        df['high%sb' % (j)] = df['high'].shift(j)
        df['high ch%sb' % (j)] = df['high'] - df['high%sb' % (j)]
        df['low%sb' % (j)] = df['low'].shift(j)
        df['low ch%sb' % (j)] = df['low'] - df['low%sb' % (j)]

        # dropList.append('p%sb' % (j))
        # dropList.append('high%sb' % (j))
        # dropList.append('low%sb' % (j))
        df.drop(['p%sb' % (j), 'high%sb' % (j), 'low%sb' % (j)], axis=1, inplace=True)

        sortList.append('ch%sb' % (j))
        sortList.append('high ch%sb' % (j))
        sortList.append('low ch%sb' % (j))

    for k in range(10, 100, 10):  # 成交增長
        df['%sbuying power' % (k)] = df['turnover'] - df['turnover'].rolling(k).mean()

        sortList.append('%sbuying power' % (k))

    # 加入第2, 3高及低值
    for l in [5, 10, 20]:
        minCount = l * 330
        df['%sday high' % (l)] = df['high'].rolling(minCount).max()
        df['%s high dif' % (l)] = df['%sday high' % (l)] - df['mid']
        df['%sday low' % (l)] = df['low'].rolling(minCount).min()
        df['%s low dif' % (l)] = df['%sday low' % (l)] - df['mid']

        # dropList.append('%sday high' %(l))
        # dropList.append('%sday low' %(l))
        df.drop(['%sday high' % (l), '%sday low' % (l)], axis=1, inplace=True)

        sortList.append('%s high dif' % (l))
        sortList.append('%s low dif' % (l))

    df = df[sortList]

    df.reset_index(inplace=True, drop=True)

    df_short = df[len(df) - 10000:len(df)]
    df_short.reset_index(inplace=True, drop=True)

    return df, df_short


def dataPrepare1(df, nextCH=1, minCH=30):
    delList = ['Unnamed: 0', 'turnover_rate', 'volume', 'pe_ratio', 'change_rate']
    for i in range(len(delList)):  # 按deList刪除列
        if delList[i] in df.columns:
            df.drop(delList[i], axis=1, inplace=True)

    for j in range(1, minCH + 1):  # 比較N分鐘前的變動
        df['open%sb' % (j)] = df['open'].shift(j)
        df['close%sb' % (j)] = df['close'].shift(j)
        df['high%sb' % (j)] = df['high'].shift(j)
        df['low%sb' % (j)] = df['low'].shift(j)

    for k in range(10, 100, 10):  # 成交增長
        df['%sbuying power' % (k)] = df['turnover'] - df['turnover'].rolling(k).mean()

    for m in range(1, 6):
        minCount = m * 330
        df['%sday high' % (m)] = df['high'].rolling(minCount).max()
        df['%sday low' % (m)] = df['low'].rolling(minCount).min()

    for n in range(10, 100, 10):
        df['%s std' % (n)] = df['close'].rolling(n).std()

    df['date'] = [x[:10] for x in df['time_key']]
    dateList = df['date'].tolist()
    dateList = list(set(dateList))
    dateList.sort()

    df['opening time'] = None
    for o in range(len(dateList)):
        OT = df.loc[df['date'] == dateList[o]].index.tolist()
        openT = 0
        for p in OT:
            df['opening time'][p] = openT
            openT += 1

    df['day high'] = None
    df['day low'] = None
    for ii in range(len(df)):
        opentime = df['opening time'][ii]
        df['day high'][ii] = df['high'][ii - opentime:ii + 1].max()
        df['day low'][ii] = df['low'][ii - opentime:ii + 1].min()

    df.drop(['date', 'opening time'], axis=1, inplace=True)
    df.drop(range(20000), inplace=True)
    df.reset_index(inplace=True, drop=True)
    df.drop(range(len(df) - 5, len(df)), inplace=True)
    df['day high'] = pd.to_numeric(df['day high'], errors='coerce')
    df['day low'] = pd.to_numeric(df['day high'], errors='coerce')

    df.reset_index(inplace=True, drop=True)

    df_short = df[len(df) - 10000:len(df)]
    df_short.reset_index(inplace=True, drop=True)

    return df, df_short


def dataPreparation(dataset, newData, minCH=30):
    newData['mid'] = (newData['high'] + newData['low']) / 2
    newData['open dif'] = newData['open'] - newData['mid']
    newData['close dif'] = newData['close'] - newData['mid']
    newData['high dif'] = newData['high'] - newData['mid']
    newData['low dif'] = newData['low'] - newData['mid']
    dataset = pd.concat([dataset, newData], ignore_index=True)

    for i in range(1, minCH + 1):  # 比較N分鐘前的變動
        dataset['p%sb' % (i)] = dataset['mid'].shift(i)
        dataset['ch%sb' % (i)] = dataset['mid'] - dataset['p%sb' % (i)]
        dataset['high%sb' % (i)] = dataset['high'].shift(i)
        dataset['high ch%sb' % (i)] = dataset['high'] - dataset['high%sb' % (i)]
        dataset['low%sb' % (i)] = dataset['low'].shift(i)
        dataset['low ch%sb' % (i)] = dataset['low'] - dataset['low%sb' % (i)]

        dataset.drop(['p%sb' % (i), 'high%sb' % (i), 'low%sb' % (i)], axis=1, inplace=True)

    for j in range(10, 100, 10):
        dataset['%sbuying power' % (j)] = dataset['turnover'] - dataset['turnover'].rolling(j).mean()

    for k in [5, 10, 20]:
        minCount = k * 330
        dataset['%s high dif' % (k)] = dataset['high'].rolling(minCount).max() - dataset['mid']
        dataset['%s low dif' % (k)] = dataset['low'].rolling(minCount).min() - dataset['mid']

    return dataset, dataset.tail(1)


def dataPreparation1(dataset, newData, minCH=30, openTime=0):
    dataset = pd.concat([dataset, newData], ignore_index=True)

    for j in range(1, minCH + 1):  # 比較N分鐘前的變動
        dataset['open%sb' % (j)] = dataset['open'].shift(j)
        dataset['close%sb' % (j)] = dataset['close'].shift(j)
        dataset['high%sb' % (j)] = dataset['high'].shift(j)
        dataset['low%sb' % (j)] = dataset['low'].shift(j)

    print(dataset.tail(5))

    for k in range(10, 100, 10):  # 成交增長
        dataset['%sbuying power' % (k)] = dataset['turnover'] - dataset['turnover'].rolling(k).mean()

    for m in range(1, 6):
        minCount = m * 330
        dataset['%sday high' % (m)] = dataset['high'].rolling(minCount).max()
        dataset['%sday low' % (m)] = dataset['low'].rolling(minCount).min()

    for n in range(10, 100, 10):
        dataset['%s std' % (n)] = dataset['close'].rolling(n).std()

    dataset['day high'][len(dataset) - 1] = dataset['high'][len(dataset) - openTime:len(dataset)].max()
    dataset['day low'][len(dataset) - 1] = dataset['low'][len(dataset) - openTime:len(dataset)].min()

    dataset['day high'] = pd.to_numeric(dataset['day high'], errors='coerce')
    dataset['day low'] = pd.to_numeric(dataset['day high'], errors='coerce')

    return dataset, dataset.tail(1)


def predict(latestData, sortList, mean, std, mean_target, std_target):
    latestData.drop(['time_key', 'open', 'close', 'turnover'], axis=1, inplace=True)
    print(latestData)
    latestData.reset_index(inplace=True, drop=True)
    latestData = latestData[sortList]

    # 標準化
    inputData = (latestData - mean) / std
    input_array = np.array(inputData)

    model = keras.models.Sequential(name='model-HK.800000',
                                    layers=[keras.layers.Flatten(input_shape=(len(latestData.columns),)),
                                            keras.layers.Dense(128, activation='relu'),
                                            keras.layers.Dense(64, activation='relu'),
                                            keras.layers.Dense(32, activation='relu'),
                                            keras.layers.Dense(2)])

    model.load_weights('Tensor/model-HK.800000/Best-model0-HK.800000.h5')

    predictions = model.predict(input_array)
    predict_low = (predictions[0][0] * std_target['next 1 low']) + mean_target['next 1 low']
    predict_high = (predictions[0][1] * std_target['next 1 high']) + mean_target['next 1 high']

    predict_low_ch = predict_low - latestData['low'][0]
    predict_high_ch = predict_high - latestData['high'][0]
    print(predict_low_ch, predict_high_ch)

    return predict_low_ch, predict_high_ch


def supTodayData(stock_i):
    minOpen = datetime.now() - timedelta(hours=9, minutes=30)
    df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '_min.csv', index_col=0)
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

    ret_sub, err_message = quote_ctx.subscribe([stock_i], [SubType.K_1M], subscribe_push=False)
    if ret_sub == RET_OK:
        ret, data = quote_ctx.get_cur_kline(stock_i, 152, SubType.K_1M, AuType.QFQ)
        if ret == RET_OK:
            print(data)
            data.drop(['code', 'last_close', 'turnover_rate', 'volume', 'pe_ratio'], axis=1, inplace=True)
            data.drop(len(data) - 1, inplace=True)
            df = pd.concat([df, data], ignore_index=True)

            return df
        else:
            print('error:', data)
    else:
        print('subscription failed', err_message)


def tradeCheck(orderID, qty, side):
    trd_ctx = OpenFutureTradeContext(host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
    ret, data_OrderList = trd_ctx.order_list_query(order_id=orderID)

    if data_OrderList.shape[0] > 0:
        orderStatus = data_OrderList['order_status'][0]
        orderPrice = data_OrderList['price'][0]

        chTime = 0
        while orderStatus != 'FILLED_ALL' and chTime < 5:
            ret, dataOrder = trd_ctx.modify_order(ModifyOrderOp.NORMAL, orderID, qty, orderPrice)
            print('改單')
            orderPrice += side
            sleep(1)
            chTime += 1
            ret, data_OrderList = trd_ctx.order_list_query(order_id=orderID)
        if orderStatus != 'FILLED_ALL' and chTime >= 5:
            ret, dataOrder = trd_ctx.modify_order(ModifyOrderOp.CANCEL, orderID, 0, 0)
            print(dataOrder)
            print('取消')

    trd_ctx.close()


def trade(prediction, pwd_unlock, trd_ctx, product_code, position_Loss, lastPosition_Loss):
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    ret, data = trd_ctx.unlock_trade(pwd_unlock)
    lastPrice, minLoss = int(0), int(0)
    positionQty_Short, positionQty_Long, latestPrice, coldDown = 0, 0, 0, 0

    ret, data_position = trd_ctx.position_list_query(code=product_code)  # 不可以用MHIcurrent
    if ret == RET_OK:
        print(data_position)
        if data_position.shape[0] > 0:
            if data_position['position_side'][0] == 'SHORT':  # 優化
                positionQty_Short = data_position['qty'][0]
            elif data_position['position_side'][0] == 'LONG':
                positionQty_Long = data_position['qty'][0]

            if data_position.shape[0] > 1:
                if data_position['position_side'][1] == 'SHORT':
                    positionQty_Short = data_position['qty'][1]
                elif data_position['position_side'][1] == 'LONG':
                    positionQty_Long = data_position['qty'][1]

            # cost = data_position['qty'][0]
    else:
        print('position_list_query error: ', data_position)

    ret_sub, err_message = quote_ctx.subscribe(['HK.MHIcurrent'], [SubType.QUOTE], subscribe_push=False)
    if ret_sub == RET_OK:
        ret, data_Price = quote_ctx.get_stock_quote(['HK.MHIcurrent'])
        if ret == RET_OK:
            lastPrice = latestPrice
            latestPrice = data_Price['last_price'][0]  # 取得最新價
        else:
            print('error:', data_Price)
    else:
        print('subscription failed', err_message)

    if position_Loss < lastPosition_Loss:  # 如對比上一分鐘, 價格下跌了超過30點便會觸發止蝕
        minLoss = lastPosition_Loss - position_Loss

    if minLoss < -300 or position_Loss < -600:  # 止蝕
        if positionQty_Long > 0:
            ret, data_place = trd_ctx.place_order(price=latestPrice, qty=1, code='HK.MHIcurrent', trd_side=TrdSide.SELL,
                                                  order_type=OrderType.MARKET)
        elif positionQty_Short > 0:
            ret, data_place = trd_ctx.place_order(price=latestPrice, qty=1, code='HK.MHIcurrent', trd_side=TrdSide.BUY,
                                                  order_type=OrderType.MARKET)
        print('止蝕, 靜止')
        print(data_place)
        coldDown = 1
    elif prediction > 0 and positionQty_Long == 0 and positionQty_Short == 0:  # 上升預測及未開倉
        ret, data_place = trd_ctx.place_order(price=latestPrice - 6, qty=1, code='HK.MHIcurrent', trd_side=TrdSide.BUY)
        print('買入')
        print(data_place)
        orderID = data_place['order_id'][0]
        worker0 = mp.Process(target=tradeCheck, args=(orderID, 1, 1,))
        worker0.start()
    elif prediction < 0 and positionQty_Long == 0 and positionQty_Short == 0:  # 下跌預測及未開倉
        ret, data_place = trd_ctx.place_order(price=latestPrice + 6, qty=1, code='HK.MHIcurrent', trd_side=TrdSide.SELL)
        print('沽出')
        print(data_place)
        orderID = data_place['order_id'][0]
        worker0 = mp.Process(target=tradeCheck, args=(orderID, 1, -1,))
        worker0.start()
    elif prediction > 0 and positionQty_Short < 0:  # 上升預測, 但持有短倉
        print('平倉, 買入')
        ret, data_place = trd_ctx.place_order(price=latestPrice, qty=1, code='HK.MHIcurrent', trd_side=TrdSide.SELL,
                                              order_type=OrderType.MARKET)
        sleep(0.1)
        ret, data_place = trd_ctx.place_order(price=latestPrice - 6, qty=1, code='HK.MHIcurrent', trd_side=TrdSide.BUY)
        print(data_place)
        orderID = data_place['order_id'][0]
        worker0 = mp.Process(target=tradeCheck, args=(orderID, 1, 1,))
        worker0.start()
    elif prediction < 0 and positionQty_Long > 0:  # 下跌預測, 但持有長倉
        print('平倉, 沽出')
        ret, data_place = trd_ctx.place_order(price=latestPrice, qty=1, code='HK.MHIcurrent', trd_side=TrdSide.SELL,
                                              order_type=OrderType.MARKET)
        sleep(0.1)
        ret, data_place = trd_ctx.place_order(price=latestPrice + 6, qty=1, code='HK.MHIcurrent', trd_side=TrdSide.SELL)
        print(data_place)
        orderID = data_place['order_id'][0]
        worker0 = mp.Process(target=tradeCheck, args=(orderID, 1, -1,))
        worker0.start()
    elif prediction > 0 and positionQty_Long > 0:  # 上升預測及已經有長倉
        pass
    elif prediction < 0 and positionQty_Short < 0:  # 下跌預測及已經有短倉
        pass

    quote_ctx.close()
    return latestPrice, coldDown


def trading_Future(stock_i, mean, std, mean_target, std_target, pwd_unlock):
    dataset = pd.read_csv('Database/' + stock_i + '/' + stock_i + '_short.csv', index_col=0)
    dataset.drop('index', axis=1, inplace=True)
    trade_record = pd.DataFrame(columns={'time', 'prediction 1', 'prediction 2', 'Price'})
    sortList = dataset.columns
    sortList = sortList.drop(['time_key', 'open', 'close', 'turnover'])
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    trd_ctx = OpenFutureTradeContext(host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
    current_time = datetime.now().time()
    position_Loss, lastPosition_Loss = int(), int()
    openTime, dayLoss, lastPrice = 1, 0, 0
    current_time_str = str(datetime.now().date())
    product_code = 'HK.MHI' + current_time_str[2:4] + current_time_str[5:7]

    while not time(9, 30) <= current_time < time(11, 50) and not time(13, 0) <= current_time < time(20, 50):
        current_time = datetime.now().time()
        if current_time > time(16, 0):
            break
    else:
        ret_sub, err_message = quote_ctx.subscribe([stock_i], [SubType.K_1M], subscribe_push=False)
        while time(9, 30) <= current_time < time(11, 50) or time(13, 0) <= current_time < time(20, 50):
            current_time = datetime.now().time()
            if str(current_time)[6:8] == '57':  # 如果一開市即刻拿取資料, 那一分鐘的資料並不完全
                ret, data_Account = trd_ctx.accinfo_query()
                print(data_Account['unrealized_pl'].sum(), data_Account['realized_pl'].sum())
                lastPosition_Loss = position_Loss
                position_Loss = data_Account['unrealized_pl'].sum()
                dayLoss = data_Account['unrealized_pl'].sum() + data_Account['realized_pl'].sum()
                print('Day Loss: ' + str(dayLoss))
                print('lastPosition_Loss: ' + str(lastPosition_Loss))
                print('position_Loss: ' + str(position_Loss))

                if dayLoss < -1200:
                    print('stop')
                    break

                ret, data = quote_ctx.get_cur_kline(stock_i, openTime, SubType.K_1M, AuType.QFQ)
                if ret == RET_OK:
                    data.drop(['code', 'last_close', 'turnover_rate', 'volume', 'pe_ratio'], axis=1, inplace=True)
                    dataset, latestData = dataPreparation1(dataset, data, 30, openTime)
                    predict_low_ch, predict_high_ch = predict(latestData, sortList, mean, std, mean_target, std_target)
                    print(dataset.tail(5))
                    dataset.drop(range(len(dataset) - openTime, len(dataset)), inplace=True)

                    if openTime > 30:  # 前30分鐘不交易
                        '''
                        lastPrice, coldDown = trade(predict_low_ch, predict_high_ch, pwd_unlock, trd_ctx, product_code, position_Loss, lastPosition_Loss)
                        if coldDown == 1:
                            openTime = 0'''

                    openTime += 1
                    newTrade = {'time': str(current_time), 'prediction 1': predict_low_ch,
                                'prediction 2': predict_high_ch, 'Price': lastPrice}
                    lastPrice = 0
                    trade_record = trade_record.append(newTrade, ignore_index=True)
            sleep(1)

    quote_ctx.close()
    trade_record.to_csv('Database/' + stock_i + '/' + stock_i + '_trade.csv')
    dataset.to_csv('Database/' + stock_i + '/' + stock_i + '_Today.csv')
    print('Day end')


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
    Q_result = {'Q0': quadrant0/1000, 'Q1': quadrant1/1000, 'Q2': quadrant2/1000, 'Q3': quadrant3/1000}
    return Q_result

