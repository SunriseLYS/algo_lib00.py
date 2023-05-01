import talib as ta
import Techanalysis as ts
from time import sleep
from datetime import timedelta, time
import pandas as pd
from futu import *
import os, shutil
import mysql.connector
from mysql.connector import Error
import base64
from email.message import EmailMessage
import os.path
from google.auth.transport.requests import Request
import google.auth
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import mimetypes

pd.set_option('display.max_columns', 10)  # pandas setting 顥示列數上限
pd.set_option('display.width', 1000)  # pandas setting 顯示列的闊度

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.dirname(
    os.path.abspath(__file__)) + "/dauntless-brace-355907-ccb9311dcf58.json"

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

            order_col = 'time'
            if table == 'Day':
                order_col = 'date'
            elif table == 'Mins':
                order_col = 'time_key'

            self.cursor.execute("SELECT * FROM %s ORDER BY %s" % (table, order_col))   # Day, Mins, YYYY_MM_DD
            df = pd.DataFrame(self.cursor.fetchall(), columns=column_list)
            return df
        except:
            print('Dose not exist')

    def table_check(self, stock_i_, table):
        try:
            self.cursor.execute("USE %s" % (stock_i_))
            self.cursor.execute("SHOW columns FROM %s" % (table))
            column_list = [i[0] for i in self.cursor.fetchall()]

            self.cursor.execute("SELECT * FROM %s.%s" % (stock_i_, table))
            df = pd.DataFrame(self.cursor.fetchall(), columns=column_list)
            return df
        except:
            print('Dose not exist')


    def date_update(self, stock_i_, table, set_value, condition):
        self.cursor.execute("USE %s" % (stock_i_))
        self.cursor.execute("UPDATE %s SET %s WHERE %s" % (table, set_value, condition))


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
        value = " VALUES" + " (" + "%s," * values_num
        sql = sqlTo + value[:-1] + ")"
        self.cursor.execute(sql, tuple(row))
        self.connection.commit()

    def data_input_tailor(self, DB_name, table, value):
        sqlTo = "INSERT INTO %s.%s VALUES " % (DB_name, table)

        sql = sqlTo + value
        self.cursor.execute(sql)
        self.connection.commit()


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


def gmail_create_draft(emailAdd, sub, content, sec_content=None, att=None):
    '''address, subject, content'''
    creds, _ = google.auth.default()

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('gmail', 'v1', credentials=creds)
        message = EmailMessage()

        message.set_content(str(content) + '\n\n' + str(sec_content))

        message['To'] = emailAdd
        message['From'] = 'origin.sunrise@gmail.com'
        message['Subject'] = sub

        if att is not None:
            type_subtype, _ = mimetypes.guess_type(att)
            maintype, subtype = type_subtype.split('/')
            with open(att, 'rb') as fp:
                attachment_data = fp.read()
            message.add_attachment(attachment_data, maintype, subtype, filename=att)

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {
            'raw': encoded_message
        }
        # pylint: disable=E1101
        send_message = (service.users().messages().send
                        (userId="me", body=create_message).execute())
        print(F'Message Id: {send_message["id"]}')
    except HttpError as error:
        print(F'An error occurred: {error}')
        send_message = None
    return send_message


class OrderBookTest(OrderBookHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret_code, data_order = super(OrderBookTest, self).on_recv_rsp(rsp_pb)
        if ret_code != RET_OK:
            print("OrderBookTest: error, msg: %s" % data_order)
            return RET_ERROR, data_order
        print("OrderBookTest ", data_order)  # OrderBookTest 自己的处理逻辑
        return RET_OK, data_order


class TickerTest(TickerHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret_code, data_tiker = super(TickerTest, self).on_recv_rsp(rsp_pb)
        if ret_code != RET_OK:
            print("TickerTest: error, msg: %s" % data_tiker)
            return RET_ERROR, data_tiker
        print("TickerTest ", data_tiker)  # TickerTest 自己的处理逻辑
        return RET_OK, data_tiker


def suspension_check(quote_ctx, symbol):
    ret, data_state = quote_ctx.get_market_state(symbol)  # 檢查停牌股票
    print(symbol)
    if ret == RET_OK:
        for state_i in range(len(data_state)):
            if data_state['market_state'][state_i] == 'CLOSED':
                data_state = data_state.drop([state_i])
    else:
        print('Market state error:', data_state)
    symbol = data_state['code'].tolist()

    return symbol

def model3(df, P_level = None):   # P_level應是現價
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    distribution_T = df.groupby('price')['turnover'].sum()

    if P_level is None:
        import statistics
        list_P = distribution_T.index
        P_level = statistics.median(list_P)

    distribution_T = pd.DataFrame(distribution_T)
    #distribution_T.reset_index(inplace=True)
    distribution_T['index'] = distribution_T.index.map(lambda x: x - P_level)
    distribution_T['distribution'] = distribution_T['turnover'] * distribution_T['index']

    an = (distribution_T['distribution'].sum() / distribution_T['distribution'].abs().sum()) * 100
    m3_value = round(an, 4)

    return m3_value

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

def market_check_HK():
    sleep(10)   # 避免太快，伺服器還未更新
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    marState = quote_ctx.get_global_state()

    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()
    #symbol = suspension_check(quote_ctx, symbol)
    symbol_dict = {i: i.replace('.', '_') for i in symbol}   # 轉變成Dictionary

    for j in symbol_dict:
        exec('df_{} = {}'.format(symbol_dict[j], 'pd.DataFrame()'))  # 創建動態變量, e.g. df_HK_00005

    ret_sub, err_message = quote_ctx.subscribe(symbol, [SubType.TICKER], subscribe_push=False)   #訂閱
    if ret_sub == RET_OK:
        pass
    else:
        print('subscription failed', err_message)

    ana_time: int = 1
    while marState[1]['market_hk'] == 'MORNING' or marState[1]['market_hk'] == 'AFTERNOON':
        start_time = str(datetime.now())
        model6_result = pd.DataFrame(columns={'lower', 'upper', 'turnover'}, index=symbol)
        for stock_i, stock_i_ in symbol_dict.items():
            ret, data = quote_ctx.get_rt_ticker(stock_i, 1000)
            if ret == RET_OK:
                exec('df_{stock_i_} = pd.concat([df_{stock_i_}, data])'.format(stock_i_=stock_i_))
                exec('df_{stock_i_}.drop_duplicates(subset=["sequence"], keep="first", inplace=True)'.format(stock_i_=stock_i_))
                exec('df_{stock_i_}.to_csv("Ram/{stock_i}.csv")'.format(stock_i_=stock_i_, stock_i=stock_i))
                if ana_time % 2 == 0:
                    df_M3 = pd.read_csv('Ram/{stock_i}.csv'.format(stock_i=stock_i), index_col=0)

                    if isinstance(df_M3['time'][0], datetime):
                        pass
                    else:
                        df_M3['time'] = pd.to_datetime(df_M3['time'])
                    model6_result.loc[stock_i] = ts.model6_2_4(df_M3)
                    #加一個Dataframe 存放當天model 3的結果, 並以附件發出gmail
            else:
                print('error:', data)
        end_time = str(datetime.now())

        if ana_time % 2 == 0:
            maket_trend = model6_result['upper'].sum() / model6_result['lower'].sum()
            #model6_result['Adjusted'] = model6_result['Ratio'] * maket_trend

            model6_result = model6_result[['lower', 'upper', 'turnover', 'Adjusted']]

            '''
            try:
                m3_df = pd.concat([m3_df, pd.DataFrame.from_dict(model6_result, orient="index", columns=[end_time[:10]])], axis=1)
            except: pass'''

            #model6_result = str(model6_result).replace(',', '\n')

            try:
                gmail_create_draft('alphax.lys@gmail.com', str(ana_time) + ' ' + start_time + ' ' + end_time, model6_result, maket_trend, 'Model3_r.csv')
            except: pass

        ana_time += 1
        sleep(600)

        marState = quote_ctx.get_global_state()
        if marState[1]['market_hk'] == 'REST':
            sleep(3600)
            while True:
                marState = quote_ctx.get_global_state()
                if marState[1]['market_hk'] != 'REST':
                    break
                sleep(10)

    else:
        print('HK Market Closed')
        for stock_i, stock_i_ in symbol_dict.items():
            ret, data = quote_ctx.get_rt_ticker(stock_i, 1000)   #收市時, 蒐集最後一次
            if ret == RET_OK:
                exec('df_{stock_i_} = pd.concat([df_{stock_i_}, data])'.format(stock_i_=stock_i_))
                exec('df_{}.to_csv("Ram/" + stock_i + ".csv")'.format(stock_i_))
            else:
                print('error:', data)

        print('price collection completed')

    ret_unsub, err_message_unsub = quote_ctx.unsubscribe(symbol, [SubType.TICKER])   #取消訂閱
    if ret_unsub == RET_OK:
        pass
    else:
        print('unsubscription failed！', err_message_unsub)

    sleep(1200)

    ddcoll(quote_ctx, symbol)

    quote_ctx.close()


def analysisUS():
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    marState = quote_ctx.get_global_state()
    if marState[1]['market_us'] == 'CLOSED':
        print('US Market Closed')
        quote_ctx.close()
        pass
    elif marState[1]['market_us'] == 'AFTERNOON':  # 美股不分早午市
        for i in range(6):  # 每半小時做一次分析, 只做6次
            watchlist = pd.read_csv('watchlistUS.csv')
            symbol = watchlist['Futu symbol'].tolist()
            ret, data = quote_ctx.get_market_state(symbol)  # 檢查有沒有停牌股票
            if ret == RET_OK:
                for state_i in range(len(data)):
                    if data['market_state'][state_i] == 'CLOSED':
                        data = data.drop([state_i])
                symbol = data['code'].tolist()
                ret, data = quote_ctx.get_market_snapshot(symbol)
                if ret == RET_OK:
                    dfsnap = data.set_index('code')
                    dfsnap.to_csv('Snapshot.csv')
                    realTimeAnalysis(symbol, dfsnap)
                else:
                    print('Snapshot error:', data)
            else:
                print('Market state error:', data)
            print('real-time analysisUS not completed')  # real-time analysis
            sleep(1800)
    elif marState[1]['market_us'] == 'PRE_MARKET_BEGIN':  # 如果仍然是開市前交易時段, 則為冬令時間延後1個小時
        sleep(3600)
        for i in range(6):  # 每半小時做一次分析, 只做6次
            watchlist = pd.read_csv('watchlistUS.csv')
            symbol = watchlist['Futu symbol'].tolist()
            ret, data = quote_ctx.get_market_state(symbol)  # 檢查有沒有停牌股票
            if ret == RET_OK:
                for state_i in range(len(data)):
                    if data['market_state'][state_i] == 'CLOSED':
                        data = data.drop([state_i])
                symbol = data['code'].tolist()
                ret, data = quote_ctx.get_market_snapshot(symbol)
                if ret == RET_OK:
                    dfsnap = data.set_index('code')
                    dfsnap.to_csv('Snapshot.csv')
                    realTimeAnalysis(symbol, dfsnap)
                else:
                    print('Snapshot error:', data)
            else:
                print('Market state error:', data)
            print('real-time analysisUS not completed')  # real-time analysis
            sleep(1800)


def realTimeAnalysis(symbol, dfsnap):
    SAR_Signal_List_H, RSI_Signal_List_H, MACD_Signal_List_H, BB_Signal_List_H, MFI_Signal_List_H, SAR_Signal_List_R, \
    RSI_Signal_List_R, MACD_Signal_List_R, BB_Signal_List_R, MFI_Signal_List_R = [], [], [], [], [], [], [], [], [], []
    SAR_Sell_List_H, RSI_Sell_List_H, MACD_Sell_List_H, BB_Sell_List_H, MFI_Sell_List_H, SAR_Sell_List_R, RSI_Sell_List_R, \
    MACD_Sell_List_R, BB_Sell_List_R, MFI_Sell_List_R = [], [], [], [], [], [], [], [], [], []

    Tech_Table_H = pd.DataFrame(columns=['symbol', 'Method', 'TA Value', 'Other'])
    Tech_Table_H['symbol'] = symbol
    Tech_Table_H = Tech_Table_H.set_index('symbol')
    Tech_Table_R = pd.DataFrame(columns=['symbol', 'Method', 'TA Value', 'Other'])
    Tech_Table_R['symbol'] = symbol
    Tech_Table_R = Tech_Table_R.set_index('symbol')

    dfsnap = dfsnap.rename(
        columns={'update_time': 'date', 'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
                 'last_price': 'close'})

    for stock_i in symbol:
        latestPrice = dfsnap.loc[[stock_i]]
        Main_record = pd.read_csv('Database/' + stock_i + '/' + stock_i + '.csv', index_col=0)
        Main_record = pd.concat([Main_record, latestPrice])
        Main_record = Main_record.reset_index()
        Main_record = Main_record.drop(['index'], axis=1)
        Method_Sum = pd.read_csv('Database/' + stock_i + '/Method Summary.csv')

        try:
            TA_Row_H = Method_Sum['Hit Rate'].idxmax()  # 取得成功率最高的列數
            if Method_Sum['Tech Method'][TA_Row_H] == 'SAR':
                print('SAR')
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_H]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_H]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_H]
                Main_record['SAR'] = ta.SAR(Main_record['high'], Main_record['low'], acceleration=TA_Con0,
                                            maximum=TA_Con1)
                TA_value = Main_record['SAR'][len(Main_record) - 1]
                Former_TA_value = Main_record['SAR'][len(Main_record) - 2]
                Latest_P = Main_record['close'][len(Main_record) - 1]
                Tech_Table_H['Method'][stock_i] = 'SAR'
                Tech_Table_H['TA Value'][stock_i] = TA_value
                Tech_Table_H['Other'][stock_i] = 'Postpone', str(TA_Con2)

                if TA_value < Latest_P and Former_TA_value > Latest_P:
                    SAR_Signal_List_H.append(stock_i, TA_value, 'Postpone', TA_Con2, 'Day')
                elif TA_value > Latest_P and Former_TA_value < Latest_P:
                    SAR_Sell_List_H.append(stock_i)

            elif Method_Sum['Tech Method'][TA_Row_H] == 'RSI':
                print('RSI')
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_H]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_H]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_H]
                TA_Con3 = Method_Sum['Method Condition3'][TA_Row_H]
                Main_record['RSI'] = ta.RSI(Main_record['close'], timeperiod=TA_Con0)
                TA_value = Main_record['RSI'][len(Main_record) - 1]
                Former_TA_value = Main_record['RSI'][len(Main_record) - 2]
                Tech_Table_H['Method'][stock_i] = 'RSI'
                Tech_Table_H['TA Value'][stock_i] = TA_value
                Tech_Table_H['Other'][stock_i] = TA_Con3

                if TA_Con3 == 'Positive' and TA_value > TA_Con1 and Former_TA_value < TA_Con1:
                    RSI_Signal_List_H.append(stock_i + ' Positive')
                elif TA_Con3 == 'Positive' and TA_value > TA_Con2 and Former_TA_value < TA_Con2:
                    RSI_Sell_List_H.append(stock_i + ' Positive')
                elif TA_Con3 == 'Negative' and TA_value < TA_Con1 and Former_TA_value > TA_Con1:
                    RSI_Signal_List_H.append(stock_i + ' Negative')
                elif TA_Con3 == 'Negative' and TA_value > TA_Con2 and Former_TA_value < TA_Con2:
                    RSI_Sell_List_H.append(stock_i + ' Negative')

            elif Method_Sum['Tech Method'][TA_Row_H] == 'MACD':
                print('MACD')
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_H]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_H]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_H]
                Main_record['MACD'], Main_record['MACD Signal'], Main_record['MACD Hist'] = ta.MACD(
                    Main_record['close'], fastperiod=TA_Con0, slowperiod=TA_Con1, singalperiod=TA_Con2)
                TA_value = Main_record['MACD Hist'][len(Main_record) - 1]
                Former_TA_value = Main_record['MACD Hist'][len(Main_record) - 2]
                Tech_Table_H['Method'][stock_i] = 'MACD'
                Tech_Table_H['TA Value'][stock_i] = TA_value

                if TA_value > 0 and Former_TA_value <= 0:
                    MACD_Signal_List_H.append(stock_i)
                elif TA_value <= 0 and Former_TA_value > 0:
                    MACD_Sell_List_H.append(stock_i)

            elif Method_Sum['Tech Method'][TA_Row_H] == 'BB':
                print('BB')
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_H]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_H]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_H]
                Main_record['BB Upper'], Main_record['BB Middle'], Main_record['BB Lower'] = ta.BBANDS(
                    Main_record['close'], Timeperiod=TA_Con0, Upper=TA_Con1, Lower=TA_Con2)
                TA_value = Main_record['BB Lower'][len(Main_record) - 1]
                TA_value1 = Main_record['BB Middle'][len(Main_record) - 1]
                Latest_P = Main_record['Close'][len(Main_record) - 1]
                Tech_Table_H['Method'][stock_i] = 'BB'
                Tech_Table_H['TA Value'][stock_i] = TA_value
                Tech_Table_H['Other'][stock_i] = TA_value1

                if TA_value > Latest_P:
                    BB_Signal_List_H.append(stock_i)
                elif TA_value1 < Latest_P:
                    BB_Sell_List_H.append(stock_i)

            elif Method_Sum['Tech Method'][TA_Row_H] == 'MFI':
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_H]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_H]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_H]
                Main_record['MFI'] = ta.MFI(Main_record['high'], Main_record['low'], Main_record['close'],
                                            Main_record['volume'], timeperiod=TA_Con0)
                TA_value = Main_record['MFI'][len(Main_record) - 1]
                Former_TA_value = Main_record['MFI'][len(Main_record) - 2]
                Tech_Table_H['Method'][stock_i] = 'MFI'
                Tech_Table_H['TA Value'][stock_i] = TA_value
                Tech_Table_H['Other'][stock_i] = Former_TA_value

                if TA_value <= TA_Con1 and Former_TA_value > TA_Con1:
                    MFI_Signal_List_H.append(stock_i)
                elif TA_value >= TA_Con1 and Former_TA_value < TA_Con1:
                    MFI_Sell_List_H.append(stock_i)

        except:
            print('Method analysis-1 stage error ', stock_i)

        try:
            TA_Row_R = Method_Sum['Return'].idxmax()  # 取得回報最高的列數
            if Method_Sum['Tech Method'][TA_Row_R] == 'SAR':
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_R]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_R]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_R]
                Main_record['SAR'] = ta.SAR(Main_record['high'], Main_record['low'], acceleration=TA_Con0,
                                            maximum=TA_Con1)
                TA_value = Main_record['SAR'][len(Main_record) - 1]
                Former_TA_value = Main_record['SAR'][len(Main_record) - 2]
                Latest_P = Main_record['close'][len(Main_record) - 1]
                Tech_Table_R['Method'][stock_i] = 'SAR'
                Tech_Table_R['TA Value'][stock_i] = TA_value
                Tech_Table_R['Other'][stock_i] = 'Postpone', str(TA_Con2)

                if TA_value < Latest_P and Former_TA_value > Latest_P:
                    SAR_Signal_List_R.append(stock_i, TA_value, 'Postpone', TA_Con2, 'Day')
                elif TA_value > Latest_P and Former_TA_value < Latest_P:
                    SAR_Sell_List_R.append(stock_i)

            elif Method_Sum['Tech Method'][TA_Row_R] == 'RSI':
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_R]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_R]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_R]
                TA_Con3 = Method_Sum['Method Condition3'][TA_Row_R]
                Main_record['RSI'] = ta.RSI(Main_record['close'], timeperiod=TA_Con0)
                TA_value = Main_record['RSI'][len(Main_record) - 1]
                Former_TA_value = Main_record['RSI'][len(Main_record) - 2]
                Tech_Table_R['Method'][stock_i] = 'RSI'
                Tech_Table_R['TA Value'][stock_i] = TA_value
                Tech_Table_R['Other'][stock_i] = TA_Con3

                if TA_Con3 == 'Positive' and TA_value > TA_Con1 and Former_TA_value < TA_Con1:
                    RSI_Signal_List_R.append(stock_i + ' Positive')
                elif TA_Con3 == 'Positive' and TA_value > TA_Con2 and Former_TA_value < TA_Con2:
                    RSI_Sell_List_R.append(stock_i + ' Positive')
                elif TA_Con3 == 'Negative' and TA_value < TA_Con1 and Former_TA_value > TA_Con1:
                    RSI_Signal_List_R.append(stock_i + ' Negative')
                elif TA_Con3 == 'Negative' and TA_value > TA_Con2 and Former_TA_value < TA_Con2:
                    RSI_Sell_List_R.append(stock_i + ' Negative')

            elif Method_Sum['Tech Method'][TA_Row_R] == 'MACD':
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_R]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_R]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_R]
                Main_record['MACD'], Main_record['MACD Signal'], Main_record['MACD Hist'] = ta.MACD(
                    Main_record['close'], fastperiod=TA_Con0, slowperiod=TA_Con1, singalperiod=TA_Con2)
                TA_value = Main_record['MACD Hist'][len(Main_record) - 1]
                Former_TA_value = Main_record['MACD Hist'][len(Main_record) - 2]
                Tech_Table_R['Method'][stock_i] = 'MACD'
                Tech_Table_R['TA Value'][stock_i] = TA_value

                if TA_value > 0 and Former_TA_value <= 0:
                    MACD_Signal_List_R.append(stock_i)
                elif TA_value <= 0 and Former_TA_value > 0:
                    MACD_Sell_List_R.append(stock_i)

            elif Method_Sum['Tech Method'][TA_Row_R] == 'BB':
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_R]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_R]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_R]
                Main_record['BB Upper'], Main_record['BB Middle'], Main_record['BB Lower'] = ta.BBANDS(
                    Main_record['close'], Timeperiod=TA_Con0, Upper=TA_Con1, Lower=TA_Con2)
                TA_value = Main_record['BB Lower'][len(Main_record) - 1]
                TA_value1 = Main_record['BB Middle'][len(Main_record) - 1]
                Latest_P = Main_record['Close'][len(Main_record) - 1]
                Tech_Table_R['Method'][stock_i] = 'BB'
                Tech_Table_R['TA Value'][stock_i] = TA_value
                Tech_Table_R['Other'][stock_i] = TA_value1

                if TA_value > Latest_P:
                    BB_Signal_List_H.append(stock_i)
                elif TA_value1 < Latest_P:
                    BB_Sell_List_H.append(stock_i)

            elif Method_Sum['Tech Method'][TA_Row_R] == 'MFI':
                TA_Con0 = Method_Sum['Method Condition0'][TA_Row_R]
                TA_Con1 = Method_Sum['Method Condition1'][TA_Row_R]
                TA_Con2 = Method_Sum['Method Condition2'][TA_Row_R]
                Main_record['MFI'] = ta.MFI(Main_record['High'], Main_record['Low'], Main_record['close'],
                                            Main_record['volume'], timeperiod=TA_Con0)
                TA_value = Main_record['MFI'][len(Main_record) - 1]
                Former_TA_value = Main_record['MFI'][len(Main_record) - 2]
                Tech_Table_R['Method'][stock_i] = 'MFI'
                Tech_Table_R['TA Value'][stock_i] = TA_value
                Tech_Table_R['Other'][stock_i] = Former_TA_value

                if TA_value <= TA_Con1 and Former_TA_value > TA_Con1:
                    MFI_Signal_List_R.append(stock_i)
                elif TA_value >= TA_Con1 and Former_TA_value < TA_Con1:
                    MFI_Sell_List_R.append(stock_i)
        except:
            print('Method analysis-2 stage error ', stock_i)

    Tech_Table_H.to_csv('EmailAtt/Tech_Table_H.csv')
    Tech_Table_R.to_csv('EmailAtt/Tech_Table_R.csv')

    Moment = datetime.now().time()
    Msg = "Check for the time: " + str(Moment) + "\n SAR Buy(H): " + str(SAR_Signal_List_H) + "\n RSI Buy(H): " + str(
        RSI_Signal_List_H) + "\n MACD Buy(H): " + str(MACD_Signal_List_H) + \
          "\n BB Buy(H): " + str(BB_Signal_List_H) + "\n MFI Buy(H): " + str(
        MFI_Signal_List_H) + "\n\n SAR Buy(R): " + str(SAR_Signal_List_R) + "\n RSI Buy(R): " + str(RSI_Signal_List_R) + \
          "\n BB Buy(R): " + str(BB_Signal_List_R) + "\n MACD Buy(R): " + str(
        MACD_Signal_List_R) + "\n MFI Buy(R): " + str(MFI_Signal_List_R) + "\n\n SAR Sell(H): " + str(SAR_Sell_List_H) + \
          "\n RSI Sell(H): " + str(RSI_Sell_List_H) + "\n MACD Sell(H): " + str(
        MACD_Sell_List_H) + "\n BB Sell(H): " + str(BB_Sell_List_H) + "\n MFI Sell(H): " + str(MFI_Sell_List_H) + \
          "\n\n SAR Sell(R): " + str(SAR_Sell_List_R) + "\n RSI Sell(R): " + str(
        RSI_Sell_List_R) + "\n MACD Sell(R): " + str(MACD_Sell_List_R) + "\n BB Sell(R): " + str(BB_Sell_List_R) + \
          "\n MFI Sell(R): " + str(MFI_Sell_List_R)

    # Msg = "Check for the time: " + str(Moment) + "\n SAR" + str(SAR_Signal_List_H) + "\n RSI" + str(RSI_Signal_List_H) + '\n MACD' + str(MACD_Signal_List_H) + '\n MFI' + str(MFI_Signal_List_H)
    att1 = 'EmailAtt/Tech_Table_H.csv'
    att2 = 'EmailAtt/Tech_Table_R.csv'
    att3 = 'EmailAtt/pn_Table.csv'



def ddcoll(quote_ctx, symbol):
    connection = Database('103.68.62.116', 'root', '630A78e77?')
    current_time = str(datetime.now().date())

    for stock_i in symbol:
        stock_i_ = stock_i.replace('.', '_')
        showHall = connection.show_db(stock_i_)

        if showHall == []:  # 檢查數據庫是否存在, []代表無現有記錄
            connection.creat_database(stock_i_)

            sql = "CREATE TABLE Day(date DATE, open FLOAT, close FLOAT, high FLOAT, low FLOAT, pe_ratio FLOAT, " \
                  "turnover_rate FLOAT, volume BIGINT, turnover BIGINT, change_rate FLOAT, mid FLOAT, PRIMARY KEY (date), " \
                  "UNIQUE INDEX date(date))"
            connection.creat_table(stock_i_, sql)

            sql = "CREATE TABLE Mins(time_key TIMESTAMP, open FLOAT, close FLOAT, high FLOAT, low FLOAT, " \
                  "volume INT, turnover BIGINT, change_rate FLOAT, mid FLOAT, pre_close FLOAT)"
            connection.creat_table(stock_i_, sql)

            # 取得每日報價
            ret, data, page_req_key = quote_ctx.request_history_kline(stock_i, start='2015-01-01', end=current_time,
                                                                      max_count=1000)
            if ret == RET_OK:
                df = data
            else:
                print('error:', data)
            while page_req_key != None:
                ret, data, page_req_key = quote_ctx.request_history_kline(stock_i, start='2015-01-01', end=current_time,
                                                                          max_count=1000,
                                                                          page_req_key=page_req_key)
                if ret == RET_OK:
                    df = pd.concat([df, data])
                else:
                    print('error:', data)
            df = df.drop(['code', 'last_close'], axis=1)
            df = df.rename(columns={'time_key': 'date'})
            df['mid'] = (df['high'] + df['low']) / 2

            for i, row in df.iterrows():
                connection.data_input(stock_i_, 'Day', 11, tuple(row))

            time.sleep(0.5)

            # 取得每分鐘報價-全新
            ret, data, page_req_key = quote_ctx.request_history_kline(stock_i, ktype=KLType.K_1M, start='2020-01-01',
                                                                      end=current_time,
                                                                      max_count=1000)
            if ret == RET_OK:
                df = data
            else:
                print('error:', data)
            while page_req_key != None:
                ret, data, page_req_key = quote_ctx.request_history_kline(stock_i, ktype=KLType.K_1M,
                                                                          start='2020-01-01',
                                                                          end=current_time, max_count=1000,
                                                                          page_req_key=page_req_key)
                if ret == RET_OK:
                    df = pd.concat([df, data])
                else:
                    print('error:', data)
            df.drop(['code', 'pe_ratio', 'last_close', 'turnover_rate'], axis=1, inplace=True)
            df['mid'] = (df['high'] + df['low']) / 2
            df['pre_close'] = 0

            for i, row in df.iterrows():
                connection.data_input(stock_i_, 'Mins', 10, tuple(row))

            print('Created a new record for ', stock_i)
            time.sleep(0.5)
        else:  # 如果有舊紀錄則和舊紀錄合併
            df = connection.data_request(stock_i_, 'Day')

            lastDay = df['date'][len(df) - 1]
            lastDay += timedelta(days=1)
            ret, data, page_req_key = quote_ctx.request_history_kline(stock_i, start=str(lastDay), end=current_time,
                                                                      max_count=100)
            if ret == RET_OK:
                data = data.drop(['code', 'last_close'], axis=1)
                data = data.rename(columns={'time_key': 'date'})
                data['mid'] = (data['high'] + data['low']) / 2

                for i, row in data.iterrows():
                    connection.data_input(stock_i_, 'Day', 11, tuple(row))

                print('Added a new record for ', stock_i)
                time.sleep(0.5)

            # 取得每分鐘報價-加入到舊記錄

            df = connection.data_request(stock_i_, 'Mins')
            lastDay = df['time_key'][len(df) - 1]
            lastClose = df['close'][len(df) - 1]
            lastDay += timedelta(days=1)
            lastDay = str(lastDay)

            ret, data, page_req_key = quote_ctx.request_history_kline(stock_i, ktype=KLType.K_1M,
                                                                      start=lastDay, end=current_time,
                                                                      max_count=1000)
            if ret == RET_OK:
                df = data
            else:
                print('error:', data)
            while page_req_key != None:
                ret, data, page_req_key = quote_ctx.request_history_kline(stock_i, ktype=KLType.K_1M, start=lastDay,
                                                                          end=current_time, max_count=1000,
                                                                          page_req_key=page_req_key)
                if ret == RET_OK:
                    df = pd.concat([df, data])
                else:
                    print('error:', data)


            df['pre_close'] = lastClose
            df['mid'] = (df['high'] + df['low']) / 2
            df.drop(['code', 'pe_ratio', 'last_close', 'turnover_rate'], axis=1, inplace=True)
            for i, row in df.iterrows():
                connection.data_input(stock_i_, 'Mins', 10, tuple(row))

            time.sleep(0.5)

        if os.path.isfile('Ram/%s.csv' % (stock_i)):
            df_ex = pd.read_csv('Ram/%s.csv' % (stock_i), index_col=0)
            df_ex.drop_duplicates(subset=['sequence'], keep='first', inplace=True)
            df_ex.to_csv('Ram/%s.csv' % (stock_i))

            current_time_table = current_time.replace('-', '_')
            sql = "CREATE TABLE %s(code CHAR(8), time TIMESTAMP, price FLOAT, volume INT, turnover INT, " \
                  "ticker_direction CHAR(9), sequence BIGINT, type CHAR(21))" % (current_time_table)
            connection.creat_table(stock_i_, sql)

            for i, row in df_ex.iterrows():
                connection.data_input(stock_i_, current_time_table, 8, tuple(row))

    quote_ctx.close()

    gmail_create_draft('alphax.lys@gmail.com', 'collection', 'HK Data collection completed')


def ddcoll_US():
    watchlistUS = pd.read_csv('watchlistUS.csv')
    symbol = watchlistUS['Futu symbol'].tolist()
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    ddcoll(quote_ctx, symbol)



def packaging_loc_to_cloud():
    tRecord = 'Last Packaging Date.txt'
    current_time = str(datetime.now().date())
    t = open(tRecord, 'w')
    t.write(current_time)
    t.close()
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    watchlist1 = pd.read_csv('watchlistUS.csv')
    symbol = watchlist['Futu symbol'].tolist()
    symbol1 = watchlist1['Futu symbol'].tolist()
    symbol += symbol1

    if not os.path.isdir('Pack for Cloud'):
        os.mkdir('Pack for Cloud')

    for stock_i in symbol:
        if not os.path.isdir('Pack for Cloud/' + stock_i):
            os.mkdir('Pack for Cloud/' + stock_i)
        shutil.copyfile('Database/' + stock_i + '/Method Summary.csv',
                        'Pack for Cloud/' + stock_i + '/Method Summary.csv')


def distribution_on_cloud():
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    watchlist1 = pd.read_csv('watchlistUS.csv')
    symbol = watchlist['Futu symbol'].tolist()
    symbol1 = watchlist1['Futu symbol'].tolist()
    symbol += symbol1

    for i in symbol:
        try:
            os.replace('Pack for Cloud/' + i + '/Method Summary.csv', 'Database/' + i + '/Method Summary.csv')
        except:
            print('Error on ' + i)


def packaging_cloud_to_loc():
    if not os.path.isdir('Pack for Local'):
        os.mkdir('Pack for Local')

    tRecord = open('Last Packaging Date.txt', 'r')
    lastTimePack = tRecord.read()

    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    watchlist1 = pd.read_csv('watchlistUS.csv')
    symbol = watchlist['Futu symbol'].tolist()
    symbol1 = watchlist1['Futu symbol'].tolist()
    # symbol += symbol1

    for i in symbol:

        try:
            df = pd.read_csv('Database/' + i + '/' + i + '.csv', index_col=0)
            df = df[df['date'] > lastTimePack]
            df.to_csv('Pack for Local/' + i + '.csv')

            dfM = pd.read_csv('Database/' + i + '/' + i + '_min.csv', index_col=0)
            dfM = dfM[dfM['time_key'] > lastTimePack]
            dfM.to_csv('Pack for Local/' + i + '_min.csv')

            # shutil.copyfile('Database/' + i + '/' + i + '.csv', 'Pack for Local/' + i + '.csv')
            # shutil.copyfile('Database/' + i + '/' + i + '_min.csv', 'Pack for Local/' + i + '_min.csv')
        except:
            print('Error on ' + i)

    current_time = str(datetime.now())
    tRecord = open('Last Packaging Date.txt', 'w')
    tRecord.write(current_time)
    tRecord.close()


def distribution_on_loc():
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()
    # watchlist1 = pd.read_csv('watchlistUS.csv')
    # symbol1 = watchlist1['Futu symbol'].tolist()
    # symbol += symbol1

    for i in symbol:

        try:
            fPath = 'Database/' + i
            if not os.path.isdir(fPath):
                os.mkdir('Database/' + i)

            df = pd.read_csv('Database/' + i + '/' + i + '.csv', index_col=0)
            dfNew = pd.read_csv('Pack for Local/' + i + '.csv', index_col=0)
            df = pd.concat([df, dfNew])
            df.to_csv('Database/' + i + '/' + i + '.csv')

            dfM = pd.read_csv('Database/' + i + '/' + i + '_min.csv', index_col=0)
            dfMNew = pd.read_csv('Pack for Local/' + i + '_min.csv', index_col=0)
            dfM = pd.concat([dfM, dfMNew])
            dfM.to_csv('Database/' + i + '/' + i + '_min.csv')

            # os.replace('Pack for Local/' + i + '_min.csv', dPathMin)
            # os.replace('Pack for Local/' + i + '.csv', dPath)

        except:
            print('Error on ' + i)


def distribution_on_loc2():
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()

    for i in symbol:

        try:
            fPath = 'Database/' + i
            if not os.path.isdir(fPath):
                os.mkdir('Database/' + i)

            os.remove('Database/' + i + '/' + i + '_min.csv')
            shutil.copyfile('Pack for Local/' + i + '_min.csv', 'Database/' + i + '/' + i + '_min.csv')

        except:
            print('Error on ' + i)


def corActSch():
    print('Checking corporate action')
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    watchlist1 = pd.read_csv('watchlistUS.csv')
    symbol = watchlist['Futu symbol'].tolist()
    symbol1 = watchlist1['Futu symbol'].tolist()
    symbol += symbol1

    corActSch = pd.DataFrame(columns=['Futu Symbol', 'ex_div_date', 'adjustment0', 'adjustment1'])
    corActSym, corActEx, corActAdj0, corActAdj1 = [], [], [], []

    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    for stock_i in symbol:
        ret, corAction = quote_ctx.get_rehab(stock_i)
        if ret == RET_OK:
            corAction.to_csv('Database/' + stock_i + '/corporate action.csv')
            time.sleep(5)
            if len(corAction) > 0:
                exDivDate = corAction['ex_div_date'][len(corAction) - 1]
                exDivDate = datetime.strptime(exDivDate, '%Y-%m-%d')
                exDivDate = datetime.date(exDivDate)
                thisDay = datetime.now().date()
                if exDivDate >= thisDay:
                    corActSym.append(stock_i)
                    corActEx.append(exDivDate)
                    corActAdj0.append(corAction['forward_adj_factorA'][len(corAction) - 1])
                    corActAdj1.append(corAction['forward_adj_factorB'][len(corAction) - 1])
        else:
            print('error:', corAction)
    quote_ctx.close()

    corActSch['Futu Symbol'] = corActSym
    corActSch['ex_div_date'] = corActEx
    corActSch['adjustment0'] = corActAdj0
    corActSch['adjustment1'] = corActAdj1
    corActSch.to_csv('Database/corporate action schedule.csv')


def exDiv_Check():
    corActSch = pd.read_csv('Database/corporate action schedule.csv', index_col=1)
    divSymList = corActSch.index

    for stock_i in divSymList:
        thisDay = datetime.now().date()
        thisDay = str(thisDay)
        this_y, this_m, this_d = thisDay.split('-')
        exDate = corActSch['ex_div_date'][stock_i]
        ex_y, ex_m, ex_d = exDate.split('-')
        if this_y == ex_y and this_m == ex_m and this_d == ex_d:
            print('Ex-dividend for ', stock_i)
            exDiv(corActSch)
        else:
            print('Checked no ex-dividend action today')


def exDiv(corActSch):
    divSymList = corActSch.index
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')

    for stock_i in divSymList:
        df = pd.read_csv('Database/' + str(stock_i) + '/' + str(stock_i) + '_min.csv', index_col=0)
        adj0 = corActSch['adjustment0'][stock_i]
        adj1 = corActSch['adjustment1'][stock_i]

        df['open'] = df['open'] * adj0
        df['close'] = df['close'] * adj0
        df['high'] = df['high'] * adj0
        df['low'] = df['low'] * adj0
        df['last_close'] = df['last_close'] * adj0

        df['open'] = df['open'] + adj1
        df['close'] = df['close'] + adj1
        df['high'] = df['high'] + adj1
        df['low'] = df['low'] + adj1
        df['last_close'] = df['last_close'] + adj1

        df.to_csv('Database/' + str(stock_i) + '/' + str(stock_i) + '_min.csv')


def dailizeWithList(dataset):
    dateList = [x[:10] for x in dataset.index]
    dateList = list(set(dateList))
    dateList.sort()

    j = 0
    k = 1

    for i in range(len(dateList)):
        dayOpenTime = dataset.index[j]
        dayEndTime = dataset.index[k]
        try:
            while dataset.index[j][:10] == dataset.index[k][:10]:
                k += 1
        except:
            pass
        dfthatday = dataset[dataset.index[j]:dataset.index[k - 1]]
        j = k

        return dfthatday


def dailize(df, dayCount=50):
    df = df.set_index('time_key')
    j = 0
    k = 1

    for i in range(dayCount):
        dayOpenTime = df.index[j]
        dayEndTime = df.index[k]
        while df.index[j][:10] == df.index[k][:10]:
            k += 1
        dfthatday = df[df.index[j]:df.index[k - 1]]
        j = k + 1

        return dfthatday


def exclud_pre_after_market():
    watchlist = pd.read_csv('watchlistUS.csv')
    symbol = watchlist['Futu symbol'].tolist()

    for stock_i in symbol:
        df = pd.read_csv('Database/' + stock_i + '/' + stock_i + '_min.csv')
        df = df.drop(['Unnamed: 0', 'code', 'last_close'], axis=1)

        # 抽取指定日期內每日的每分鐘報價
        startDay = df['time_key'][1]
        startDay = startDay[:10]
        endDay = df['time_key'][len(df) - 1]
        endDay = endDay[:10]

        dayCount = datetime.strptime(endDay, '%Y-%m-%d') - datetime.strptime(startDay, '%Y-%m-%d')
        dayCount = dayCount.days

        df['time_key'] = pd.to_datetime(df['time_key'])
        df.set_index('time_key', inplace=True)
        # print(df.info())

        df0 = pd.DataFrame()
        formatDay0 = datetime.strptime(startDay, '%Y-%m-%d') + timedelta(hours=9, minutes=30)
        formatDay1 = datetime.strptime(startDay, '%Y-%m-%d') + timedelta(hours=16)
        for i in range(dayCount + 1):
            df0 = pd.concat([df0, df[formatDay0:formatDay1]])
            formatDay0 += timedelta(days=1)
            formatDay1 += timedelta(days=1)
        df0.to_csv('Database/' + stock_i + '/' + stock_i + '_min_excluded pre and after.csv')
        print(stock_i, ' Done')


def gmail_create_draft(con):
    creds, _ = google.auth.default()

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('gmail', 'v1', credentials=creds)
        message = EmailMessage()

        message.set_content(str(con))

        message['To'] = 'alphax.lys@gmail.com'
        message['From'] = 'origin.sunrise@gmail.com'
        message['Subject'] = 'Automated draft'

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {
            'raw': encoded_message
        }

        # pylint: disable=E1101
        send_message = (service.users().messages().send
                        (userId="me", body=create_message).execute())
        print(F'Message Id: {send_message["id"]}')

    except HttpError as error:
        print(F'An error occurred: {error}')
        send_message = None
    return send_message


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

if __name__ == '__main__':
    gmail_create_draft('check')
    '''

    connection = create_server_connection('103.68.62.116', 'root', '630A78e77?')
    cursor = connection.cursor()
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')

    symbol = watchlist['Futu symbol'].tolist()
    symbol_dict = {i: i.replace('.', '_') for i in symbol}   # 轉變成Dictionary

    for i in symbol_dict:
        try:
            sql = "USE %s" %(symbol_dict[i])
            cursor.execute(sql)

            sql = "DELETE FROM Mins WHERE time_key>='2023-03-07'"
            cursor.execute(sql)
            connection.commit()


        except: pass'''

