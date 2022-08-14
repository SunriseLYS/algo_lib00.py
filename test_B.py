# -*- coding: utf-8 -*-
import datetime
import sys, time, os
import pandas as pd
import Techanalysis as ts
from futu import *
import numpy as np
import mysql.connector
from mysql.connector import Error
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
pd.set_option('display.max_columns', 150)       #pandas setting 顥示列數上限
pd.set_option('display.width', 5000)           #pandas setting 顯示列的闊度
#pd.set_option('display.max_colwidth',20)      #pandas setting 每個數據顥示上限
pd.set_option('display.max_rows', 5000)       #pandas setting 顯示行的闊度
pd.options.mode.chained_assignment = None

quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

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


if __name__ == "__main__":


    i = 0
    while i < 1:
        ret_sub, err_message = quote_ctx.subscribe(['HK.00700', 'HK.00005'],
                                                   [SubType.QUOTE, SubType.ORDER_BOOK, SubType.TICKER, SubType.BROKER],
                                                   subscribe_push=False)
        if ret_sub == RET_OK:
            # ret, bid_frame_table, ask_frame_table = quote_ctx.get_broker_queue('HK.00700') -> SF行情
            ret, data = quote_ctx.get_stock_quote(['HK.00700'])
            if ret == RET_OK:
                print(data)
            else:
                print('error:', data)

            ret, data = quote_ctx.get_order_book('HK.00700', num=10)
            if ret == RET_OK:
                print(data)
            else:
                print('error:', data)

            ret, data = quote_ctx.get_rt_ticker('HK.00700', 1000)
            if ret == RET_OK:
                print(data)
            else:
                print('error:', data)

        else:
            print('subscription failed', err_message)

        i += 1

    quote_ctx.close()

