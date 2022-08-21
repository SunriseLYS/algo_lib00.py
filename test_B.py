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
    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()


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

    class BrokerTest(BrokerHandlerBase):

        def on_recv_rsp(self, rsp_pb):
            ret_code, err_or_stock_code, data = super(BrokerTest, self).on_recv_rsp(rsp_pb)
            if ret_code != RET_OK:
                print("BrokerTest: error")
                return RET_ERROR, data
            #print("BrokerTest: stock: {} data: {} ".format(err_or_stock_code, data))
            return RET_OK, data

    handler = OrderBookTest()
    handler1 = TickerTest()
    #handler2 = BrokerTest()
    quote_ctx.set_handler(handler, handler1)
    quote_ctx.subscribe(['HK.00700', 'HK.00005'], [SubType.ORDER_BOOK, SubType.TICKER])
    sleep(5)
    quote_ctx.close()



