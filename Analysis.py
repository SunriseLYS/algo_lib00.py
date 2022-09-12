# -*- coding: utf-8 -*-
import pandas as pd
import Techanalysis, Performance_Analysis
import os
from multiprocessing import Process, Pool
import mysql.connector
from mysql.connector import Error

watchlist = pd.read_csv('watchlist.csv', index_col = 0, encoding = 'Big5')
#watchlistUS = pd.read_csv('watchlistUS.csv', index_col = 0)
#Ticket = list(watchlist['Futu symbol']) + list(watchlistUS['Futu symbol'])
Ticket = list(watchlist['Futu symbol'])
Ticket = Ticket[:6]
Ticket1 = Ticket[:len(Ticket)//6]
Ticket2 = Ticket[len(Ticket)//6:len(Ticket)//6*2]
Ticket3 = Ticket[len(Ticket)//6*2:len(Ticket)//6*3]
Ticket4 = Ticket[len(Ticket)//6*3:len(Ticket)//6*4]
Ticket5 = Ticket[len(Ticket)//6*4:len(Ticket)//6*5]
Ticket6 = Ticket[len(Ticket)//6*5:len(Ticket)]

Job_Schedule = ['Techanalysis.SAR_Beta', 'Techanalysis.MFI_Beta', 'Techanalysis.MACD_Beta', 'Techanalysis.RSI_Beta', 'Techanalysis.BB_Beta']

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

if __name__ == '__main__' :
    #connection = create_server_connection('103.68.62.116', 'root', '630A78e77?')
    PL = Pool(6)
    for stock_i in Ticket:
        PL.apply_async(Techanalysis.beta, (stock_i, ))
        #PL.apply_async(Performance_Analysis.Performing, (stock_i,))
    PL.close()
    PL.join()

    print('Beta Completed')

