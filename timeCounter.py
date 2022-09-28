from datetime import datetime, time
from time import sleep
import pandas as pd
import timejob
import multiprocessing
import algo_lib


def timer_old():
    print("All start")
    while True:
        current_time = datetime.now().time()
        ToWeekday = datetime.weekday(datetime.now())

        if ToWeekday == 5 and time(14, 0) < current_time <= time(14, 0):
            timejob.corActSch()
            timejob.packaging_cloud_to_loc()
            print('Weekend Market off')
            sleep(151140)
        elif ToWeekday == 5 or ToWeekday == 6:
            sleep(21540)
        elif time(9, 30) < current_time <= time(9, 31):
            print('HK Market Process Open ', current_time)
            worker1 = multiprocessing.Process(target=timejob.market_check_HK)
            worker1.start()
        elif time(12, 15) < current_time <= time(12, 16):
            print('US data collection')
            worker2 = multiprocessing.Process(target=timejob.ddcoll_US)
            worker2.start()
        elif time(16, 15) < current_time <= time(16, 16):
            print('HK data collection')
            worker1 = multiprocessing.Process(target=timejob.ddcoll_HK)
            worker1.start()
        elif time(21, 30) < current_time <= time(21, 31):
            print('US Market Process Open ', current_time)
            worker2 = multiprocessing.Process(target=timejob.analysisUS)
            worker2.start()

        sleep(60)


def timer():
    print("All start")
    while True:
        current_time = datetime.now().time().replace(second=0, microsecond=0)
        ToWeekday = datetime.weekday(datetime.now())

        if ToWeekday == 5 and current_time == time(7, 0):
            print('Weekend Market off')
            sleep(172800)  # 48 hours
        elif current_time == time(9, 30):
            print('HK Market Process Open ', current_time)
            timejob.market_check_HK()
        '''
        elif current_time == time(16, 30):
            timejob.ddcoll_HK()
            print('HK data collection done')'''


def timer_test():
    print("All start")

    while True:
        current_time = datetime.now().time()
        ToWeekday = datetime.weekday(datetime.now())

        if time(9, 30) < current_time <= time(9, 31):
            algo_lib.tradingProgram_Future('HK.800000')

        sleep(60)


if __name__ == '__main__':
    timer()
