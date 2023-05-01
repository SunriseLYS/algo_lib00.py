from datetime import datetime, time
from time import sleep
import timejob
import multiprocessing
import algo_lib


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
        elif current_time == time(8, 0) and 1 < ToWeekday < 6:
            print('US collect', current_time)
            timejob.ddcoll_US()

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
