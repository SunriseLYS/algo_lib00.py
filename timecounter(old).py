from datetime import datetime, time
from time import sleep
from futu import OpenQuoteContext
import timejob
import multiprocessing

Morning_Start = time(9, 31)
Morning_End = time(9, 32)

Morning_Start1 = time(10, 31)
Morning_End1 = time(10, 32)

Mid_Start = time(12, 1)
Mid_End = time(12, 2)

Mid1_Start = time(13, 30)
Mid1_End = time(13, 31)

Afternoon_Start = time(14, 30)
Afternoon_End = time(14, 31)

Afternoon_Start1 = time(16, 11)
Afternoon_End1 = time(16, 12)

packaging_start = time(8, 0)
packaging_end = time(8, 1)

def timer():
    print("All start")

    while True:
        current_time = datetime.now().time()
        ToWeekday = datetime.weekday(datetime.now())

        if ToWeekday == 5 or ToWeekday == 6:
            if ToWeekday == 5 and packaging_start <= current_time <= packaging_end:
                timejob.packaging_cloud_to_loc()
            print('Weekend Market-off', ToWeekday)
            sleep(21540)
            continue
        elif Morning_Start <= current_time <= Morning_End:
            worker1 = multiprocessing.Process(target = timejob.emailnotice)
            worker1.start()
        elif Morning_Start1 <= current_time <= Morning_End1:
            worker1 = multiprocessing.Process(target = timejob.emailnotice)
            worker1.start()
        elif Mid_Start <= current_time <= Mid_End:
            worker1 = multiprocessing.Process(target = timejob.emailnotice)
            worker1.start()
        elif Mid1_Start <= current_time <= Mid1_End:
            worker1 = multiprocessing.Process(target=timejob.emailnotice)
            worker1.start()
        elif Afternoon_Start <= current_time <= Afternoon_End:
            worker1 = multiprocessing.Process(target = timejob.ddcoll)
            worker1.start()
        elif Afternoon_Start1 <= current_time <= Afternoon_End1:
            worker1 = multiprocessing.Process(target = timejob.ddcoll)
            worker1.start()

        sleep(60)

if __name__ == '__main__':
    worker0 = multiprocessing.Process(target = timer)
    worker0.start()
