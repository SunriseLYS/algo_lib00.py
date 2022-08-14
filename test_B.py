# -*- coding: utf-8 -*-
import datetime
import sys, time, os
import pandas as pd
import Techanalysis as ts
import itertools
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



aL = list(np.arange(0.03, 0.06, 0.01))
bL = list(np.arange(0.3, 0.6, 0.1))
cL = list(range(0, 4))

for i, j, k in itertools.product(aL, bL, cL):
    print(i, j, k)




