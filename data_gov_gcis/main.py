#!/usr/bin/env python3
# -*-coding: utf-8-*-
# 
# 公司設立登記/變更/解散清冊
# 商業設立登記/變更/解散清冊:
#
# Author: Chiata Liu
# Date: 14-Dec-2020
import os
import shutil
import warnings
warnings.filterwarnings('ignore')
# logging config
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import datetime
from dateutil import relativedelta
# package
from package import gcis

# config
ENCODE = 'utf-8'
PROJECT = 'gcis'
TMP_DIR = './dataset/'
OUTPUT_DIR = './output/'
LOG_DIR = './log/'
today = datetime.datetime.today()
today_yyyymmdd = today.strftime('%Y%m%d')
this_month = today.strftime('%Y%m')
last_month = (today + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
once = True
# logging config
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(name)s  %(levelname)s  %(message)s',
                    datefmt='%m-%d %H:%M',
                    handlers = [logging.FileHandler(f'{LOG_DIR}{PROJECT}_{today_yyyymmdd}.txt', 'a', 'utf-8'),])
# start
start = datetime.datetime.now() 
logging.info('START')
try:
    file_list = [f for f in os.listdir(f'./OUTPUT') if os.path.isfile(os.path.join(OUTPUT_DIR, f))]
    for f in file_list:
        shutil.move(os.path.join(OUTPUT_DIR, f), os.path.join(OUTPUT_DIR, 'his'))
    logging.info(f'move {len(file_list)} files')
except Exception as e:
    logging.warning(f'mv file: {e}')  
# url
# 公司設立登記清冊
url_1 = 'https://data.gcis.nat.gov.tw/od/detail?oid=AD28285B-7B0E-4241-9F58-F2F0F289333E#moreBtn'
# 公司變更登記清冊(月份)
url_2 = 'https://data.gcis.nat.gov.tw/od/detail?oid=75353060-3C3D-453E-8E5C-4ADDEAA8260F#moreBtn'
# 公司解散登記清冊(月份)
url_3 = 'https://data.gcis.nat.gov.tw/od/detail?oid=4302E583-A7B5-4BE2-A3D6-9707B1AACE1C#moreBtn'
# 商業設立登記清冊(月份)
url_4 = 'https://data.gcis.nat.gov.tw/od/detail?oid=B03FD818-6483-4F04-88BE-B7B18068A842#moreBtn'
# 商業變更登記清冊(月份)
url_5 = 'https://data.gcis.nat.gov.tw/od/detail?oid=EE1F22A1-CC3D-465E-83DF-016488A9AD10#moreBtn'
# 商業變更登記清冊(月份)
url_6 = 'https://data.gcis.nat.gov.tw/od/detail?oid=136231C9-362B-4D30-BDC4-EC90DCA5DEF1#moreBtn'

url_list = [eval(f'url_{i}') for i in range(1,7)]
data_list = []
for url in url_list:
    try:
        file_list = gcis.create_file_list(url)
        data = gcis.download_data(file_list, once=once)
        data_list.append(data)
    except Exception as e:
        logging.error(f'part1: download, error:{e}')
s_list = ['設立','變更','解散','設立','變更','歇業']
for i, s in zip(range(6), s_list):
    try:
        data_list[i]['status'] = s    
    except Exception as e:
        logging.error(f'part2-1: add status, error:{e}')

company_reg = pd.DataFrame()
commercial_reg = pd.DataFrame()
for i, df in enumerate(data_list):
    try:
        if i < 3:
            df = gcis.transfer_data(df, cate='company')
            company_reg = pd.concat([company_reg, df])
            company_reg = company_reg.reset_index(drop=True)
        else:
            df = gcis.transfer_data(df, cate='commercial')
            commercial_reg = pd.concat([commercial_reg, df])
            commercial_reg = commercial_reg.reset_index(drop=True)            
    except Exception as e:
        logging.error(f'part2-2: transfer data, error:{e}')

try:
    company_reg = gcis.clean_data(company_reg)
    commercial_reg = gcis.clean_data(commercial_reg)
except:
    logging.error(f'part3: clean data, error:{e}')
else:
    company_reg.to_csv(f'./{OUTPUT_DIR}/company_reg_{this_month}.csv', index=False)
    commercial_reg.to_csv(f'./{OUTPUT_DIR}/commercial_reg_{this_month}.csv', index=False)
end = datetime.datetime.now() 
delta = str(end - start)
logging.info(f'DONE, time:{delta}')
logging.shutdown()    
