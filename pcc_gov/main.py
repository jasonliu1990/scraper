#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# 透過 ronny所開的 API將每日決標訊息爬下來
# 網址: https://ronnywang.github.io/pcc-viewer/index.html 
# 欄位有 公司統編, 公司名稱, 公司電話, 決標金額, 公告日期
# 這包資料將進一步合併稅籍資料, 提供政府採購註記, 以及電話號碼
#
# Author: Chiata Liu
# Date: 23-SEP-2019
import os
import logging
import warnings
warnings.filterwarnings('ignore')
import requests
from bs4 import BeautifulSoup
import time
import json 
import datetime
import pandas as pd
import numpy as np
import random
import time
from dateutil import relativedelta
# import package
from package import pcc_gov
# config
ENCODE = 'utf-8'
PROJECT = 'pcc_gov'
TMP_DIR = './dataset/'
OUTPUT_DIR = './output/'
LOG_DIR = './log/'
today = datetime.datetime.today()
today_yyyymmdd = today.strftime('%Y%m%d')
this_month = today.strftime('%Y%m')
last_month = (today + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
year = datetime.datetime.now().strftime('%Y')
# logging config
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(name)s  %(levelname)s  %(message)s',
                    datefmt='%m-%d %H:%M',
                    handlers = [logging.FileHandler(f'{LOG_DIR}{PROJECT}_{today_yyyymmdd}.txt', 'a', 'utf-8'),])
# start
start = datetime.datetime.now() 
logging.info('START')
# mv file
try:
    file_list = [f for f in os.listdir(f'./OUTPUT') if os.path.isfile(os.path.join(OUTPUT_DIR, f))]
    for f in file_list:
        shutil.move(os.path.join(OUTPUT_DIR, f), os.path.join(OUTPUT_DIR, 'his'))
    logging.info(f'move {len(file_list)} files')
except Exception as e:
    logging.warning(f'mv file: {e}')  
    
# 過去一週
date_list = []
for i in range(3, 10):
    date_list.append((datetime.datetime.now() - datetime.timedelta(days=today.weekday() + i)).strftime('%Y%m%d'))
# 取得每天的資料
comp_df_all = pd.DataFrame()
for date in date_list:
    try:
        comp_df_all = pd.concat([comp_df_all, pcc_gov.pcc_gov(date)])
    except Exception as e:
        logging.error(f'part1: {e}')        
try:        
    comp_df = comp_df_all.copy()
    comp_df_cln = pcc_gov.clean_data(comp_df)
    comp_df = comp_df.reset_index(drop=True)
    comp_df_cln = comp_df_cln.reset_index(drop=True)
except Exception as e:
    logging.error(f'part2: {e}')
    
# 寫檔
comp_df_cln.to_csv(f'./{OUTPUT_DIR}/pcc_gov_{today_yyyymmdd}.csv', index=False)
   
# end
end = datetime.datetime.now() 
delta = str(end - start)
logging.info(f'DONE, time:{delta}')
logging.shutdown()   
    
