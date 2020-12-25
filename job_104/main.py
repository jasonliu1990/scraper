#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# 爬取104刊登資訊
# 網址: https://www.104.com.tw
# 欄位: 'comp_name', 'location', 'industry', 'capital', 'employee',
#       'appear_date': 職缺最後看刊登日, 'job', 'snap_date'
#
# 爬取104上的公司職缺數量
#  
# Author: Chiata Liu
# Date: 14-Dec-2020

import os
# logging config
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import datetime
import re
import warnings
warnings.filterwarnings('ignore')
import time
import http.client
http.client._MAXLINE = 655360
from package import job104
# config
ENCODE = 'utf-8'
PROJECT = 'job104'
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
                    
# set cates
ind_list = ['1003001000','1003002000','1005001000','1005002000','1005003000','1006001000',
            '1006002000','1006003000','1007001000','1009001000','1009002000','1009004000',
            '1009005000','1009006000','1009007000','1001001000','1001002000','1001003000',
            '1001004000','1001005000','1001006000','1002001000','1002002000','1002003000',
            '1002004000','1002005000','1002006000','1002007000','1002008000','1002009000',
            '1002010000','1002011000','1002012000','1002013000','1002014000','1002015000',
            '1002016000','1002017000','1014001000','1014002000','1014003000','1014004000',
            '1010001000','1010002000','1010003000','1008001000','1008002000','1008003000',
            '1011001000','1011002000','1011003000','1012001000','1012002000','1015001000',
            '1015002000','1015003000','1016001000','1016002000',           
           ]

# set headers
headers = {}
original_headers_str = """Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7
Cache-Control: max-age=0
Connection: keep-alive
Cookie: _hjid=bde5a4fd-11b2-4a85-9536-b0b9ce3cca88; __auc=fcc1de7e16b7457272ec6da75ec; luauid=348264616; _gid=GA1.3.1142716462.1577931661; bprofile_history=%5B%7B%22key%22%3A%22wi2uotk%22%2C%22custName%22%3A%22%E5%92%8C%E9%91%AB%E5%85%89%E9%9B%BB%E8%82%A1%E4%BB%BD%E6%9C%89%E9%99%90%E5%85%AC%E5%8F%B8%22%2C%22custLink%22%3A%22https%3A%2F%2Fwww.104.com.tw%2Fcompany%2Fwi2uotk%22%7D%5D; cust_same_ab=1; lup=348264616.4702989186930.5392331437617.1.4640712161167; lunp=5392331437617; mode=s; _ga=GA1.1.647183600.1561024998; TS016ab800=01180e452d37fb650f5b4feea5ba5528137b5b698581d7eb3cb6d361201c4fc393996051957c0b2a7a180e22787ffc5129eb9a09501e187a7c2276b8e0189037a386569428; _ga_W9X1GB1SVR=GS1.1.1577945728.4.1.1577945877.46
Host: www.104.com.tw
Sec-Fetch-Mode: navigate
Sec-Fetch-Site: none
Sec-Fetch-User: ?1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36"""
for d in original_headers_str.split('\n'):
    headers[d.split(': ')[0]] = d.split(': ')[1].strip()

# init
result_df = pd.DataFrame()
company_df = pd.DataFrame()
url_list = job104.create_url_list(ind_list)                    
# start scraper
for i, url in enumerate(url_list):    
    try:
        tmp_df = job104.create_comp_df(url, headers)
        tmp_df = tmp_df[~tmp_df['url'].isnull()].reset_index(drop=True)
        target_url_list = tmp_df['url']
        refer_url_list = tmp_df['refer_url']
        target_id_list = tmp_df['target_id']
        results_list = []
        time.sleep(1)
              
        for target_url, refer_url, target_id in zip(target_url_list, refer_url_list, target_id_list):
            if url is not None:
                try:
                    job, appear_date = job104.create_job(target_url, refer_url)
                    results = {}
                    results['target_id'] = target_id
                    results['url'] = target_url
                    results['job'] = job
                    results['appear_date'] = appear_date
                    results_list.append(results)
                except Exception as e:
                    try:
                        time.sleep(60)
                        job, appear_date = job104.create_job(target_url, refer_url)
                        results = {}
                        results['target_id'] = target_id
                        results['url'] = target_url
                        results['job'] = job
                        results['appear_date'] = appear_date
                        results_list.append(results)
                    except Exception as e:
                        logging.error(f'phase2 error:{e} in {url}')

        company_df = pd.concat([company_df, tmp_df])
        result_df = pd.concat([result_df, pd.DataFrame(results_list)])

        if i % 10 == 0:
            time.sleep(10)
            #company_df.to_csv(f'company_df_{today}_tmp.csv', index=False)
            #result_df.to_csv(f'result_df_{today}_tmp.csv', index=False)
            #print(f'{i} / {len(url_list)}')  
    except Exception as e:

        logging.error(f'phase1 error:{e} in {url}')
# merge df & clean files    
company_df_clean = company_df[~company_df['url'].isnull()].reset_index(drop=True)
result_df_clean = result_df[~result_df['url'].isnull()].reset_index(drop=True)
final_df = pd.merge(company_df_clean, result_df_clean, on=['target_id', 'url'])
final_df['snap_date'] = today
final_df['appear_date'] = final_df['appear_date'].apply(lambda x: datetime.datetime \
                                                                          .strptime(year+'/'+x, "%Y/%m/%d") \
                                                                          .strftime('%Y%m%d') if x is not None else None)
final_df = final_df.drop(['url', 'refer_url', 'target_id'], axis=1)
final_df.to_csv(f'./{OUTPUT_DIR}/job104_{this_month}.csv', index=False)              
# end
end = datetime.datetime.now() 
delta = str(end - start)
logging.info(f'DONE, time:{delta}')
logging.shutdown()    

                   
