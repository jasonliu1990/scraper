#!/usr/bin/env python3
# -*-coding: utf-8-*-
#
# Author: Chiata Liu
# Date: 24-Nov-2020
#
# NCCC 信用卡公開資料
# url: https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/
# 每月月底更新
#
import warnings
warnings.filterwarnings('ignore')
import logging
import os
import shutil
import time
import datetime
from dateutil import relativedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from package import cc_opendata

# config
ENCODE = 'utf-8'
PROJECT = 'cc_opendata'
PATH = '/scraper/cc_opendata'
TMP_DIR = os.path.join(PATH, 'dataset')
OUTPUT_DIR = os.path.join(PATH, 'output')
LOG_DIR = os.path.join(PATH, 'log')
place_holder = 'PLACEHOLDER'
today = datetime.datetime.today()
today_yyyymmdd = today.strftime('%Y%m%d')
this_month = today.strftime('%Y%m')
last_month = (today + relativedelta.relativedelta(months=-1)).strftime('%Y%m')

# logging config
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(name)s  %(levelname)s  %(message)s',
                    datefmt='%m-%d %H:%M',
                    handlers = [logging.FileHandler(f'{LOG_DIR}/{PROJECT}_{today_yyyymmdd}.txt', 'a', 'utf-8'),])
# start
start = datetime.datetime.now() 
logging.info('START')
print('START')

# mv file
try:
    file_list = [f for f in os.listdir(OUTPUT_DIR) if os.path.isfile(os.path.join(OUTPUT_DIR, f))]
    for f in file_list:
        # shutil.move(os.path.join(OUTPUT_DIR, f), os.path.join(OUTPUT_DIR, 'his'))
        shutil.copy(os.path.join(OUTPUT_DIR, f), os.path.join(OUTPUT_DIR, 'his'))
        os.remove(os.path.join(OUTPUT_DIR, f))
    logging.info(f'move {len(file_list)} files')
    print(f'move {len(file_list)} files')
except Exception as e:
    logging.warning(f'mv file: {e}')  
    print(f'mv file: {e}')  

# url
# txn gender
url_1 = f'https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/Sexconsumption?WCM_PI=1&WCM_Page.bde3b591-d396-4179-9156-bc84ba6c998b={place_holder}&WCM_PageSize.bde3b591-d396-4179-9156-bc84ba6c998b=10'
# txn age
url_2 = f'https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/Ageconsumption?WCM_PI=1&WCM_Page.bde3b591-d396-4179-9156-bc84ba6c998b={place_holder}'
# ec
url_3 = 'https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/ECommerce'
#foreign
url_4 = f'https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/Top10ForeignCardConsumptionCount?WCM_PI=1&WCM_Page.aa5a95cd-0b6b-403f-bb0c-ee319cf16024={place_holder}'
url_5 = f'https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/Top10ForeignCardConsumptionPrice?WCM_PI=1&WCM_Page.aa5a95cd-0b6b-403f-bb0c-ee319cf16024={place_holder}'

if __name__ == '__main__':
    # txn gender
    try:    
        data_1 = cc_opendata.download_file(url_1, place_holder=place_holder)
    #     data_1.to_csv(f'{TMP_DIR}/cc_open_txn_{this_month}.csv', index=False)
        txn_df, txn_df_2 = cc_opendata.create_txn_df(data_1)
        txn_df.to_csv(f'{TMP_DIR}/cc_open_txn_gender.{today_yyyymmdd}', index=False)
        txn_df_2.to_csv(f'{TMP_DIR}/cc_open_txn.{today_yyyymmdd}', index=False)
        logging.info('txn gender ok')
        print('txn gender ok')
    except Exception as e:
        logging.error(f'part1: txn gender, error:{e}')
        print(f'part1: txn gender, error:{e}')

    # txn age
    try:   
        data_2 = cc_opendata.download_file(url_2, place_holder=place_holder)
    #     data_2.to_csv(f'{TMP_DIR}/cc_open_txn_age_{this_month}.csv', index=False)
        txn_age_df = cc_opendata.create_txn_age_df(data_2)
        txn_age_df.to_csv(f'{TMP_DIR}/cc_open_txn_age.{today_yyyymmdd}', index=False)
        logging.info('txn age ok')
        print('txn age ok')

    except Exception as e:
        logging.error(f'part2: txn age, error:{e}')
        print(f'part2: txn age, error:{e}')

    # ec
    try:
        data_3 = cc_opendata.download_file(url_3, place_holder=place_holder, ec=True)
    #     data_3.to_csv(f'{TMP_DIR}/cc_open_txn_ec_{this_month}.csv', index=False)
        txn_ec_df = cc_opendata.create_txn_ec_df(data_3)
        txn_ec_df.to_csv(f'{TMP_DIR}/cc_open_txn_ec.{today_yyyymmdd}', index=False)
        logging.info('ec ok')
        print('ec ok')
    except Exception as e:
        logging.error(f'part3: ec, error:{e}')
        print(f'part3: ec, error:{e}')

    # foreign
    try:
        data_4 = cc_opendata.download_file(url_4, place_holder=place_holder)
        data_4['order_by'] = 'txn_cnt'
    #     data_4.to_csv(f'{TMP_DIR}/cc_open_txn_foreign_cnt_{this_month}.csv', index=False)
        data_5 = cc_opendata.download_file(url_5, place_holder=place_holder)
        data_5['order_by'] = 'txn_amt'
    #     data_5.to_csv(f'{TMP_DIR}/cc_open_txn_foreign_cnt_{this_month}.csv', index=False)
        txn_foreign_df = cc_opendata.create_txn_foreign_df(pd.concat([data_4, data_5]))
        txn_foreign_df.to_csv(f'{TMP_DIR}/cc_open_txn_foreign.{today_yyyymmdd}', index=False)
        logging.info('foreign ok')
        print('foreign ok')
    except Exception as e:
        logging.error(f'part4: foreign, error:{e}')
        print(f'part4: foreign, error:{e}')

    # concat files
    try:
        txn_df['cate'] = 'gender_txn'
        txn_df = txn_df.drop(['file_title'], axis=1)
        txn_df = txn_df.rename(columns={'area': 'city'})
        txn_df_2['cate'] = 'total_txn'
        txn_age_df['cate'] = 'age_txn'
        txn_ec_df['cate'] = 'ec_txn'
        txn_ec_df = txn_ec_df.drop(['country'], axis=1)
        txn_foreign_df['cate'] =  'foreign_txn'
        final_df = pd.concat([txn_df,txn_df_2,txn_age_df,txn_ec_df,txn_foreign_df])
        final_df = final_df.where(final_df.notnull(), None)
        final_df = final_df[['cate', 'snap_yyyymm', 'city', 'mcc_group', 'txn_cnt', \
                             'txn_amt', 'gender', 'age', 'country', 'order_by']]
        final_df.to_csv(f'{OUTPUT_DIR}/cc_open.{today_yyyymmdd}', index=False)
        logging.info('concat ok')
        print('concat ok')
    except Exception as e:
        logging.error(f'final part: concat, error:{e}')
        print(f'final part: concat, error:{e}')

    end = datetime.datetime.now() 
    delta = str(end - start)
    logging.info(f'DONE, time:{delta}')
    print(f'DONE, time:{delta}')    
    logging.shutdown()   
