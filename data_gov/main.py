#!/usr/bin/env python3
# -*-coding: utf-8-*-
#
# Author: Chiata Liu
# Date: 11-DEC-2020
#
# 政府開放平台資料
# 涵蓋以下資料集:
#    1. 財政部稅籍登記資料
#    2. 董監事名單
#    3. 政府幾關, 非營利組織, 學校統編資訊
#
# 每月一號更新
import warnings
warnings.filterwarnings('ignore')
import logging
import os
import shutil
import pandas as pd
import numpy as np
import requests
from zipfile import ZipFile
import time
import datetime
from dateutil import relativedelta
# package
from package import data_gov

# config
ENCODE = 'utf-8'
PROJECT = 'data_gov'
PATH = '/scraper/data_gov'
TMP_DIR = os.path.join(PATH, 'dataset')
OUTPUT_DIR = os.path.join(PATH, 'output')
LOG_DIR = os.path.join(PATH, 'log')
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
# 全國營業(稅籍)登記資料集
corp_tax_url = 'https://eip.fia.gov.tw/data/BGMOPEN1.zip'
# 董監事資料集
director_url = 'https://data.gcis.nat.gov.tw/od/file?oid=7E5201D9-CAD2-494E-8920-5319D66F66A1'
# 政府機關
gov_url = 'https://www.fia.gov.tw/download/9bc4de1485014443b518beb37d8f35fe'
# 學校
edu_url = 'https://eip.fia.gov.tw/data/BGMOPEN99X.csv'
# 非營利組織
org_url = 'https://eip.fia.gov.tw/data/BGMOPEN99.csv'


if __name__ == '__main__':
    # create tmp folder
    if not os.path.exists('./tmp'):
        os.mkdir('tmp')
    # data_gov_corp_tax
    corp_tax_path = 'tmp/bgmopen1.zip'
    corp_tax_filename = 'data_gov_corp_tax'

    try:
        path = data_gov.download_corp_tax(corp_tax_url, corp_tax_path)
    except Exception as e:
        logging.error(f'part1, data_gov_corp_tax: download file, error:{e}')
        print(f'part1, data_gov_corp_tax: download file, error:{e}')
    else:
        try:
            data_gov_corp_tax = data_gov.clean_corp_tax(path[0])
        except Exception as e:
            logging.error(f'part1, data_gov_corp_tax: clean data, error:{e}')
            print(f'part1, data_gov_corp_tax: clean data, error:{e}')
        else:
            data_gov_corp_tax.to_csv(f'{OUTPUT_DIR}/{corp_tax_filename}.{today_yyyymmdd}', index=False)
    # data_gov_director
    director_path = 'tmp/director.zip'
    director_filename = 'data_gov_director'
    try:
        path = data_gov.download_director(director_url)
    except Exception as e:
        logging.error(f'part2, data_gov_director: download file, error:{e}')
        print(f'part2, data_gov_director: download file, error:{e}')
    else:   
        try:
            data_gov_director = data_gov.clean_director(path)
        except Exception as e:
            logging.error(f'part2, data_gov_director: clean data, error:{e}')
            print(f'part2, data_gov_director: clean data, error:{e}')
        else:
            data_gov_director.to_csv(f'{OUTPUT_DIR}/{director_filename}.{today_yyyymmdd}', index=False)
    # data_gov_edu_org
    gov_edu_org_filename = 'data_gov_edu_org'
    try:
        data_gov_edu_org = data_gov.clean_concat_org(gov_url, edu_url, org_url)
    except Exception as e:
        logging.error(f'part3, data_gov_edu_org error:{e}')
        print(f'part3, data_gov_edu_org error:{e}')
    else:
        data_gov_edu_org.to_csv(f'{OUTPUT_DIR}/{gov_edu_org_filename}.{today_yyyymmdd}', index=False)
    # del tmp folder
    path_tmp = './tmp'
    try:
        shutil.rmtree(path_tmp)
    except Exception as e:
        logging.error(f'please check error: {e}')
        print(f'please check error: {e}')
    else:
        # end
        end = datetime.datetime.now() 
        delta = str(end - start)
        logging.warning(f'done, used time:{delta}')
        print(f'done, used time:{delta}')
        logging.shutdown()
