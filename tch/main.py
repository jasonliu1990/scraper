#!/usr/bin/env python3
# -*-coding: utf-8-*-
#
# Author: Chiata Liu
# Date: 24-Nov-2020
# 公庫處: 北市府公開資料
#
# 本程式更新資料:
# 1. revenue
# 2. population
# 3. food play
import warnings
warnings.filterwarnings('ignore')
import logging
import os
import re
import datetime
from dateutil import relativedelta
import pdfplumber
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import unquote
import shutil
# package
from package import d04_play, d04_food, d04_population, d04_download_img
from package import d03_revenue
# args
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-f", "--foodplay", required=False,
	help="download food play or not;(y/n)")
args = vars(ap.parse_args())

# config
ENCODE = 'utf-8'
PROJECT = 'tch'
PATH = '/scraper/tch'
# PATH = './'
TMP_DIR = os.path.join(PATH, 'dataset')
OUTPUT_DIR = os.path.join(PATH, 'output')
LOG_DIR = os.path.join(PATH, 'log')
IMG_DIR = os.path.join(OUTPUT_DIR, 'SHAREIMG')
today = datetime.datetime.today()
today_yyyymmdd = today.strftime('%Y%m%d')
this_month = today.strftime('%Y%m')
last_month = (today + relativedelta.relativedelta(months=-1)).strftime('%Y%m')
# mkdir
for d in [TMP_DIR, OUTPUT_DIR, LOG_DIR, IMG_DIR]:
    if not os.path.exists(d):
        os.mkdir(d)
        
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
    
# url pool
# monthly
popu_url = 'https://ca.gov.taipei/News_Content.aspx?n=8693DC9620A1AABF&sms=D19E9582624D83CB&s=E70E0ADF8510073C'
source_url = 'https://data.taipei/api/dataportal/get-dataset-detail?id=09b12b01-c562-4a0e-9aeb-93312ce57372'
dept_url = 'https://dof.gov.taipei/News.aspx?n=4D21C7BE105121C3&sms=CB3C416EA7B4104E'

# prn
# food
food_url_1 = 'https://guide.michelin.com/tw/zh_TW/restaurants/1-star-michelin' # 一星
food_url_2 = 'https://guide.michelin.com/tw/zh_TW/restaurants/2-stars-michelin' # 二星
food_url_3 = 'https://guide.michelin.com/tw/zh_TW/restaurants/3-stars-michelin' # 三星
food_url_4 = 'https://guide.michelin.com/tw/zh_TW/restaurants/bib-gourmand' # 富比登
food_url_5 = 'https://guide.michelin.com/tw/zh_TW/restaurants/the-plate-michelin' # 餐盤
# play
play_url = 'https://www.travel.taipei/zh-tw/attraction/all-regions?mode=list&sortby=tripadvisor&page=1'

if __name__ == '__main__':
    # part1: TCH_POPULATION
    try:
        popu_param = d04_population.download_file(popu_url, TMP_DIR)
        popu_df = d04_population.create_dataset(*popu_param)
        popu_df.to_csv(f'{OUTPUT_DIR}/TCH_POPULATION.{popu_param[2]}', 
                          index=False)
    except Exception as e:
        logging.error(f'part1: TCH_POPULATION, error:{e}')
        print(f'part1: TCH_POPULATION, error:{e}')

    # part2: TCH_REVENUE
    try:
        rev_snap_yyyymmdd = d03_revenue.download_data(source_url, dept_url, TMP_DIR)
        # check file 1 & file 2 snap date
        if rev_snap_yyyymmdd[0] == rev_snap_yyyymmdd[1]:
            final_df = d03_revenue.create_dataset(TMP_DIR, rev_snap_yyyymmdd[0], rev_snap_yyyymmdd[2])
        else:
            print(rev_snap_yyyymmdd[0])
            print(rev_snap_yyyymmdd[1])
        final_df.to_csv(f'{OUTPUT_DIR}/TCH_REVENUE.{rev_snap_yyyymmdd[2]}', index=False)
    except Exception as e:
        logging.error(f'part2: TCH_REVENUE, error:{e}')
        print(f'part2: TCH_REVENUE, error:{e}')

    # part6: TCH_FOOD_PLAY
    # FOOD 米其林
    if args['foodplay'] == 'y':
        print(f"food pay: {args['foodplay']}")
        try:
            food_url_list = [food_url_1,food_url_2,food_url_3,food_url_4,food_url_5]
            condition = [('STAR', 1), ('STAR', 2), ('STAR', 3), ('FORK', 0), ('PLATE', 0)]
            food = pd.DataFrame()
            for url, (guide_type, star) in zip(food_url_list, condition):
                try:
                    data = d04_food.create_dataset(url)
                except Exception as e:
                    logging.error(f'part6: TCH_FOOD, error: {e}, phase 1, {url}')
                    print(f'part6: TCH_FOOD, error: {e}, phase 1, {url}')
                try:
                    tmp_df = d04_food.data_clean(data, guide_type=guide_type, star=star)
                except Exception as e:
                    logging.error(f'part6: TCH_FOOD, error: {e}, phase 2, {url}')
                    print(f'part6: TCH_FOOD, error: {e}, phase 2, {url}')
                food = pd.concat([food, tmp_df])
            food = food.reset_index(drop=True)
        except Exception as e:
            logging.error(f'part6: TCH_FOOD, error:{e}')
            print(f'part6: TCH_FOOD, error:{e}')
        try:
            # PLAY 景點
            play_url_list = d04_play.create_url_list(play_url)
            info_df = pd.DataFrame()
            for url in play_url_list:
                try:
                    info_list = d04_play.create_info_list(url)
                    info_df = pd.concat([info_df, pd.DataFrame(info_list)])
                    info_df = info_df[list(info_list[0].keys())].reset_index(drop=True)
                except Exception as e:
                    logging.error(f'part6: TCH_PLAY, error: {e}, {url}')
                    print(f'part6: TCH_PLAY, error: {e}, {url}')
            info_df = info_df.where(info_df.notnull(), None)
            play = d04_play.create_final_df(info_df)    
        except Exception as e:
            logging.error(f'part6: TCH_PLAY, error:{e}')
            print(f'part6: TCH_PLAY, error:{e}')

        # combined food & play 
        # download img
        try:
            tch_food_play = pd.concat([food, play]).reset_index(drop=True)
            tch_food_play = d04_download_img.rebuild_dataset(tch_food_play, IMG_DIR, today_yyyymmdd)
            tch_food_play['PIC'] = tch_food_play['PIC'].apply(lambda x: x.replace('./output', ''))    
            tch_food_play['SNAP_DATE'] = tch_food_play['SNAP_DATE'].apply(lambda x: datetime.datetime.strptime(x.strftime('%Y%m%d'),'%Y%m%d'))
            tch_food_play['STAR'] = tch_food_play['STAR'].astype(float)
            # 中英文名稱格式變更 20201224
            tch_food_play['C_NAME'] = tch_food_play['C_NAME'].apply(lambda x: x.upper() if x.replace(' ', '').isalpha() else x)
            tch_food_play['E_NAME'] = tch_food_play['E_NAME'].fillna('')
            tch_food_play['E_NAME'] = tch_food_play['E_NAME'].apply(lambda x: x.replace('-', ' ').capitalize())
            tch_food_play['E_NAME'] = tch_food_play['E_NAME'].apply(lambda x: None if x == '' else x) 
            # 處理編碼問題 20210114
            tch_food_play['E_NAME'] = tch_food_play['E_NAME'].apply(lambda x: unquote(x, 'utf-8') if x is not None else None)            
            tch_food_play = tch_food_play.where(tch_food_play.notnull(), None)
            tch_food_play.to_csv(f'{OUTPUT_DIR}/TCH_FOOD_PLAY.{today_yyyymmdd}',
                                 index=False,
                                 encoding=ENCODE)
        except Exception as e:
            logging.error(f'part6: TCH_PLAY, error:{e}')
            print(f'part6: TCH_PLAY, error:{e}')

    end = datetime.datetime.now() 
    delta = str(end - start)
    logging.info(f'DONE, time:{delta}')
    print(f'DONE, time:{delta}')
    logging.shutdown()    
