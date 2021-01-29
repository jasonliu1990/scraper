#!/usr/bin/env python3
# -*-coding: utf-8-*-
#
# Author: Chiata Liu
# Date: 20-Nov-2020

import warnings
warnings.filterwarnings('ignore')
import os
import re
import datetime
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

# 找出總共頁數建立url清單
def create_url_list(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
        'Host': 'www.travel.taipei',
        'Referer': 'https://www.travel.taipei/'
    }
    url_list = []   
    r = requests.get(url, verify=False, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    last_page_url = soup.find('a', attrs={'class': 'last-page'})['href']
    last_page = int(last_page_url[last_page_url.find('page=')+5:])
    tmp_url_1 = url[:url.find('page=')]
    for i in range(1, last_page + 1):
        tmp_url = tmp_url_1 + f'page={i}'
        url_list.append(tmp_url)
    return url_list
# 爬取每一頁所需資訊
def create_info_list(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
        'Host': 'www.travel.taipei',
        'Referer': 'https://www.travel.taipei/'
    }
    r = requests.get(url, verify=False, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    info_list = []
    for tmp_info in soup.findAll('div', attrs={'class': 'info-row-item'}):
        tmp_info_dict = {}
        tmp_info_dict['name'] = tmp_info.find('button')['data-name']
        tmp_info_dict['addr'] = tmp_info.find('button')['data-address'].replace(' ', '')
        tmp_info_dict['img'] = 'https://www.travel.taipei' + tmp_info.find('button')['data-img']
        try:
            tmp_info_dict['hits'] = tmp_info.find('button')['data-hits']
        except Exception as e:
            tmp_info_dict['hits'] = None
        try:
            tmp_info_dict['star'] = float(tmp_info.find('p', attrs={'class': 'tripadvisor-rating'}).find('img')['alt'])
        except Exception as e:
            tmp_info_dict['star'] = None
        try:
            tmp_info_dict['comment'] = int(re.sub('[^0-9]', '' , \
                                                  tmp_info.find('p', attrs={'class': 'tripadvisor-rating'}).find('span').text))
        except Exception as e:
            tmp_info_dict['comment'] = None
        info_list.append(tmp_info_dict)
    return info_list
# 整理出所需欄位
def create_final_df(df):
    tpe_dict = df[df['addr'].str.contains('臺北市')]['addr'].apply(lambda x: x[6:9]).unique()    
    df2 = pd.DataFrame(0, index=np.arange(len(df)), columns=['SNAP_DATE', 'DISTRICT', 'ITEM_TYPE', \
                                                             'C_NAME', 'E_NAME', 'GUIDE_TYPE', 'STAR', 'PIC'])
    df2['SNAP_DATE'] = datetime.datetime.today()
    df2['DISTRICT'] = df['addr'].apply(lambda x: x[6:9])
    df2['ITEM_TYPE'] = 'E'
    df2['C_NAME'] = df['name']
    df2['E_NAME'] = None
    df2['GUIDE_TYPE'] = None
    df2['STAR'] = df['star']
    df2['PIC'] = df['img']
    df2 = df2[df2['DISTRICT'].isin(tpe_dict)].reset_index(drop=True)
    return df2
