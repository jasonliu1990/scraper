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
import requests
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
    'Referer': 'https://guide.michelin.com/tw/zh_TW/restaurants/the-plate-michelin'
}
# 檢查該分類有幾頁, 並建立頁碼的url_list
def create_page_list(url):
    url_list = []
    r = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(r.text, 'html.parser')
    tot_page = soup.findAll('h1')[0].text.replace(' ', '').replace('/', '').split('\n')
    tot_page = [re.sub('[^0-9]', '', i) for i in tot_page if i.find('餐廳') > -1]
    tot_page = int(tot_page[0])
    pages = tot_page // 20 + 1
    for i in range(1, pages+1):
        tmp_url = url + f'/page/{i}'
        url_list.append(tmp_url)
    return url_list
# 每一頁的餐廳資訊及url
def create_res_url_list(url, headers=headers):
    r = requests.get(url, verify=False, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    res_url = soup.findAll('a', attrs={'class': 'link'})
    src_url = soup.findAll('a', attrs={'class': 'image-wrapper pl-image '})
    return res_url, src_url
# 爬取該餐廳的地址
def create_addr(url):
    r = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(r.text, 'html.parser')
    addr = soup.findAll('ul', attrs={'class': 'restaurant-details__heading--list'})[0]
    addr = addr.find('li').text
    return addr
# 建立dataframe, 包含上面function所產生的結果
def create_dataset(url, \
                   create_page_list=create_page_list, \
                   create_res_url_list=create_res_url_list, \
                   create_addr=create_addr):

    url_list = create_page_list(url)
    res_url_list = []
    src_url_list = []
    for url in url_list:
        tmp_list, tmp_list_2 = create_res_url_list(url)
        res_url_list.extend(tmp_list)
        src_url_list.extend(tmp_list_2)

    data = pd.DataFrame({
        'title_ch': [x['aria-label'][5:] for x in res_url_list],
        'title_eng': [x['href'].split('/')[-1] for x in res_url_list],
        'url': ['https://guide.michelin.com/' + x['href'] for x in res_url_list],
        'img': [x.find('img')['src'] for x in src_url_list]
    })
    
    return data
# 資料清洗並etl成入檔的格式
def data_clean(data, guide_type, create_addr=create_addr, star=0):
    data['addr'] = data['url'].apply(lambda x: create_addr(x))
    data['city'] = data['addr'].apply(lambda x: x.split(',')[-3].replace(' ', ''))
    data['dict'] = data['addr'].apply(lambda x: x[:3])
    data = data[data['city'] == 'Taipei'].reset_index(drop=True)
    data['snap_date'] = datetime.datetime.today()
    data['item_type'] = 'F'
    data['guide_type'] = guide_type
    data['star'] = star
    data = data[['snap_date', 'dict', 'item_type', 'title_ch', 'title_eng', 'guide_type', 'star', 'img']]
    data.columns = ['SNAP_DATE','DISTRICT', 'ITEM_TYPE','C_NAME','E_NAME','GUIDE_TYPE','STAR','PIC']
    
    return data
