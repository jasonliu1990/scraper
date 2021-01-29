#!/usr/bin/env python3
# -*-coding: utf-8-*-
#
# Author: Chiata Liu
# Date: 20-Nov-2020

import warnings
warnings.filterwarnings('ignore')
#logging
import os
import re
import json
import datetime
import pdfplumber
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

# 下載檔案
def download_data(source_url, dept_url, foldername):

    # download data 1 and write file
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'}
    # request 1
    try:
        r = requests.get(source_url, verify=False, headers=headers)
        data = json.loads(r.text)
        filename = data['resources'][0]['resourceName']
        yyyy = str(int(filename.split('年')[0]) + 1911)
        mm = filename.split('年')[1].split('月')[0]
        snap_yyyymm = yyyy + mm
        snap_yyyymm = datetime.datetime.strptime(snap_yyyymm, '%Y%m').strftime('%Y%m')
        # source_url = data['resources'][0]['download']
        source_url = [x for x in data['resources'] if x['resourceNarrative '].find('來源別') > 0][0]['download']
        tmp_snap_date = datetime.datetime.strptime(snap_yyyymm, '%Y%m') 
        snap_yyyymmdd = tmp_snap_date + relativedelta(months=1) + relativedelta(days=-1)
        snap_yyyymmdd = datetime.datetime.strftime(snap_yyyymmdd, '%Y%m%d')    
        # request 2
        url_2 = f'https://data.taipei{source_url}'
        r_2 = requests.get(url_2, verify=False)
        with open(f'{foldername}/D03_REV_SOURCE_{snap_yyyymm}.csv', 'wb') as f:
            f.write(r_2.content)
    except Exception as e:
        print(f'data 1 failed: {e}')
    # download data 2 and write file  
    try:
        r = requests.get(dept_url, verify=False)
        soup = BeautifulSoup(r.text)
        data_list = soup.findAll('td', attrs={'class': 'CCMS_jGridView_td_Class_1'})
        tmp_dt = soup.find('td', attrs={'headers': 'CCMS_jGridView_TH_3'}).span.text
        yyyy = str(int(tmp_dt.split('-')[0]) + 1911)
        mm = tmp_dt.split('-')[1]
        snap_yyyymm_2 = yyyy + mm
        snap_yyyymm_2 = datetime.datetime.strptime(snap_yyyymm_2, '%Y%m') + relativedelta(months=-1)
        snap_yyyymm_2 = datetime.datetime.strftime(snap_yyyymm_2, '%Y%m')    
        for d in data_list:
            if d.find('a')['title'].find('主管別') > -1:
                source_url = d.find('a')['href']
                source_title = d.find('a')['title']
        r_2 = requests.get(source_url, verify=False)
        with open(f'{foldername}/D03_REV_DEPT_{snap_yyyymm}.pdf', 'wb') as f:
            f.write(r_2.content)
    except Exception as e:
        print(f'data 2 failed: {e}')
        
    return (snap_yyyymm, snap_yyyymm_2, snap_yyyymmdd)
# 兩段ETL將兩張表合併並轉成入檔的格式
def create_dataset(foldername, snap_yyyymm, snap_yyyymmdd):
    try:
        data = pd.read_csv(f'{foldername}/D03_REV_SOURCE_{snap_yyyymm}.csv', 
                           engine='python', 
                           encoding='big5',
                           skiprows=[0, 1],
                           header=None
                          )
        data = data.dropna(how='all', axis=1)
        data = data.dropna(how='all', axis=0)
        data = data.iloc[:-4, :]
        col_list = []
        for i in range(data.shape[1]):
            tmp_str = ''
            for j in range(3):
                tmp_str += str(data.iloc[j, i]).replace(' ', '')
            col_list.append(tmp_str)
        col_list = [c.replace('nan', '') for c in col_list]        
        data.columns = col_list
        data = data.iloc[3:, :]
        data['資料年月'] = snap_yyyymmdd
        data['ITEM_TYPE'] = '來源別'
        data = data[['資料年月', 'ITEM_TYPE', '本年度部分科目', '全年度預算數A', '本月分配數', '本月實收數', '分配預算累計數B',
                    '實收累計數C','上年度同期實收累計數D','以前年度部分收入數c']]
        data['本年度部分科目'] = data['本年度部分科目'].apply(lambda x: x.replace('\u3000', ''))
        data = data.fillna(0).reset_index(drop=True)
        for c in data.columns[3:]:
            data[c] = data[c].apply(lambda x: int(str(x).replace(',', '')))
        data.columns = ['SNAP_DATE', 'ITEM_TYPE', 'ITEM','Plan_Year','Allocate_Month',
                         'Actual_month','Allocate_Accu','Actual_accu','YOY','LY_Income']
    except Exception as e:
        try:
            data = pd.read_csv(f'{foldername}/D03_REV_SOURCE_{snap_yyyymm}.csv', 
                           engine='python', 
                           encoding='big5',

                          )
            data = data.dropna(how='all', axis=1)
            data = data.dropna(how='all', axis=0)
            data = data.iloc[:-4, :]
            data['資料年月'] = snap_yyyymmdd
            data['ITEM_TYPE'] = '來源別'
            data = data[['資料年月', 'ITEM_TYPE', '科目', '全年度預算數_A', '本月分配數', '本月實收數', '分配預算累計數_B',
                        '實收累計數_C','上年度同期實收累計數_D','收入數_c']]
            data['科目'] = data['科目'].apply(lambda x: x.replace('\u3000', ''))
            data = data.fillna(0).reset_index(drop=True)
            for c in data.columns[3:]:
                data[c] = data[c].apply(lambda x: int(str(x).replace(',', '')))
            data.columns = ['SNAP_DATE', 'ITEM_TYPE', 'ITEM','Plan_Year','Allocate_Month',
                             'Actual_month','Allocate_Accu','Actual_accu','YOY','LY_Income']
        except Exception as e:  
            print(f'{e}: phase 1 ETL')
    # etl 2
    try:
        pdf = pdfplumber.open(f'{foldername}/D03_REV_DEPT_{snap_yyyymm}.pdf')
        pg_1 = pd.DataFrame(pdf.pages[0].extract_tables()[0][1:])
        pg_2 = pd.DataFrame(pdf.pages[1].extract_tables()[0][1:])

        col_list = pdf.pages[0].extract_tables()[0][0]
        col_list = list(filter(None, col_list))
        col_list = [c.replace('\n', '') for c in col_list]
        col_list = ['idx'] + col_list

        data_2 = pd.concat([pg_1, pg_2])
        data_2.columns = col_list
        data_2['主管機關名稱'] = data_2['主管機關名稱'].apply(lambda x: '合計' if x == None else x.replace('\n', ''))
        data_2['資料年月'] = snap_yyyymmdd
        data_2['ITEM_TYPE'] = '機關別'
        data_2 = data_2[['資料年月','ITEM_TYPE', '主管機關名稱','年度預算數','累計分配數','實收累計數']]

        for c in data_2.columns[3:]:
            data_2[c] = data_2[c].apply(lambda x: x.replace(',', ''))
            data_2[c] = data_2[c].apply(lambda x: x.replace('-', '0'))
            data_2[c] = data_2[c].astype(int) 
        data_2.columns = ['SNAP_DATE','ITEM_TYPE', 'ITEM','Plan_Year','Allocate_Accu','Actual_accu']
    except Exception as e:
        print(f'{e}: phase 2 ETL')

    # concat data 1 & data 2
    try:
        final_df = pd.concat([data, data_2]).fillna(0).reset_index(drop=True)
        final_df = final_df[data.columns]
        final_df['ITEM'] = final_df['ITEM'].apply(lambda x: x.replace(' ', ''))        
        for c in final_df.columns[3:]:
            final_df[c] = final_df[c].astype(float)
        final_df['SNAP_DATE'] = final_df['SNAP_DATE'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'))
    except Exception as e:
        print(f'{e}: final part')
        
    return final_df
