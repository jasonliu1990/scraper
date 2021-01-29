#!/usr/bin/env python3
# -*-coding: utf-8-*-
#
# Author: Chiata Liu
# Date: 23-Nov-2020

import warnings
warnings.filterwarnings('ignore')
import os
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd

def download_file(url: str, place_holder, ec: bool=False) -> pd.DataFrame:
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
        'Referer': 'https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData',
        'Origin': 'https://www.nccc.com.tw'
    }
    if not ec:
        url_2 = url.replace(place_holder, '1')
        r = requests.get(url_2, verify=False, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')
        page_list = soup.find('div', attrs={'class': "page"}).findAll('a')
        last_page = max([eval(p.text) for p in page_list if p.text.isnumeric()]) 
        data = pd.DataFrame()
        for i in range(1, last_page):
            try:
                url_2 = url.replace(place_holder, str(i))
                r = requests.get(url_2, verify=False, headers=headers)
                soup = BeautifulSoup(r.text, 'html.parser')
                title_list = soup.findAll('td', attrs={'style': "vertical-align: middle;text-align:left"})
                title_list = [t.text for t in title_list]
                file_list = soup.findAll('a', attrs={'class': "btn2"})
                file_list = [f['href'] for f in file_list]
            except Exception as e:
                print(i)
                print(e)           
            for f, t in zip(file_list, title_list):
                try: 
                    f = f.replace(' ', '%20')
                    df = pd.read_csv(f, encoding='utf-8')
                    df = df.iloc[:-2, :].dropna(subset=['年月'])
                    if len([c for c in df.columns if c == '國別']) > 0:
                        try:
                            df['city'] = df.columns[3].split('[')[0]
                        except:
                            df['city'] = '全國'
                        df.columns = ['年月', '國別', 'txn_cnt', 'txn_amt', 'city']
                        data = pd.concat([data, df])
                    else:                           
                        df['file_title'] = t
                        data = pd.concat([data, df])
                    time.sleep(1)

                except Exception as e:
                    print(e)
                    print(f)
            time.sleep(1)
    if ec:
        r = requests.get(url, verify=False, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')
        file_list = soup.findAll('a', attrs={'class': "btn2"})
        file_list = [f['href'] for f in file_list]
        data = pd.DataFrame()
        for f in file_list:
            try: 
                df = pd.read_csv(f, encoding='utf-8')
                stop = 0
                try:
                    stop = df[df['時間'] == '備註1'].index[0]
                except:
                    stop = None
                df = df.iloc[:stop, :].dropna(subset=['時間'])
                data = pd.concat([data, df])
                time.sleep(0.5)
            
            except Exception as e:
                print(e)
                print(f)

    return data

# etl 
def create_txn_df(df: pd.DataFrame) -> pd.DataFrame:
    # 分別處理 男女
    df1 = df[['file_title', '地區', '女性[筆數]', '女性[金額，新台幣]', '年月', '類別']]
    df1.columns = ['file_title', 'area', 'txn_cnt', 'txn_amt', 'snap_yyyymm', 'mcc_group']
    df1['gender'] = 'F'
    df2 = df[['file_title', '地區', '男性[筆數]', '男性[金額，新台幣]', '年月', '類別']]
    df2.columns = ['file_title', 'area', 'txn_cnt', 'txn_amt', 'snap_yyyymm', 'mcc_group']
    df2['gender'] = 'M'
    # output1: txn by gender
    txn_df = pd.concat([df1, df2])
    txn_df['snap_yyyymm'] = txn_df['snap_yyyymm'].apply(lambda x: str(int(x.split('年')[0])+1911) + x.split('年')[1].replace('月', ''))
    txn_df['area'] = txn_df['area'].fillna('全國')
    # 處理原檔案, 直接加總不分性別
    df['地區'] = df['地區'].fillna('全國')
    df['txn_cnt'] = df['女性[筆數]'] + df['男性[筆數]']
    df['txn_amt'] = df['女性[金額，新台幣]'] + df['男性[金額，新台幣]']
    # output2: txn
    txn_df_2 = df[['地區', '年月', '類別', 'txn_cnt', 'txn_amt']]
    txn_df_2.columns = ['city', 'snap_yyyymm', 'mcc_group', 'txn_cnt', 'txn_amt']
    txn_df_2['mcc_group'].unique()
    txn_df_2['snap_yyyymm'] = txn_df_2['snap_yyyymm'].apply(lambda x: str(int(x.split('年')[0])+1911) + x.split('年')[1].replace('月', ''))
 
    return txn_df, txn_df_2

def  create_txn_age_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['地區', '年月', '類別','未滿20[筆數]',
            '未滿20[金額，新台幣]','20[含]-25[筆數]', '20[含]-25[金額，新台幣]', '25[含]-30[筆數]', '25[含]-30[金額，新台幣]',
            '30[含]-35[筆數]', '30[含]-35[金額，新台幣]', '35[含]-40[筆數]', '35[含]-40[金額，新台幣]',
            '40[含]-45[筆數]', '40[含]-45[金額，新台幣]', '45[含]-50[筆數]', '45[含]-50[金額，新台幣]',
            '50[含]-55[筆數]', '50[含]-55[金額，新台幣]', '55[含]-60[筆數]', '55[含]-60[金額，新台幣]',
            '60[含]-65[筆數]', '60[含]-65[金額，新台幣]', '65[含]-70[筆數]', '65[含]-70[金額，新台幣]',
            '70[含]-75[筆數]', '70[含]-75[金額，新台幣]', '75[含]-80[筆數]', '75[含]-80[金額，新台幣]',
            '80[含]以上[筆數]', '80[含]以上[金額，新台幣]']]

    df['地區'] = df['地區'].fillna('全國')
    txn_age_df = df.set_index(['地區', '年月', '類別']).stack().reset_index()
    txn_age_df['level_3'] = txn_age_df['level_3'].apply(lambda x: x.replace('筆數', ',筆數'))
    txn_age_df['level_3'] = txn_age_df['level_3'].apply(lambda x: x.replace('金額', ',金額'))
    txn_age_df['age'] = txn_age_df['level_3'].apply(lambda x: x.split('[,')[0])
    txn_age_df['txn_type'] = txn_age_df['level_3'].apply(lambda x: x.split('[,')[1])
    txn_age_df['txn_type'] = txn_age_df['txn_type'].apply(lambda x: x.replace('，新台幣]', '').replace(']', ''))
    txn_age_df['年月'] = txn_age_df['年月'].apply(lambda x: str(int(x.split('年')[0])+1911) + x.split('年')[1].replace('月', ''))
    txn_age_df = txn_age_df[['地區', '年月', '類別', 'age', 'txn_type', 0]]
    txn_age_df.columns = ['city', 'snap_yyyymm', 'mcc_group', 'age', 'txn_type', 'value']

    df1 = txn_age_df[txn_age_df['txn_type'] == '筆數']
    df1.columns = ['city', 'snap_yyyymm', 'mcc_group', 'age', 'txn_type', 'txn_cnt']
    df2 = txn_age_df[txn_age_df['txn_type'] == '金額']
    df2.columns = ['city', 'snap_yyyymm', 'mcc_group', 'age', 'txn_type', 'txn_amt']

    final_df = pd.merge(df1, df2, on=['city', 'snap_yyyymm', 'mcc_group', 'age']).drop(['txn_type_x', 'txn_type_y'], axis=1)
    age_grp_map = {
        '未滿20': '1. <20', 
        '20[含]-25': '2. 20-24', 
        '25[含]-30': '3. 25-29', 
        '30[含]-35': '4. 30-34', 
        '35[含]-40': '5. 35-39', 
        '40[含]-45': '6. 40-44',
        '45[含]-50': '7. 45-49', 
        '50[含]-55': '8. 50-54', 
        '55[含]-60': '9. 55-59', 
        '60[含]-65': '10. 60-64', 
        '65[含]-70': '11. 65-69',
        '70[含]-75': '12. 70-74', 
        '75[含]-80': '13. 75-79', 
        '80[含]以上': '14.>=80'
    }
    final_df['age'] = final_df['age'].map(age_grp_map)
    
    return final_df

def create_txn_ec_df(df: pd.DataFrame) -> pd.DataFrame:
    df['時間'] = df['時間'].apply(lambda x: str(int(x.split('年')[0])+1911) + x.split('年')[1].replace('月', ''))
    df.columns = ['snap_yyyymm', 'country', 'mcc_group', 'cate', 'txn_cnt', 'txn_amt']
    return df

def create_txn_foreign_df(df: pd.DataFrame) -> pd.DataFrame:
    df['年月'] = df['年月'].apply(lambda x: str(int(x.split('年')[0])+1911) + x.split('年')[1].replace('月', ''))
    df = df[['年月', 'city', 'order_by', '國別', 'txn_cnt', 'txn_amt']]
    df.columns = ['snap_yyyymm', 'city', 'order_by', 'country', 'txn_cnt', 'txn_amt']
    return df   
    
