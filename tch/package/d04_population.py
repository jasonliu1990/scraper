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
from dateutil.relativedelta import relativedelta
import requests 
from bs4 import BeautifulSoup
import pandas as pd
import ezodf

def read_ods(filename: str, sheet_no=0, header=0) -> pd.DataFrame:
    tab = ezodf.opendoc(filename=filename).sheets[sheet_no]
    
    return pd.DataFrame({col[header].value:[x.value for x in col[header+1:]]
                         for col in tab.columns()})

# 上個月
def download_file(url: str, foldername: str) -> tuple:
    # set param
    sheet = datetime.datetime.now().month - 2
    st = datetime.datetime.now()
    snap_yyyymmdd = datetime.datetime.strptime(st.strftime('%Y%m'), '%Y%m') + relativedelta(days=-1)
    snap_yyyymmdd = snap_yyyymmdd.strftime('%Y%m%d')
    # download data
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'}
    r = requests.get(url, verify=False, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    a_list = soup.findAll('a')
    idx_list = [a_list.index(a) - 1 for a in a_list if a.text.find('人口數') > -1]
    target_url = a_list[idx_list[1]]
    r = requests.get(target_url['href'])
    with open(f'{foldername}/population_{snap_yyyymmdd}.ods', 'wb') as f:
            f.write(r.content)
    return (f'{foldername}/population_{snap_yyyymmdd}.ods', sheet, snap_yyyymmdd)
    
def create_dataset(file_path: str, sheet: int, data_snap_yyyymmdd: str) -> pd.DataFrame:
    data = read_ods(file_path, header=1, sheet_no=sheet)
    data = data.dropna(how='all', axis=1)
    data = data.dropna(how='all', axis=0)
    data.columns = [c.replace(' ', '') for c in data.columns]
    age_grp_m = data[data['性別'] == '男'].loc[:, [c for c in data.columns if (c.find('區域別') > -1) | (c.find('合計') > -1) | (c.find('100') > -1)]]
    age_grp_m['child'] = age_grp_m.loc[:, ['合計_0~4歲', '合計_5~9歲', '合計_10~14歲']].sum(axis=1)
    age_grp_m['adult'] = age_grp_m.loc[:, ['合計_15~19歲', '合計_20~24歲', '合計_25~29歲', '合計_30~34歲', '合計_35~39歲',
                                   '合計_40~44歲', '合計_45~49歲', '合計_50~54歲', '合計_55~59歲', '合計_60~64歲']].sum(axis=1)
    age_grp_m['oldman'] = age_grp_m.loc[:, ['合計_65~69歲', '合計_70~74歲', '合計_75~79歲', '合計_80~84歲', '合計_85~89歲',
                                   '合計_90~94歲', '合計_95~99歲', '100歲以上']].sum(axis=1)
    age_grp_m = age_grp_m[['區域別', 'child', 'adult', 'oldman']].reset_index(drop=True)
    age_grp_f = data[data['性別'] == '女'].loc[:, [c for c in data.columns if (c.find('區域別') > -1) | (c.find('合計') > -1) | (c.find('100') > -1)]]
    age_grp_f['child_f'] = age_grp_f.loc[:, ['合計_0~4歲', '合計_5~9歲', '合計_10~14歲']].sum(axis=1)
    age_grp_f['adult_f'] = age_grp_f.loc[:, ['合計_15~19歲', '合計_20~24歲', '合計_25~29歲', '合計_30~34歲', '合計_35~39歲',
                                       '合計_40~44歲', '合計_45~49歲', '合計_50~54歲', '合計_55~59歲', '合計_60~64歲']].sum(axis=1)
    age_grp_f['oldman_f'] = age_grp_f.loc[:, ['合計_65~69歲', '合計_70~74歲', '合計_75~79歲', '合計_80~84歲', '合計_85~89歲',
                                       '合計_90~94歲', '合計_95~99歲', '100歲以上']].sum(axis=1)
    age_grp_f = age_grp_f[['區域別', 'child_f', 'adult_f', 'oldman_f']].reset_index(drop=True)
    age_grp = pd.merge(age_grp_m, age_grp_f, on='區域別')
    gender_grp = data[data['性別'].isin(['男', '女'])].iloc[:,:3].reset_index(drop=True)
    gender_grp = gender_grp.set_index(['區域別','性別']).unstack()
    gender_grp.columns = gender_grp.columns.droplevel(0)
    gender_grp = gender_grp.rename_axis(None, axis=1)
    gender_grp = gender_grp.reset_index()
    final_df = pd.merge(gender_grp, age_grp, on='區域別')
    final_df['snap_date'] = data_snap_yyyymmdd
    final_df = final_df[['snap_date', '區域別', '男', '女', 'child', 'adult', 'oldman', 'child_f', 'adult_f', 'oldman_f']]
    final_df = final_df.rename(columns={'區域別': 'DISTRICT',
                                        '男': 'MALE',
                                        '女': 'FEMALE'})
    final_df.columns = [c.upper() for c in final_df.columns]
    final_df['DISTRICT'] = final_df['DISTRICT'].apply(lambda x: x.replace(' ', ''))
    for c in final_df.columns[2:]:
        final_df[c] = final_df[c].astype(int)
    final_df['SNAP_DATE'] = final_df['SNAP_DATE'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'))
    return final_df
