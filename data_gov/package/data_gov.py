import os
import pandas as pd
import numpy as np
import requests
from zipfile import ZipFile
import shutil
import time
import datetime
   
def download_corp_tax(url: str, corp_tax_path: str) -> str:

    file = requests.get(url) 
    filename = corp_tax_path
    with open(filename,'wb') as f:
        f.write(file.content)
        f.close()

    zfile = ZipFile(filename, 'r')
    for zf in zfile.namelist():
        zfile.extract(zf, './tmp')
        zfile.close()
    return [f for f in ZipFile(filename, 'r').namelist() if f.lower().find('bgm') > -1]
    
def clean_corp_tax(path: str) -> pd.DataFrame:
    # 讀檔, 轉西元年
    data = pd.read_csv(f'tmp/{path}', converters={'統一編號': str,
                                                       '總機構統一編號':str,
                                                       '行業代號': str,
                                                       '行業代號1': str,
                                                       '行業代號2': str,
                                                       '行業代號3': str})
    # 把空值換成空字串
    # 再一起轉成None
    # 讀檔convert成str時, 會把原本的空值變成空字串''或是 'NaN'
    data = data.fillna('')
    for c in data.columns:
        data[c] = data[c].apply(lambda x: None if x == '' else x)
        
    data = data.iloc[1:,:]
    data['設立日期'] = data['設立日期'].apply(lambda x: str(int(x) + 19110000))    
    # addr_wt_id 是加上統編, 做參考
    # addr 是照金控打點系統的格式, 做之後進一步的打點處理
    addr_wt_id = pd.DataFrame({'編碼': list(range(1,(data.shape[0] + 1))),
                               '地址': data['營業地址'],
                               '統一編號': data['統一編號']})
    addr = addr_wt_id.drop(['統一編號'], axis=1)
    data.columns = ['company_addr', 'company_id', 'company_hid', 'company_name',
                    'capital', 'est_date', 'type', 'invoice',
                    'industry_code', 'industry_name', 'industry_code_1', 'industry_name_1',
                    'industry_code_2', 'industry_name_2', 'industry_code_3','industry_name_3']
    # change company_addr, replace ',' to '，'　by chiata
    data['company_addr'] = data['company_addr'].apply(lambda x: x.replace(',', '，'))
    # add snap date by chiata
    data['snap_date'] = datetime.datetime.now().strftime('%Y%m')
    return data

def download_director(url: str) -> str:
    # 下載後解壓縮
    file = requests.get(url, verify=False)
    filename = 'tmp/director.zip' 
    with open(filename,'wb') as f:
        f.write(file.content)
        f.close()
        
    with ZipFile('tmp/director.zip', 'r') as zf:
        for fn in zf.namelist():
            right_fn = fn.encode('cp437').decode('ms950')  # 編碼
            with open('tmp/' + right_fn, 'wb') as output_file:  
                with zf.open(fn, 'r') as origin_file:  
                    shutil.copyfileobj(origin_file, output_file)  
    return right_fn
def clean_director(path: str) -> pd.DataFrame:
    data = pd.read_csv(f'tmp/{path}', engine='python', encoding='utf-8',
                       converters={'統一編號': str}
                      )
    # 把空值換成空字串
    # 再一起轉成None
    # 讀檔convert成str時, 會把原本的空值變成空字串''或是 'NaN'
    data = data.fillna('')
    for c in data.columns:
        data[c] = data[c].apply(lambda x: None if x == '' else x)
    
    # 欄位對照
    # 統一編號、公司名稱、職稱、姓名、所代表法人、持有股份數
    # company_id company_name title person_name rep_juristic_person_name tot_share_hold

    data.columns = ['company_id', 'company_name', 'title', 'person_name', 'rep_juristic_person_name', 'tot_share_hold']
    data['snap_date'] = datetime.datetime.now().strftime('%Y%m')
    return data
    
def clean_concat_org(gov_url: str, edu_url: str, org_url: str) -> pd.DataFrame:
    try:
        gov = pd.read_csv(gov_url, converters={'統一編號':str})
        gov['org_group'] = 'gov'
        gov.columns = ['tax_id_number', 'org_name', 'org_group']
    except:
        raise FileNotFoundError('gov')
    
    try:
        edu = pd.read_csv(edu_url, 
                          skiprows=[1],
                          converters={'統一編號':str})
        edu['org_group'] = 'edu'
        edu.columns = ['tax_id_number', 'org_name', 'city', 'modify_dt', 'modify_code', 'modify_comment', 'org_group']
    except:
        raise FileNotFoundError('edu')
    try:
        org = pd.read_csv(org_url,  
                          skiprows=[1],
                          converters={'統一編號':str})
        org['org_group'] = 'org'
        org.columns = ['tax_id_number', 'org_name', 'city', 'modify_dt', 'modify_code', 'modify_comment', 'org_group']
    except:
        raise FileNotFoundError('org')
        
    final_df = pd.concat([gov, edu, org])
    final_df = final_df[['tax_id_number', 'org_name', 'city', 'modify_dt', 'modify_code', 'modify_comment', 'org_group']]
    final_df = final_df.fillna('')
    for c in final_df.columns:
        if (c == 'modify_dt') | (c == 'modify_code'):
            final_df[c] = final_df[c].apply(lambda x: None if x == '' else str(int(x)))
        final_df[c] = final_df[c].apply(lambda x: None if x == '' else x)
    final_df = final_df.reset_index(drop=True)
    final_df['snap_yyyymm'] = datetime.datetime.now().strftime('%Y%m')
    return final_df
