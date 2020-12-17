import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# 建立url 清單
# 找到所有資料的連結
def create_file_list(url: str) -> list:
    headers = {
    'Referer': f"{url.split('#')[0]}",
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'
    }
    r = requests.get(url, verify=False, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    docs_list = soup.find_all('a', attrs={'class': 'csv'})
    docs_list = list(map(lambda x: x.get('onclick'), docs_list))
    docs_list = list(map(lambda x: x.split('(')[-1].split(')')[0], docs_list))
    docs_list = list(map(lambda x: eval(x), docs_list))
    link = 'https://data.gcis.nat.gov.tw'
    docs_list = list(map(lambda x: link + x, docs_list))  
    return docs_list
# 下載檔案, 直接用pd.read_csv讀到記憶體, 沒有存實質檔案
# once: 下載一次, 即下載當月更新的資料
def download_data(file_list: list, once=False) -> pd.DataFrame:
    data = pd.DataFrame()
    if once == True:
        file_list = file_list[:1]
    for i, f in enumerate(file_list):
        retry = 0
        while True:
            try:
                tmp_df = pd.read_csv(f)
                if tmp_df.shape[0] < 1:
                    raise Exception
                data = pd.concat([data, tmp_df])           
                break
            except Exception as e:
                print(f'{e}: {i}')
                print(f'retry: {retry}')
                retry += 1
                if retry >= 3:
                    break
        time.sleep(2)     
    return data
# 轉置, 依照檔案分類不同做出相應處理以利合併
def transfer_data(df: pd.DataFrame, cate: str) -> pd.DataFrame:
    if cate == 'company':
        df = df.fillna('')
        if df['status'].unique()[0] == '設立':
            df['核准設立日期'] = df['核准設立日期'].apply(lambda x: str(int(x) + 19110000) if x != '' else None)
            df['snap_yyyymm'] = df['核准設立日期'].apply(lambda x: x[:6])
        elif  df['status'].unique()[0] == '變更':
            df['核准變更日期'] = df['核准變更日期'].apply(lambda x: str(int(x) + 19110000) if x != '' else None)
            df['snap_yyyymm'] = df['核准變更日期'].apply(lambda x: x[:6])
        else:
            df['核准設立日期'] = df['核准設立日期'].apply(lambda x: str(int(x) + 19110000) if x != '' else None)
            df['核准解散日期'] = df['核准解散日期'].apply(lambda x: str(int(x) + 19110000) if x != '' else None)
            df['代表人'] = df['代表人'].apply(lambda x: None if x == '' else x)
            df['snap_yyyymm'] = df['核准解散日期'].apply(lambda x: x[:6])
        df['cate'] = '公司登記'
        df['資本額'] = df['資本額'].astype(int)
        df = df.drop(['序號'], axis=1)
    if cate == 'commercial':
        df = df.fillna('')
        if df['status'].unique()[0] == '設立':
            df['設立日期'] = df['設立日期'].apply(lambda x: str(int(x) + 19110000) if x != '' else None)
            df['snap_yyyymm'] = df['設立日期'].apply(lambda x: x[:6])
        elif df['status'].unique()[0] == '變更':
            df['變更日期'] = df['變更日期'].apply(lambda x: str(int(x) + 19110000) if x != '' else None)
            df['snap_yyyymm'] = df['變更日期'].apply(lambda x: x[:6])
        else:
            df['停業日期'] = df['變更日期'].apply(lambda x: str(int(x) + 19110000) if x != '' else None)
            df = df.drop(['變更日期'], axis=1)
            df['snap_yyyymm'] = df['停業日期'].apply(lambda x: x[:6] if x != None else None)
        df['cate'] = '商業登記'
        df['資本額'] = df['資本額'].apply(lambda x: int(x) if x != '' else 0)
        df = df.drop(['序號'], axis=1)
    
    return df
# 最後整理
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    if df['cate'].unique()[0] == '公司登記':
        df = df.where(pd.notnull(df), None)
        df = df[['cate', '統一編號', '公司名稱', '公司所在地', '代表人',\
                 '資本額', '核准設立日期', '核准變更日期','核准解散日期', \
                 'status', 'snap_yyyymm']]
        col_list = ['cate', 'company_id', 'company_name', 'company_addr', 'representative', 'capital', \
                    'create_dt', 'modify_dt', 'close_dt','status', 'snap_yyyymm']
        df.columns = col_list
    if df['cate'].unique()[0] == '商業登記':
        df = df.where(pd.notnull(df), None)
        df = df[['cate', '統一編號', '商業名稱', '商業所在地', '負責人', '資本額', '組織型態', \
                                         '設立日期', '變更日期','停業日期','status', 'snap_yyyymm']]
        col_list = ['cate', 'company_id', 'company_name', 'company_addr', 'representative', 'capital',\
                    'type', 'create_dt', 'modify_dt', 'close_dt','status', 'snap_yyyymm']
        df.columns = col_list
    return df
