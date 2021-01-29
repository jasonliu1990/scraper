import warnings
warnings.filterwarnings('ignore')
import os
import re
import datetime
import requests 
import pandas as pd
import time

# download img & write file
def download_img(url: str, path: str):
    r = requests.get(url, verify=False)
    with open(path, 'wb') as f:
        f.write(r.content)   
# add cols PIC & download img
def rebuild_dataset(df: pd.DataFrame, IMG_DIR: str, snap_yyyymmdd_today: str) -> pd.DataFrame:
    df = df.rename(columns={'PIC': 'PIC_URL'})
    df['PIC'] = [f"{IMG_DIR}/{snap_yyyymmdd_today}_{c}.jpg" for c in range(len(df))]
    
    for u, p in zip(df['PIC_URL'], df['PIC']):
        try:
            download_img(u, p)
        except:
            try:
                time.sleep(5)
                download_img(u, p)
            except Exception as e:
                print(e)
    
    df = df.drop(['PIC_URL'], axis=1)
    return df
