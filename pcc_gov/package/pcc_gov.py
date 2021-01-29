import warnings
warnings.filterwarnings('ignore')
import requests
from bs4 import BeautifulSoup
import time
import json 
import datetime
import pandas as pd
import numpy as np
import random
import time
import dateutil.relativedelta

def pcc_gov(date:str):

    # 第一次 requests, 拿到當前日期的所有資料
    # 做假的 user_agents, 隨機抽一個
    user_agents = ['Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
                   'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
                   'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36']

    headers = {'user-agent': random.choice(user_agents)}
    url = f'https://pcc.g0v.ronny.tw/api/listbydate?date={date}'
    req = requests.get(url, headers=headers, verify=False)
    tmp = json.loads(req.text)

    # 如果當天沒有資料就不做事
    if tmp != {}:
        try:
            url2_list = []
            for i in range(len(tmp['records'])):

                # 挑選出決標公告, 組成url後做第二次requests
                if tmp['records'][i]['brief']['type'] == '決標公告':
                    unit_id =  tmp['records'][i]['unit_id']
                    job_number = tmp['records'][i]['job_number']
                    filename = tmp['records'][i]['filename']
                    url2 = f'http://pcc.g0v.ronny.tw/api/tender?unit_id={unit_id}&job_number={job_number}&filename={filename}'
                    url2_list.append(url2)
        # 也許會沒有資料, exception發生時, print出來檢查
        except Exception as e:     
            if e == 'records':
                pass      
            else:
                # logging.error(f'please check error: {e}')
                print((f'please check error: {e}'))
        else:
            # 拿到 url list後送出第二輪 requests 
            comp_df = pd.DataFrame()   
            i = 0
            for url2 in url2_list:
                try:

                    res2 = requests.get(url2, headers={'user-agent': random.choice(user_agents)}, verify=False)
                    tmp2 = json.loads(res2.text)
                    for j in range(len(tmp2['records'])):       
                        # 拿到的 tmp2由很多份公告組成, 所以先挑出決標公告
                        if tmp2['records'][j]['brief']['type'] == '決標公告':
                            # 檢查id_list和 comp_name_list的長度是否一致
                            check1 = len(tmp2['records'][j]['brief']['companies']['ids'])
                            check2 = len(tmp2['records'][j]['brief']['companies']['names'])
                            if check1 != check2:
                                pass
                            else:
                                # 從 name_key中找到得標的廠商 
                                comp_list = tmp2['records'][j]['brief']['companies']['names']
                                name_key = tmp2['records'][j]['brief']['companies']['name_key']
                                # comp_name & comp_id
                                comp_name_list = []
                                for k, v in name_key.items():
                                    if v[-1].find('未得標') == -1 :
                                        comp_name_list.append(k)
                                id_list = []
                                for comp_name in comp_name_list:
                                    idx = comp_list.index(comp_name)
                                    comp_id = tmp2['records'][j]['brief']['companies']['ids'][idx]
                                    id_list.append(comp_id)
                                # 電話 & 決標金額, 有些為空值, 如果為空,就填 np.nan
                                tel_list = []
                                amount_list = []
                                tmp_dict = tmp2['records'][j]['detail']
                                for comp_id in id_list:
                                    comp = list(tmp_dict.keys())[list(tmp_dict.values()).index(comp_id)]
                                    try:
                                        tel_tmp = comp[:-4] + '廠商電話'
                                        tel = tmp_dict[tel_tmp]
                                        tel_list.append(tel)
                                    except:
                                        tel_list.append(np.nan)
                                    try:
                                        amount_tmp = comp[:-4] + '決標金額'
                                        amount = tmp_dict[amount_tmp]
                                        amount_list.append(amount)
                                    except:
                                        amount_list.append(np.nan)

                                # 加上公告日期, 換成西元年
                                date_tmp = tmp_dict['決標資料:決標公告日期'].split('/')
                                yy = int(date_tmp[0])  + 1911
                                announcement_date = str(yy) + date_tmp[1] + date_tmp[2]
                                # 組裝成df
                                comp_df_tmp = pd.DataFrame({'company_id': id_list,
                                                            'company_name': comp_name_list,
                                                            'company_tel': tel_list,
                                                            'case_amount': amount_list})
                                # 'date' 在資料庫為保留字, 故作更改 by chiata
                                #comp_df_tmp['date'] = announcement_date
                                comp_df_tmp['snap_date'] = announcement_date

                                # 這邊將總金額的符號, 文字去除, 將空值補0
                                comp_df = pd.concat([comp_df, comp_df_tmp],ignore_index=True)
                                comp_df['case_amount'] = comp_df['case_amount'].apply(lambda x: str(x).replace(',', '').replace('元', '')).fillna(0)
                                i += 1
                                #if i % 100 == 0:
                                #    print(f'=== {i} / {len(url2_list)} ===')
                                                
                                time.sleep(1)

                except Exception as e:
    #                 logging.error(f'please check error:{e}')
                    print(e)
                    continue
        return comp_df

def clean_data(data:pd.DataFrame) -> pd.DataFrame:
    
    this_month = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m')
    last_month = (datetime.datetime.now() + dateutil.relativedelta.relativedelta(months=-1)).strftime('%Y%m')
    
    data['case_month'] = data['snap_date'].apply(lambda x: str(x)[:6])
    data = data[['company_id', 'company_name', 'case_month', 'company_tel','case_amount']]
    data = data[(data['case_month'] == this_month) | (data['case_month'] == last_month)]
    data = data.reset_index(drop=True)
    
    wrong_id_index = []
    for i, c in enumerate(data['company_id']):
        if len(c) != 8:
            wrong_id_index.append(i)
    data = data.drop(wrong_id_index)
    data = data[data['company_id'].str.contains('[^A-Za-z]')]

    data['company_tel'] = data['company_tel'].fillna('')
    data['company_tel'] = data['company_tel'].apply(lambda x: None if x == '' else x)
    data['case_amount'] = data['case_amount'].fillna(0)

    # 刪掉(合夥) 及 (獨資)
    data['company_name'] = data['company_name'].apply(lambda x: x.replace('(合夥)', '').replace('(獨資)', ''))

    data = data.reset_index(drop=True)
    data.columns = ['company_id', 'company_name', 'case_date', 'company_tel','case_amount']
    data['case_amount'] = data['case_amount'].astype(float)
    
    return data
