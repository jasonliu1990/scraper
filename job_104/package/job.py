import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import datetime
import re
import warnings
warnings.filterwarnings('ignore')
import time
import http.client
http.client._MAXLINE = 655360

# 建立全部的 url_list
def create_url_list(ind_list:list) -> list:
    """ ind_list: 目標cate網址
    """
    target_list = []
    for ind in ind_list:
        url = f'https://www.104.com.tw/cust/list/index/?page=0&order=5&mode=s&jobsource=checkc&indcat={ind}'
        target_list.append(url)
    print(f'target list: {len(target_list)}')
    # 送cate url 拿到全部的url
    url_list = []
    for url in target_list:
        try:
            r = requests.get(url, verify=False, headers=headers)
            html = BeautifulSoup(r.text, 'html.parser')
            total_page = re.sub('[^0-9]', '', html.find_all('div', attrs={'class': "page-total"})[0].text)
            for p in range(1, int(total_page) + 1):
                page = f'page={p}'
                url = url.replace(f'page={p-1}', page)
                url_list.append(url)
        except Exception as e:
            logging.error(f'check error:{e}')
    print(f'total url: {len(url_list)}')
    return url_list
# 爬蟲 function
def create_comp_df(url:str, headers:set) -> pd.DataFrame:
    """ 用url 拿到公司基本訊息的dataframe
    """
    r = requests.get(url, verify=False, headers=headers)
    html = BeautifulSoup(r.text, 'html.parser')
    company_list = html.find("div", attrs={'id':"company-result"}).find_all("article", attrs={'class':"items"})
    comp_name_list = []
    location_list = []
    industry_list = []
    capital_list = []
    employee_list = []
    target_url_list = []
    refer_url_list = []
    target_id_list = []   
    for comp in company_list:
        try:
            comp_name = comp.a.string
            location = comp.find_all('p', attrs={'class': 'data'})[0].text.split('，')[0].replace('\n', '')
            industry = comp.find_all('p', attrs={'class': 'data'})[0].text.split('，')[1].replace('\n', '')
            capital = comp.find_all('p', attrs={'class': 'data'})[1].text.split('，')[0].split('：')[1].replace('\n', '')       
            employee = comp.find_all('p', attrs={'class': 'data'})[1].text.split('，')[1].split('：')[1].replace('\n', '') 
            if len(comp.find_all('a')) > 1:
                target_id = comp.find_all('a')[-1]['href'].split('?')[0].split('/')[-1]
                target_url = f'https://www.104.com.tw/company/ajax/joblist/{target_id}?roleJobCat=0_0&area=0&page=1&pageSize=20&order=11&asc='
                refer_url = comp.find_all('a')[-1]['href']             
            else:
                target_url = None
                target_id = None
                refer_url = None
                           
            comp_name_list.append(comp_name)
            location_list.append(location)
            industry_list.append(industry)
            capital_list.append(capital)
            employee_list.append(employee)
            target_url_list.append(target_url)
            refer_url_list.append(refer_url)
            target_id_list.append(target_id)

        except Exception as e:
            logging.error(f'check error:{e}')
            
    comp_dict = {
        'comp_name': comp_name_list,
        'location': location_list,
        'industry': industry_list,
        'capital': capital_list,
        'employee': employee_list,
        'url': target_url_list,
        'refer_url': refer_url_list,
        'target_id': target_id_list,
                }
    comp_df = pd.DataFrame(comp_dict)
    
    return comp_df

# 查職缺數
def create_job(url:str, refer_url:str) -> tuple:
    
    try:
        headers_2 = {}
        headers_2['Referer'] = refer_url
        headers_2['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'

        if type(url) == str:
            r = requests.get(url, headers=headers_2, verify=False)
            data = json.loads(r.text)
            job = data['data']['totalCount']
            if job != 0:
                appear_date = max(list(map(lambda x: x['appearDate'], data['data']['list']['normalJobs'])))
            else:
                appear_date = None
        else:
            job = 0
            appear_date = None
    except Exception as e:
        logging.error(f'check error:{e}')
    
    return job, appear_date
