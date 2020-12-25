from selenium import webdriver
from selenium.webdriver.common.keys import Keys

def search_company_info(query_id: str):
    try:
        search_bar = driver.find_element_by_id('qryCond')
        search_bar.clear() 
        search_bar.send_keys(query_id)
        driver.find_element_by_id('qryBtn').click()
        company_name = driver.find_element_by_xpath(".//a[@class='hover']").text
        driver.find_element_by_link_text(company_name).click()
        table_div = driver.find_element_by_xpath(".//div[@class='table-responsive']")
        table_info = table_div.text
        table_list = table_info.split('\n')
        table_list = list(filter(None, table_list))

        target = ['統一編號','公司狀況','公司名稱','資本總額(元)','代表人姓名','公司所在地','核准設立日期','最後核准變更日期',
                 '商業名稱', '現況', '資本額(元)', '地址', '最近異動日期']
        info = {}
        for t in target:
            try:
                info[t] = [x.split(' ') for x in table_list if x.find(t) > -1][0][1]
            except :
                info[t] = None
    except Exception as e:
        print(e)
    
    else:
        driver.find_element_by_link_text('回查詢清單').click()
    
    return table_list, info
    
# 起driver
driver = webdriver.Chrome()
# 目標 url
url = 'https://findbiz.nat.gov.tw/fts/query/QueryList/queryList.do'
driver.get(url)
# checkbox
driver.find_element_by_xpath('//input[@value="brCmpyType"]').send_keys(Keys.SPACE)  
driver.find_element_by_xpath('//input[@value="busmType"]').send_keys(Keys.SPACE)  
driver.find_element_by_xpath('//input[@value="lmtdType"]').send_keys(Keys.SPACE)  
# 取得資訊
search_company_info('98899507')
