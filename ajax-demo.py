import requests
from bs4 import BeautifulSoup
import json
# fubon ebmb info
url = 'https://ebank.taipeifubon.com.tw/EXT/common/Ous.faces'
r = requests.get(url)
soup = BeautifulSoup(r.text, 'html.parser')
view_state_value = soup.find('input', attrs={'id': 'javax.faces.ViewState'})['value']
r2 = requests.post(url, 
                   cookies=r.cookies, 
                   data={
                       'txRes': '1',
                       'ajaxAction': 'generateImmediate',
                       'javax.faces.ViewState': view_state_value
                       }
                  )
r3 = requests.post(url, 
                   cookies=r.cookies, 
                   data={
                       'txRes': '2',
                       'ajaxAction': 'generateImmediate',
                       'javax.faces.ViewState': view_state_value
                       }
                  )
r4 = requests.post(url,
                   cookies=r.cookies,
                   data={
                       'ajaxAction': 'genNmiCount',
                       'javax.faces.ViewState': view_state_value
                   }
                  )
r5 = requests.post(url,
                   cookies=r.cookies,
                   data={
                       'ajaxAction': 'getEsbCount',
                       'javax.faces.ViewState': view_state_value
                   }
                  )
r6 = requests.post(url,
                   cookies=r.cookies,
                   data={
                       'ajaxAction': 'getEsbMonitorOperation',
                       'javax.faces.ViewState': view_state_value
                   }
                  )
dict1 = json.loads(r2.text)
dict1.update(json.loads(r3.text))
dict1.update(json.loads(r4.text))
dict1.update(json.loads(r5.text))
dict1.update(json.loads(r6.text))
print(dict1)
