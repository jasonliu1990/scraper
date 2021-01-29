# CC OPENDATA
信用卡公開資料
# 涵蓋資料集
1. 性別: https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/Sexconsumption?WCM_PI=1&WCM_Page.bde3b591-d396-4179-9156-bc84ba6c998b={place_holder}&WCM_PageSize.bde3b591-d396-4179-9156-bc84ba6c998b=10
2. 年齡區間: https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/Ageconsumption?WCM_PI=1&WCM_Page.bde3b591-d396-4179-9156-bc84ba6c998b={place_holder}
3. 電商: https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/ECommerce
4. TOP 10海外交易(筆數): https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/Top10ForeignCardConsumptionCount?WCM_PI=1&WCM_Page.aa5a95cd-0b6b-403f-bb0c-ee319cf16024={place_holder}
5. TOP 10海外交易(金額): https://www.nccc.com.tw/wps/wcm/connect/zh/home/openinformation/CreditCardData/Top10ForeignCardConsumptionPrice?WCM_PI=1&WCM_Page.aa5a95cd-0b6b-403f-bb0c-ee319cf16024={place_holder}

# 程式執行時間
預計每月27日執行
# 產出:
1. 性別: cc_open_txn_gender.{today_yyyymmdd}
2. 總交易: cc_open_txn.{today_yyyymmdd}
3. 年齡: cc_open_txn_age.{today_yyyymmdd}
4. 店商: cc_open_txn_ec.{today_yyyymmdd}
5. 海外交易: cc_open_txn_foreign.{today_yyyymmdd}
6. 合併檔: cc_open.{today_yyyymmdd}
