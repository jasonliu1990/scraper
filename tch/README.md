# TCH
公庫處: 北市府公開資料
# 涵蓋資料集
#### 每月更新
1. 人口數: https://ca.gov.taipei/News_Content.aspx?n=8693DC9620A1AABF&sms=D19E9582624D83CB&s=E70E0ADF8510073C
2. 歲入(來源別): https://data.taipei/api/dataportal/get-dataset-detail?id=09b12b01-c562-4a0e-9aeb-93312ce57372
3. 歲入(機關別): https://dof.gov.taipei/News.aspx?n=4D21C7BE105121C3&sms=CB3C416EA7B4104E
#### 需要時更新
1. 米其林餐廳: https://guide.michelin.com/tw/zh_TW/restaurants/
2. 台北景點: https://www.travel.taipei/zh-tw/attraction/all-regions?mode=list&sortby=tripadvisor&page=1
# 程式執行時間
每月11日執行
# 產出:
1. 人口數: TCH_POPULATION.{snap_yyyymmdd}
2. 歲入: TCH_REVENUE.{snap_yyyymmdd}
3. 美食景點: TCH_FOOD_PLAY.{snap_yyyymmdd}
# 主程式參數
-f FOODPLAY, --foodplay FOODPLAY download food play or not;(y/n) <br>
若要更新美食景點, 則在 comment lines輸入: python main.py -f y
