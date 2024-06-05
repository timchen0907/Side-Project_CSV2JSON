# -*- coding: utf-8 -*-
"""
Desire Output:
[
 {
  "_id":"A0001568657",
  "member_id":"A0001568657",
  "tags":[
          {"tag_name":"一般會員",
           "detail":[{"detail_name": "官網登入次數",
                      "detail_value":24},
                     {"detail_name": "銷售網站登入次數",
                      "detail_value":14},
                     ] 
           }, 
          {"tag_name": "三年客訴",
           "detail":[{"detail_name": "問題單號",
                      "detail_value":"XfU49733998"}
                     ]
           }
          ]
  }
,]
"""
# 載入套件
import json
import zipfile
import pandas as pd

def csv_to_json():
    # 解壓縮zip
    with zipfile.ZipFile('./CSV2JSON.zip', 'r') as zip_ref:
        zip_ref.extractall()
        
    # 同時設定dtype與low_memory = False，藉由使用更多的ram資源來增加讀取效率
    member_info = pd.read_csv('./CSV2JSON.csv', dtype={'member_id': 'str', 'tag_name': 'str', 'detail_name': 'str', 'detail_value': 'str'}, low_memory=False)
    
    # 根據'member_id'和'tag_name'進行groupby以減少後續迭代次數
    member_info_grouped = member_info.groupby(['member_id', 'tag_name'])
    
    # 利用dict的形式來儲存後成果，key為member_id，value則為本次所要求的成果
    output_data = {}
    # 存儲資料格式轉換失敗的紀錄
    error_data = {}
    # 每次迭代選取memeber_id, tag_name和group並依照下方規則寫入output_data
    for (member_id, tag_name), group in member_info_grouped:
        
        # 確認該member_id是否出現在output_data中
        if member_id not in output_data:
            # 建立存儲格式
            output_data[member_id] = {"_id": member_id, "member_id": member_id, "tags": []}
        
        # 透過tag_name判別是否detail_value的資料格式
        if '會員' in tag_name:
            # 設定轉換失敗機制
            try:
                group["detail_value"] = group["detail_value"].astype(int)
            except:
                print(f"轉換失敗: {(member_id, tag_name)}")
                error_data[member_id + ',' + tag_name] = group[["detail_name", "detail_value"]].to_dict('records')
        
        output_data[member_id]["tags"].append({
            "tag_name": tag_name,
            # 將group的detail_name & detail_value分別轉換成dict key & value後，包入list當中
            "detail": group[["detail_name", "detail_value"]].to_dict('records')
            })
    
    # 透過json.dumps()將output_format轉換成string格式，並print出第一筆結果進行檢查
    # ensure_ascii = False => 使文字保持原樣輸出
    # indent = 2 => 利用縮排增加可讀性
    print('檢查成果範例:\n',json.dumps(list(output_data.values())[0], ensure_ascii=False, indent=2))
    
    # 檢查是否有error紀錄 
    print('檢查是否有轉換失敗紀錄:\n', json.dumps(error_data, ensure_ascii=False, indent=2))
    
    # 輸出成json檔案
    with open('./data.json', 'w') as f:
        json.dump(list(output_data.values()), f, ensure_ascii=False, indent=2)
    
    print('輸出已完成')

if __name__ == '__main__':
    csv_to_json()
