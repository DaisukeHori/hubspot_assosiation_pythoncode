# coding: utf-8

import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta, timezone
import traceback

# 定数
BATCH_SIZE = 100  # 一度に送信するバッチのサイズ
JST = timezone(timedelta(hours=+9))  # 日本標準時 (JST)
UTC = timezone.utc  # UTCタイムゾーン
API_KEY = 'xxxxxxxxxxxxxxxxxxxxxx0'  # ここに実際のAPIキーを入力してください

# CSVファイルを読み込む関数
def read_csv(file_path):
    print(f"Reading CSV file from: {file_path}")
    encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-16']
    for enc in encodings:
        try:
            print(f"Trying encoding: {enc}")
            df = pd.read_csv(file_path, dtype=str, encoding=enc)  # 異なるエンコーディングを試す
            df = df.fillna('')  # 欠損値を空文字に置き換える
            return df
        except UnicodeDecodeError:
            print(f"Failed with encoding: {enc}")
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            raise
        except Exception as e:
            print(f"An error occurred while reading the file with encoding {enc}: {e}")
            print(traceback.format_exc())
            raise
    raise ValueError("Unable to read the file with any of the given encodings.")

# 日付をUTCの深夜0時のタイムスタンプに変換する関数
def convert_to_utc_midnight_timestamp(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y/%m/%d").replace(tzinfo=UTC)
        dt_midnight_utc = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        timestamp = int(dt_midnight_utc.timestamp() * 1000)  # ミリ秒に変換
        return timestamp
    except ValueError:
        print(f"Skipping invalid date: {date_str}")
        return ''  # 日付形式でない場合は空文字を返す

# 日時をUTCのタイムスタンプに変換する関数
def convert_to_utc_timestamp(datetime_str):
    try:
        dt = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S").replace(tzinfo=JST)
        dt_utc = dt.astimezone(UTC)
        timestamp = int(dt_utc.timestamp() * 1000)  # ミリ秒に変換
        return timestamp
    except ValueError:
        print(f"Skipping invalid datetime: {datetime_str}")
        return ''  # 日付形式でない場合は空文字を返す

# HubSpot APIを呼び出して取引のIDを取得する関数
def get_deal_ids(deal_numbers):
    url = 'https://api.hubapi.com/crm/v3/objects/deals/batch/read?archived=false'
    headers = {
        'authorization': f'Bearer {API_KEY}',
        'content-type': 'application/json'
    }
    inputs = [{"id": str(deal_number)} for deal_number in deal_numbers]
    data = {
        "idProperty": "no_____",
        "inputs": inputs,
        "properties": ["dealname"]
    }
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        results = response.json().get("results", [])
        deal_ids = {result["properties"]["no_____"]: result["id"] for result in results}
        return deal_ids
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return {}

# HubSpot APIを呼び出して商品項目を作成する関数
def create_line_items(line_items):
    url = 'https://api.hubapi.com/crm/v3/objects/line_items/batch/create'
    headers = {
        'authorization': f'Bearer {API_KEY}',
        'content-type': 'application/json'
    }
    data = {"inputs": line_items}
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return {}

# CSVファイルを読み込んで商品項目を作成するメイン関数
def main(file_path):
    df = read_csv(file_path)
    
    deal_numbers = df["伝票No."].unique().tolist()
    deal_ids = get_deal_ids(deal_numbers)
    
    line_items = []
    for _, row in df.iterrows():
        deal_id = deal_ids.get(row["伝票No."], None)
        if not deal_id:
            print(f"Deal ID not found for deal number: {row['伝票No.']}")
            continue
        
        line_item = {
            "associations": [
                {
                    "types": [
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 20
                        }
                    ],
                    "to": {
                        "id": deal_id
                    }
                }
            ],
            "properties": {
                "hs_price_jpy": row["売上金額"],
                "tanka_shosu_keta": row["単価小数桁"],
                "hakosuu_shosu_keta": row["箱数小数桁"],
                "name": row["商品名"],
                "nyuusuu": row["入数"],
                "nyuusuu_shosu_keta": row["入数2"],
                "shouhin_code": row["商品コード"],
                "shu_shiire_saki_code": row["仕入先コード"],
                "suuryou_shosu_keta": row["数量小数桁"],
                "tanni": row["単位"],
                "bugyo_biko": row["備考"],
                "bugyo_chumon_no": row["注文No."],
                "bugyo_denpyo_no": row["伝票No."],
                "bugyo_douzi_shori_code": row["同時処理コード"],
                "bugyo_fusen_memo": row["付箋メモ"],
                "bugyo_fusenshoku_code": row["付箋色コード"],
                "bugyo_genka_shohizei": row["原価消費税"],
                "bugyo_genka_zeikomi_kubun_code": row["原価税込区分コード"],
                "bugyo_hako_su": row["箱数"],
                "bugyo_irisu2_shousu_keta": row["入数2小数桁"],
                "bugyo_kazeikubun_code": row["課税区分コード"],
                "bugyo_shohinmei2": row["商品名2"],
                "bugyo_shohinmei3": row["商品名3"],
                "bugyo_shohinmei4": row["商品名4"],
                "bugyo_shohinmei5": row["商品名5"],
                "bugyo_shohinmei6": row["商品名6"],
                "bugyo_shohizei": row["消費税"],
                "bugyo_shukka_kubun": row["出荷区分"],
                "bugyo_shusei_hiduke_uriage_denpyo_base": convert_to_utc_timestamp(row["修正日付"]),
                "bugyo_shusei_sha_urage_denpyo_base": row["修正者名"],
                "bugyo_souko_code": row["倉庫コード"],
                "bugyo_syohin_code_shurui_code": row["商品コード種類コード"],
                "bugyo_tani_genka_urage_denpyo_base": row["単位原価"],
                "bugyo_tanka": row["単価"],
                "price": row["単価"],  # "price"プロパティに"単価"を追加
                "bugyo_tanka_kubun": row["単価区分"],
                "bugyo_torihiki_zyotai_kubun_code": row["取引状態区分コード"],
                "bugyo_touroku_hiduke_uriage_denpyo_base": convert_to_utc_timestamp(row["登録日付"]),
                "bugyo_tourokusha_mei_uriage_denpyo_base": row["登録者名"],
                "bugyo_urage_kingaku_urage_denpyo_base": row["売上金額"],
                "bugyo_uriage_genka": row["売上原価"],
                "bugyo_uriage_kubun_code": row["売上区分コード"],
                "bugyo_uritanka": row["売単価"],
                "bugyo_zeikomi_kubun_code": row["税込区分コード"],
                "bugyo_zeiritsu": row["税率"],
                "bugyo_zeiritsu_kubun_code": row["税率区分コード"],
                "bugyo_zeiritsu_shubetu": row["税率種別"],
                "n2": row["売上原価2"],
                "no____": row["伝票No."],
                "sha512": row["sha512"],
                "sha512_contents": row["sha512_contents"],
                "quantity": row["数量"]
            }
        }
        
        line_items.append(line_item)
        if len(line_items) >= BATCH_SIZE:
            create_line_items(line_items)
            line_items.clear()
    
    if line_items:
        create_line_items(line_items)

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(script_dir, '新規だけ5.csv')  # CSVファイルの相対パスを指定
    print(f"Script directory: {script_dir}")
    print(f"CSV file path: {csv_file_path}")
    main(csv_file_path)
