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

# データを変換する関数
def transform_data(df):
    properties_mapping = {
        "取引ステージ": "dealstage",
        "パイプライン": "pipeline",
        "合計売上": "amount",
        "クローズ日": "closedate",
        "取引名": "dealname",
        "部門コード": "bugyo_bumon_code",
        "直送先役職": "bugyo_chokusosaki_yakushoku",
        "直送先コード": "bugyo_chokusou_saki_code",
        "直送先電話番号": "bugyo_chokusou_saki_denwa_bangou",
        "直送先FAX番号": "bugyo_chokusou_saki_fax_bangou",
        "直送先住所1": "bugyo_chokusou_saki_juusho1",
        "直送先住所2": "bugyo_chokusou_saki_juusho2",
        "直送先敬称": "bugyo_chokusou_saki_keishou",
        "直送先名1": "bugyo_chokusou_saki_mei1",
        "直送先名2": "bugyo_chokusou_saki_mei2",
        "直送先担当者": "bugyo_chokusou_saki_tantousha",
        "直送先郵便番号": "bugyo_chokusou_saki_yuubin_bangou",
        "伝票フラグコード": "bugyo_denpyou_flag_code",
        "受注ID": "bugyo_juchuu_id",
        "受注明細ID": "bugyo_juchuu_meisai_id",
        "回収期日": "bugyo_kaishuu_kijitsu",
        "入金摘要": "bugyo_nyuukin_tekiyou",
        "プロジェクトコード": "bugyo_project_code",
        "請求先コード": "bugyo_seikyuu_saki_code",
        "信販会社コード": "bugyo_shinpan_kaisha_code",
        "信販手数料": "bugyo_shinpan_tesuuryou",
        "修正日付": "bugyo_shuusei_hizuke",
        "修正者名": "bugyo_shuuseisha_mei",
        "担当者コード": "bugyo_tantousha_code",
        "摘要": "bugyo_tekiyou",
        "摘要2": "bugyo_tekiyou2",
        "摘要3": "bugyo_tekiyou3",
        "得意先コード": "bugyo_tokuisaki_code",
        "得意先名1": "bugyo_tokuisaki_mei1",
        "得意先名2": "bugyo_tokuisaki_mei2",
        "得意先担当者": "bugyo_tokuisaki_tantousha",
        "登録日付": "bugyo_touroku_hizuke",
        "登録者名": "bugyo_tourokusha_mei",
        "売上日付": "bugyo_uriage_hiduke",
        "税額通知コード": "bugyo_zeigaku_tsuuchi_code",
        "伝票No.": "no_____",
        "sha512": "sha512",
        "sha512_contents": "sha512_contents"
    }

    # ステージ名とパイプライン名を対応するIDに変換するマッピング
    stage_mapping = {
        "FAX受注 (実績)": "149884283"
    }
    pipeline_mapping = {
        "実績": "79111068"
    }

    deals = []
    for _, row in df.iterrows():
        properties = {}
        for csv_name, internal_name in properties_mapping.items():
            if internal_name == "dealstage":
                properties[internal_name] = stage_mapping.get(row[csv_name], row[csv_name])
            elif internal_name == "pipeline":
                properties[internal_name] = pipeline_mapping.get(row[csv_name], row[csv_name])
            elif internal_name in ["closedate", "bugyo_uriage_hiduke"]:
                timestamp = convert_to_utc_midnight_timestamp(row[csv_name])
                properties[internal_name] = timestamp
            elif internal_name in ["bugyo_touroku_hizuke", "bugyo_shuusei_hizuke"]:
                timestamp = convert_to_utc_timestamp(row[csv_name])
                properties[internal_name] = timestamp
            else:
                properties[internal_name] = row[csv_name]
        deals.append({"properties": properties})
    return deals

# バッチを送信する関数
def send_batch(deals_batch):
    url = 'https://api.hubapi.com/crm/v3/objects/deals/batch/create'
    headers = {
        'Authorization': 'Bearer xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        'Content-Type': 'application/json'
    }
    data = {"inputs": deals_batch}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()

# メイン関数
def main(file_path):
    try:
        df = read_csv(file_path)
        all_deals = transform_data(df)

        # バッチ処理
        for i in range(0, len(all_deals), BATCH_SIZE):
            batch = all_deals[i:i+BATCH_SIZE]
            response = send_batch(batch)
            print(f"Batch {i//BATCH_SIZE + 1} response: ", response)
    except Exception as e:
        print(f"Error occurred: {e}")
        print(traceback.format_exc())

# 実行
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(script_dir, '新規だけ4.csv')  # CSVファイルの相対パスを指定
    print(f"Script directory: {script_dir}")
    print(f"CSV file path: {csv_file_path}")
    main(csv_file_path)
