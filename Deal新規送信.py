import pandas as pd
import requests
import json

# 定数
BATCH_SIZE = 100  # 一度に送信するバッチのサイズ

# CSVファイルを読み込む関数
def read_csv(file_path):
    return pd.read_csv(file_path, dtype=str)

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
        "修正日付": "bugyo_shuusei_hiduke",
        "修正者名": "bugyo_shuuseisha_mei",
        "担当者コード": "bugyo_tantousha_code",
        "摘要": "bugyo_tekiyou",
        "摘要2": "bugyo_tekiyou2",
        "摘要3": "bugyo_tekiyou3",
        "得意先コード": "bugyo_tokuisaki_code",
        "得意先名1": "bugyo_tokuisaki_mei1",
        "得意先名2": "bugyo_tokuisaki_mei2",
        "得意先担当者": "bugyo_tokuisaki_tantousha",
        "登録日付": "bugyo_touroku_hiduke",
        "登録者名": "bugyo_tourokusha_mei",
        "売上日付": "bugyo_uriage_hiduke",
        "税額通知コード": "bugyo_zeigaku_tsuuchi_code",
        "伝票No.": "no_____",
        "sha512": "sha512",
        "sha512_origin_key_": "sha512_origin_key_",
        "sha512_removed_update_date": "sha512_removed_update_date"
    }

    deals = []
    for _, row in df.iterrows():
        properties = {}
        for csv_name, internal_name in properties_mapping.items():
            properties[internal_name] = row[csv_name]
        deals.append({"properties": properties})
    return deals

# バッチを送信する関数
def send_batch(deals_batch):
    url = 'https://api.hubapi.com/crm/v3/objects/deals/batch/create'
    headers = {
        'Authorization': 'Bearer xxxxxxxxxxxxxxxxxxxxxxxxxx',
        'Content-Type': 'application/json'
    }
    data = {"inputs": deals_batch}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()

# メイン関数
def main(file_path):
    df = read_csv(file_path)
    all_deals = transform_data(df)

    # バッチ処理
    for i in range(0, len(all_deals), BATCH_SIZE):
        batch = all_deals[i:i+BATCH_SIZE]
        response = send_batch(batch)
        print(f"Batch {i//BATCH_SIZE + 1} response: ", response)

# 実行
if __name__ == "__main__":
    csv_file_path = 'C:\Users\Administrator\OneDrive - 株式会社レボル\OBCsalesdata/新規だけ4.csv'  # CSVファイルのパスを指定
    main(csv_file_path)
