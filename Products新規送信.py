import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta, timezone
import traceback
import sys

# 定数
BATCH_SIZE = 100 # 一度に送信するバッチのサイズ
JST = timezone(timedelta(hours=+9)) # 日本標準時 (JST)
UTC = timezone.utc # UTCタイムゾーン

# CSVファイルを読み込む関数
def read_csv(file_path):
    print(f"Reading CSV file from: {file_path}")
    df = pd.read_csv(file_path, dtype=str) # すべてのカラムを文字列として読み込む
    df = df.fillna('') # 欠損値を空文字に置き換える
    return df

# 日付をUTCの深夜0時のタイムスタンプに変換する関数
def convert_to_utc_midnight_timestamp(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y/%m/%d").replace(tzinfo=UTC)
        dt_midnight_utc = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        timestamp = int(dt_midnight_utc.timestamp() * 1000) # ミリ秒に変換
        return timestamp
    except ValueError:
        print(f"Skipping invalid date: {date_str}")
        return '' # 日付形式でない場合は空文字を返す

# 日時をUTCのタイムスタンプに変換する関数
def convert_to_utc_timestamp(datetime_str):
    try:
        dt = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S").replace(tzinfo=JST)
        dt_utc = dt.astimezone(UTC)
        timestamp = int(dt_utc.timestamp() * 1000) # ミリ秒に変換
        return timestamp
    except ValueError:
        print(f"Skipping invalid datetime: {datetime_str}")
        return '' # 日付形式でない場合は空文字を返す

# データを変換する関数
def transform_data(df):
    properties_mapping = {
        "標準価格（税抜）": ["hs_price_jpy", "hyoujun_kakaku_zeinuki"],
        "売価No.１（税込）": "baika_no1_zeikomi",
        "売価No.１（税抜）": "baika_no1_zeinuki",
        "売価No.２（税込）": "baika_no2_zeikomi",
        "売価No.２（税抜）": "baika_no2_zeinuki",
        "売価No.３（税込）": "baika_no3_zeikomi",
        "売価No.３（税抜）": "baika_no3_zeinuki",
        "売価No.４（税込）": "baika_no4_zeikomi",
        "売価No.４（税抜）": "baika_no4_zeinuki",
        "売価No.５（税込）": "baika_no5_zeikomi",
        "売価No.５（税抜）": "baika_no5_zeinuki",
        "標準価格（税込）": "hyoujun_kakaku_zeikomi",
        "仕入原価（税込）": "siire_genka_zeikomi",
        "仕入原価（税抜）": "siire_genka_zeinuki",
        "単価小数桁": "tanka_shosu_keta",
        "単位原価（税込）": "tanni_genka_zeikomi",
        "単位原価（税抜）": "tanni_genka_zeinuki",
        "在庫単価（税抜）": "zaiko_tanka_zeinuki",
        "税区分（販売）": "zei_kubun_hanbai",
        "税区分（仕入）": "zei_kubun_shiire",
        "税込区分（販売）": "zeikomi_kubun_hanbai",
        "税込区分（仕入）": "zeikomi_kubun_shiire",
        "税率種別（販売）": "zeiritsu_shubetsu_hanbai",
        "税率種別（仕入）": "zeiritsu_shubetsu_shiire",
        "台帳インデックス": "daichou_index",
        "発注区分": "hacchuu_kubun",
        "発注単位数量": "hacchuu_tanni_suuryou",
        "発注点": "hacchuu_ten",
        "箱数小数桁": "hakosuu_shosu_keta",
        "個別管理": "kobetsu_kanri",
        "個別対応": "kobetsu_taiou",
        "共用区分": "kyouyou_kubun",
        "明細区分": "meisai_kubun",
        "メモ１": "memo1",
        "メモ２": "memo2",
        "メモ３": "memo3",
        "商品名": "name",
        "入数": "nyuusuu",
        "入数小数桁": "nyuusuu_shosu_keta",
        "利用状態": "riyou_joutai",
        "最高点": "saikou_ten",
        "主仕入先コード": "shu_shiire_saki_code",
        "数量小数桁": "suuryou_shosu_keta",
        "単位": "tanni",
        "有効期間（開始）": "yuukou_kikan_kaishi",
        "有効期間（終了）": "yuukou_kikan_shuryou",
        "在庫管理": "zaiko_kanri",
        "入数２": "bugyo_iri_su2",
        "入数２小数桁": "bugyo_irisu2_shousu_keta",
        "商品名２": "bugyo_shohinmei2",
        "商品名３": "bugyo_shohinmei3",
        "商品名４": "bugyo_shohinmei4",
        "商品名５": "bugyo_shohinmei5",
        "商品名６": "bugyo_shohinmei6",
        "商品区分３コード": "bugyo_shouhin_kubun_3_code",
        "商品区分２コード": "bugyo_shouhin_kubun_2_code",
        "商品区分１コード": "bugyo_shouhin_kubun_1_code",
        "商品コード２": "bugyo_shouhin_code_2",
        "商品コード３": "bugyo_shouhin_code_3",
        "商品コード４": "bugyo_shouhin_code_4",
        "商品コード５": "bugyo_shouhin_code_5",
        "印刷用商品コード": "bugyo_insatsuyou_syouhin_code",
        "引当管理": "bugyo_hikiate_kanri",
        "ロット管理": "bugyo_lot_kanri",
        "在庫評価方法": "bugyo_zaiko_hyouka_houhou",
        "主倉庫コード": "bugyo_shu_souko_code",
        "主ロケーションNo.": "bugyo_shu_location_no",
        "出荷予定日設定": "bugyo_shukka_yoteibi_settei",
        "発注納品期日設定": "bugyo_hacchuu_nouhin_kijitsu_settei",
        "期限サイクル設定": "bugyo_kigen_cycle_settei",
        "期限サイクル（月）": "bugyo_kigen_cycle_month",
        "期限サイクル（日）": "bugyo_kigen_cycle_day",
        "単価区分２単位当り単価区分１数": "bugyo_tanka_kubun_2_tani_atari_tanka_kubun_1_suu",
        "単価区分２単位": "bugyo_tanka_kubun_2_tani",
        "単価区分２単価区分内容": "bugyo_tanka_kubun_2_tanka_kubun_naiyou",
        "単価区分２単価区分備考": "bugyo_tanka_kubun_2_tanka_kubun_bikou",
        "単価区分３単位当り単価区分１数": "bugyo_tanka_kubun_3_tani_atari_tanka_kubun_1_suu",
        "単価区分３単位": "bugyo_tanka_kubun_3_tani",
        "単価区分３単価区分内容": "bugyo_tanka_kubun_3_tanka_kubun_naiyou",
        "単価区分３単価区分備考": "bugyo_tanka_kubun_3_tanka_kubun_bikou",
        "単価区分４単位当り単価区分１数": "bugyo_tanka_kubun_4_tani_atari_tanka_kubun_1_suu",
        "単価区分４単位": "bugyo_tanka_kubun_4_tani",
        "単価区分４単価区分内容": "bugyo_tanka_kubun_4_tanka_kubun_naiyou",
        "単価区分４単価区分備考": "bugyo_tanka_kubun_4_tanka_kubun_bikou",
        "単価区分５単位当り単価区分１数": "bugyo_tanka_kubun_5_tani_atari_tanka_kubun_1_suu",
        "単価区分５単位": "bugyo_tanka_kubun_5_tani",
        "単価区分５単価区分内容": "bugyo_tanka_kubun_5_tanka_kubun_naiyou",
        "単価区分５単価区分備考": "bugyo_tanka_kubun_5_tanka_kubun_bikou",
        "売上単価区分": "bugyo_uriage_tanka_kubun",
        "仕入単価区分": "bugyo_shiire_tanka_kubun",
        "登録日付": "bugyo_toroku_hizuke_syouhin_master_base",
        "登録者名": "bugyo_torokushamei_syouhin_master_base",
        "修正日付": "bugyo_shuusei_hizuke_syouhin_master_base",
        "修正者名": "bugyo_shuuseishamei_syouhin_master_base",
        "商品区分１名": "bugyo_revol_syouhin_shouhin_kubun_1_mei",
        "商品区分２名": "bugyo_tasha_maker_syouhin_shouhin_kubun_2_mei",
        "商品区分３名": "bugyo_sonota_syouhin_shouhin_kubun_3_mei",
        "sha512": "bugyo_sha512_syouhin_master_base",
        "sha512_contents": "bugyo_sha512_contents_syouhin_master_base",
        "商品コード": "shouhin_code"
    }
    products = []
    for _, row in df.iterrows():
        properties = {}
        for csv_name, internal_names in properties_mapping.items():
            if isinstance(internal_names, list):
                for internal_name in internal_names:
                    properties[internal_name] = row[csv_name]
            else:
                if internal_names in ["bugyo_toroku_hizuke_syouhin_master_base", "bugyo_shuusei_hizuke_syouhin_master_base"]:
                    timestamp = convert_to_utc_timestamp(row[csv_name])
                    properties[internal_names] = timestamp
                else:
                    properties[internal_names] = row[csv_name]
        products.append({"properties": properties})
    return products

# バッチを送信する関数
def send_batch(products_batch):
    url = 'https://api.hubapi.com/crm/v3/objects/products/batch/create'
    headers = {
        'Authorization': 'Bearer xxxxxxxxxxxxxxxxxxxxxx',
        'Content-Type': 'application/json'
    }
    data = {"inputs": products_batch}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()

# メイン関数
def main(file_path):
    try:
        print("Reading CSV file...")
        df = read_csv(file_path)
        print("Transforming data...")
        all_products = transform_data(df)
        print("Sending batches...")
        # バッチ処理
        for i in range(0, len(all_products), BATCH_SIZE):
            batch = all_products[i:i+BATCH_SIZE]
            response = send_batch(batch)
            print(f"Batch {i//BATCH_SIZE + 1} response: ", response)
    except Exception as e:
        print(f"Error occurred: {e}")
        print(traceback.format_exc())

# 実行
if __name__ == "__main__":
    if getattr(sys, 'frozen', False):
        script_dir = os.path.dirname(sys.executable)
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    
    csv_file_path = os.path.join(script_dir, '新規だけ商品.csv') # CSVファイルの相対パスを指定
    print(f"Script directory: {script_dir}")
    print(f"CSV file path: {csv_file_path}")
    main(csv_file_path)
