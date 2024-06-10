import pandas as pd
import hashlib

# CSVファイルの読み込み時に全てのカラムを文字列として扱う
df = pd.read_csv('中間ファイル5.csv', dtype=str)

# 必要なカラム名（SHA512用）
columns_sha512 = [
    '伝票No.', '売上区分コード', '出荷区分', '商品コード種類コード', '商品コード', '商品名', '商品名2', '商品名3', '商品名4', 
    '商品名5', '商品名6', '注文No.', '倉庫コード', '単価区分', '入数', '入数2', '箱数', '数量', '単位', '単価', '単位原価', 
    '売単価', '売上金額', '売上原価', '売上原価2', '課税区分コード', '取引状態区分コード', '税率種別', '税率区分コード', 
    '税率', '税込区分コード', '原価税込区分コード', '入数小数桁', '入数2小数桁', '箱数小数桁', '数量小数桁', '単価小数桁', 
    '消費税', '原価消費税', '同時処理コード', '仕入先コード', '備考', '付箋色コード', '付箋メモ'
]

# 必要なカラム名（SHA512_contents用）
columns_sha512_contents = columns_sha512 + ['修正日付', '修正者名', '登録日付', '登録者名']

# 必要なカラムがすべて存在することを確認
for col in columns_sha512_contents:
    if col not in df.columns:
        raise ValueError(f"必要なカラム {col} がCSVファイルに存在しません。")

# SHA-512を生成する関数
def create_sha512(row, cols):
    combined = ','.join('' if pd.isna(row[col]) else str(row[col]) for col in cols)
    return hashlib.sha512(combined.encode()).hexdigest()

# SHA-512ハッシュを生成して'sha512'カラムに追加
df['sha512'] = df.apply(lambda row: create_sha512(row, columns_sha512_contents), axis=1)

# SHA-512ハッシュを生成して'SHA512_contents'カラムに追加
df['sha512_contents'] = df.apply(lambda row: create_sha512(row, columns_sha512), axis=1)

# 元のCSVファイルに上書き保存
df.to_csv('中間ファイル5.csv', index=False)

print("SHA-512ハッシュを生成して'sha512'および'sha512_contents'に追加し、元のファイルに上書き保存しました。")
