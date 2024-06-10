import pandas as pd
import hashlib

# CSVファイルの読み込み時に全てのカラムを文字列として扱う
df = pd.read_csv('中間ファイル4.csv', dtype=str)

# 必要なカラム名（SHA512用）
columns_sha512 = [
    '伝票区分コード', '売上日付', '請求日付', '伝票No.', '受注ID', '受注明細ID', '得意先コード', '得意先名1', '得意先名2',
    '請求先コード', '税額通知コード', '得意先担当者', '部門コード', '担当者コード', 'プロジェクトコード', '信販会社コード',
    '摘要', '摘要2', '摘要3', '伝票フラグコード', '直送先コード', '直送先名1', '直送先名2', '直送先担当者', '直送先敬称',
    '直送先役職', '直送先郵便番号', '直送先住所1', '直送先住所2', '直送先電話番号', '直送先FAX番号', '回収期日', '信販手数料', '入金摘要', '合計売上'
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

# 新しい"SHA512"にSHA-512ハッシュを格納
df['sha512'] = df.apply(lambda row: create_sha512(row, columns_sha512_contents), axis=1)

# 新しい"SHA512_contents"にSHA-512ハッシュを格納
df['sha512_contents'] = df.apply(lambda row: create_sha512(row, columns_sha512), axis=1)

# 元のCSVファイルに上書き保存
df.to_csv('中間ファイル4.csv', index=False)

print("SHA-512ハッシュを生成して'SHA512'および'SHA512_contents'に追加し、元のファイルに上書き保存しました。")
