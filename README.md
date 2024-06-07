# コードの説明と利用方法

このコードは、HubSpotのAPIを使用して会社間の関連付けを管理するPythonプログラムです。以下の手順で動作します：

1. **アクセストークンの取得**:
   - 環境変数からHubSpotのアクセストークンを取得し、APIリクエストのヘッダーに設定します。

2. **入力プロパティの取得**:
   - 関連付けの元のレコードID (`source_record_id`) と、関連付けたい先のキーが入っているプロパティ (`source_property_with_association_key`) を取得します。

3. **変数の設定**:
   - 関連付けに使用するプロパティ名 (`property_name`)、元と先のオブジェクトタイプ (`from_object_type`、`to_object_type`)、関連付けのタイプID (`association_type_id`)、ラベル (`association_label`)、およびデバッグ用の名前 (`source_name`、`target_name`) を設定します。

4. **関連付けの方向と削除フラグの設定**:
   - 元と先のオブジェクトタイプが同じ場合にのみ、`direction` と `delete_reverse` を設定します。それ以外の場合はデフォルト値を使用します。

5. **既存の関連付けの確認と削除**:
   - 既存の関連付けを取得し、指定された条件に一致する関連付けを削除します。

6. **新しい関連付けの作成**:
   - `source_property_with_association_key` が `Null` でない場合、指定されたプロパティを使用して会社を検索し、新しい関連付けを作成します。

7. **エラーハンドリング**:
   - 例外が発生した場合は、エラーメッセージとデバッグ情報を返します。

## 変数の説明
- **`access_token`**: HubSpotのAPI認証に使用するアクセストークン。
- **`headers`**: APIリクエストに必要なヘッダー情報。
- **`record_id`**: 関連付けの元となるレコードのID。
- **`property_name`**: 関連付け先の関連付け対象のキーのプロパティ名。（ユニークなプロパティを推奨）
- **`source_property_with_association_key`**: property_nameと一致するキーが格納されているプロパティ。
- **`from_object_type`**: 関連付けの元となるオブジェクトの種類。
- **`to_object_type`**: 関連付け先のオブジェクトの種類。
- **`association_type_id`**: 関連付けのタイプID。
- **`association_label`**: 関連付けのラベル。
- **`source_name`**: デバッグ用の関連付けの元の名前。
- **`target_name`**: デバッグ用の関連付け先の名前。
- **`direction`**: 関連付けの方向（順方向または逆方向）。
- **`delete_reverse`**: 逆方向の関連付けを削除するかどうかのフラグ。

  コードに含めるプロパティーに設定

  キー：source_property_with_association_key　プロパティは関連付けたい先のキーが格納されているプロパティを指定してください
  キー：source_record_id　プロパティは関連付けの元となるレコードのIDのプロパティを指定してください

## プログラムの利用上の注意点

1. **アクセストークンの管理**:
   - アクセストークンは環境変数に設定されている必要があります。公開されないように注意してください。

2. **入力プロパティの確認**:
   - `source_record_id` と `source_property_with_association_key` は必須入力です。適切に設定されていることを確認してください。

3. **プロパティ名の設定**:
   - `property_name` は正しいプロパティ名を設定してください。間違ったプロパティ名を設定すると、期待する結果が得られません。

4. **オブジェクトタイプの設定**:
   - `from_object_type` と `to_object_type` は正しいオブジェクトタイプを設定してください。オブジェクトタイプが異なる場合、`direction` と `delete_reverse` はデフォルト値（0）を使用します。

5. **関連付けタイプIDとラベルの確認**:
   - `association_type_id` と `association_label` は正しい値を設定してください。HubSpotが発行する値を使用します。

6. **デバッグ情報の利用**:
   - `debug_message` はエラーハンドリングや結果の確認に使用されます。デバッグ情報を活用して問題を特定してください。

7. **APIリクエストの制限**:
   - APIリクエストには制限があります。頻繁なリクエストを避け、適切なレート制限を守ってください。

8. **エラーハンドリング**:
   - プログラムは例外が発生した場合にエラーメッセージを返します。エラーメッセージを確認し、適切に対処してください。

9. **テスト環境での確認**:
   - 実運用前にテスト環境で十分にテストを行い、問題がないことを確認してください。

以上の注意点を守ることで、このプログラムを安全かつ効果的に利用することができます。




```Python
import os  # OSモジュールをインポートして環境変数にアクセスするために使用します
import requests  # HTTPリクエストを行うためのrequestsモジュールをインポートします

def main(event):
    # 環境変数からHubSpotのアクセストークンを取得します
    access_token = os.getenv('HubSpotSelf')
    headers = {  # リクエストヘッダーを設定します
        "Content-Type": "application/json",  # リクエストのコンテンツタイプを指定します
        "Authorization": f"Bearer {access_token}"  # 認証ヘッダーを設定します
    }
    
    # 入力プロパティから請求先コードとレコードIDを取得します
    record_id = event['inputFields']['source_record_id']  # 関連付けの元のレコード番号
    source_property_with_association_key = event['inputFields'].get('source_property_with_association_key')  # 関連付けたい先のキーが入っている、関連付けの元側のプロパティを指定します。   
    # 関連付けに使用する変数を設定します
    property_name = 'bugyo_tokuisaki_code_unique'  # 関連付け先のキーのプロパティ名。プロパティ一覧から探せます。
    from_object_type = 'companies'  # 関連付けの元のオブジェクトタイプ オブジェクト一覧)companies（会社）, contacts（連絡先）, deals（取引）, tickets（チケット）, line_items（商品）, quotes（見積もり）, products（製品）, calls（通話）, emails（メール）, tasks（タスク）, meetings（会議）, notes（ノート）
    to_object_type = 'companies'  # 関連付け先のオブジェクトタイプ。 オブジェクト一覧)companies（会社）, contacts（連絡先）, deals（取引）, tickets（チケット）, line_items（商品）, quotes（見積もり）, products（製品）, calls（通話）, emails（メール）, tasks（タスク）, meetings（会議）, notes（ノート）
    association_type_id = 12  # 該当関連付けの「APIの詳細」に記載がある関連付けのタイプID。HubSpotが発行します。
    association_label = '請求元'  # 該当関連付けの「APIの詳細」に記載がある関連付けのラベル。HubSpotが発行します。
    source_name = '請求元'  # デバッグ用表示に使う関連付けの元のラベルの名前 *任意
    target_name = '請求先'  # デバッグ用表示に使う関連付け先のラベルの名前 *任意

    # 関連付けの方向と削除フラグを設定します（関連付け元と先のオブジェクトタイプが同じ場合のみ機能します）
    if from_object_type == to_object_type:
        direction = event['inputFields'].get('direction', 0)  # 関連付け方向を逆方向にする場合は1。デフォルトは0（逆方向）
        delete_reverse = event['inputFields'].get('delete_reverse', 0)  # 逆方向の関連付けを削除するかどうかのフラグ。デフォルトは0
    else:
        direction = 0  # デフォルトは0（順方向）
        delete_reverse = 0  # 逆方向の関連付けを削除しない

    try:
        # 既存の関連付けを確認するためのURLを設定します
        list_url = f"https://api.hubapi.com/crm/v4/objects/{from_object_type}/{record_id}/associations/{to_object_type}"
        list_response = requests.get(list_url, headers=headers)  # 既存の関連付けを取得します
        list_result = list_response.json()  # 取得した結果をJSON形式で解析します
        
        debug_message = f"既存の関連付け: {list_result}"  # デバッグメッセージに既存の関連付け情報を追加します
        
        # 既存の関連付けを削除するための準備をします
        to_delete = []
        if 'results' in list_result:  # 結果が存在する場合
            to_delete = [
                {
                    "from": {"id": str(record_id)},  # 削除する元のIDを設定します
                    "to": [{"id": str(assoc['toObjectId'])}]  # 削除する対象のIDを設定します
                }
                for assoc in list_result['results']
                if any(
                    atype['typeId'] == association_type_id or  # 順方向のタイプIDに一致する場合
                    (delete_reverse == 1 and atype['typeId'] == association_type_id + 1)  # 逆方向のタイプIDに一致する場合
                    for atype in assoc['associationTypes']
                )
            ]
            
            if to_delete:  # 削除対象が存在する場合
                delete_url = f"https://api.hubapi.com/crm/v4/associations/{from_object_type}/{to_object_type}/batch/archive"
                delete_request = {"inputs": to_delete}  # 削除リクエストのボディを設定します
                delete_response = requests.post(delete_url, headers=headers, json=delete_request)  # 削除リクエストを送信します
                debug_message += f", 削除リクエスト: {delete_request}, 削除結果: {delete_response.status_code}, 削除応答: {delete_response.text}"  # デバッグメッセージに削除リクエスト情報を追加します
                
                if delete_response.status_code not in [200, 202, 204]:  # 削除が成功したか確認します
                    return {
                        "outputFields": {
                            "message": f"既存の関連付けの削除に失敗しました。",
                            "debug": debug_message
                        }
                    }

                # 削除後の確認を行います
                confirm_list_response = requests.get(list_url, headers=headers)  # 再度関連付けを取得します
                confirm_list_result = confirm_list_response.json()  # 取得結果をJSON形式で解析します
                debug_message += f", 削除後の確認: {confirm_list_result}"  # デバッグメッセージに削除後の確認情報を追加します
                
                if any(
                    atype['typeId'] == association_type_id or 
                    (delete_reverse == 1 and atype['typeId'] == association_type_id + 1)
                    for assoc in confirm_list_result.get('results', [])
                    for atype in assoc['associationTypes']
                ):  # 削除が完全に行われたか確認します
                    return {
                        "outputFields": {
                            "message": f"関連付けの削除が確認できませんでした。",
                            "debug": debug_message
                        }
                    }
        
        # source_property_with_association_key が Null でない場合のみ会社の検索と新しい関連付けを作成します
        if source_property_with_association_key:
            search_url = f"https://api.hubapi.com/crm/v3/objects/{to_object_type}/search"  # 会社を検索するためのURLを設定します
            search_request = {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName":　property_name,
                                "operator": "EQ",
                                "value": source_property_with_association_key  # 請求先コードで会社を検索します
                            }
                        ]
                    }
                ]
            }
            
            search_response = requests.post(search_url, headers=headers, json=search_request)  # 検索リクエストを送信します
            search_result = search_response.json()  # 検索結果をJSON形式で解析します
            
            if search_result['total'] == 0:  # 検索結果が存在しない場合
                return {
                    "outputFields": {
                        "message": f"一致する{target_name}が見つかりませんでした。",
                        "debug": debug_message
                    }
                }
            
            target_id = search_result['results'][0]['id']  # 検索結果から対象IDを取得します
            debug_message += f", 検索結果: {search_result['results']}, {target_name}会社ID: {target_id}"  # デバッグメッセージに検索結果を追加します
            
            # 方向に応じて関連付けを設定します
            if direction == 1:  # 逆方向の場合
                association_url = f"https://api.hubapi.com/crm/v4/objects/{to_object_type}/{target_id}/associations/{from_object_type}/{record_id}"
                source_name, target_name = target_name, source_name  # ソースとターゲットを入れ替えます
            else:  # 順方向の場合
                association_url = f"https://api.hubapi.com/crm/v4/objects/{from_object_type}/{record_id}/associations/{to_object_type}/{target_id}"
            
            association_request = [
                {
                    "associationCategory": "USER_DEFINED",
                    "associationTypeId": association_type_id  # 関連付けタイプIDを設定します
                }
            ]
            
            association_response = requests.put(association_url, headers=headers, json=association_request)  # 関連付けリクエストを送信します
            association_result = association_response.json()  # 結果をJSON形式で解析します
            debug_message += f", 関連付けリクエスト: {association_request}, 関連付け結果: {association_result}"  # デバッグメッセージに関連付け結果を追加します
            
            return {
                "outputFields": {
                    "message": f"{source_name}({record_id}) と {target_name}({target_id}) の関連付けが成功しました。",
                    "debug": debug_message  # 関連付けの成功メッセージとデバッグ情報を返します
                }
            }
        
        # source_property_with_association_key が Null の場合の処理を行います
        return {
            "outputFields": {
                "message": f"source_property_with_association_key が Null のため、検索と関連付けはスキップされました。",  # source_property_with_association_keyがNullの場合のメッセージを設定します
                "debug": debug_message  # デバッグメッセージを設定します
            }
        }
        
    except Exception as e:  # 例外が発生した場合の処理を行います
        return {
            "outputFields": {
                "message": f"エラーが発生しました: {e}",  # エラーメッセージを設定します
                "debug": debug_message if 'debug_message' in locals() else "デバッグ情報なし"  # デバッグメッセージを返します
            }
        }
```
