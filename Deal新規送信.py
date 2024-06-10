import pandas as pd
import requests
import json

# �萔
BATCH_SIZE = 100  # ��x�ɑ��M����o�b�`�̃T�C�Y

# CSV�t�@�C����ǂݍ��ފ֐�
def read_csv(file_path):
    return pd.read_csv(file_path, dtype=str)

# �f�[�^��ϊ�����֐�
def transform_data(df):
    properties_mapping = {
        "����X�e�[�W": "dealstage",
        "�p�C�v���C��": "pipeline",
        "���v����": "amount",
        "�N���[�Y��": "closedate",
        "�����": "dealname",
        "����R�[�h": "bugyo_bumon_code",
        "�������E": "bugyo_chokusosaki_yakushoku",
        "������R�[�h": "bugyo_chokusou_saki_code",
        "������d�b�ԍ�": "bugyo_chokusou_saki_denwa_bangou",
        "������FAX�ԍ�": "bugyo_chokusou_saki_fax_bangou",
        "������Z��1": "bugyo_chokusou_saki_juusho1",
        "������Z��2": "bugyo_chokusou_saki_juusho2",
        "������h��": "bugyo_chokusou_saki_keishou",
        "�����於1": "bugyo_chokusou_saki_mei1",
        "�����於2": "bugyo_chokusou_saki_mei2",
        "������S����": "bugyo_chokusou_saki_tantousha",
        "������X�֔ԍ�": "bugyo_chokusou_saki_yuubin_bangou",
        "�`�[�t���O�R�[�h": "bugyo_denpyou_flag_code",
        "��ID": "bugyo_juchuu_id",
        "�󒍖���ID": "bugyo_juchuu_meisai_id",
        "�������": "bugyo_kaishuu_kijitsu",
        "�����E�v": "bugyo_nyuukin_tekiyou",
        "�v���W�F�N�g�R�[�h": "bugyo_project_code",
        "������R�[�h": "bugyo_seikyuu_saki_code",
        "�M�̉�ЃR�[�h": "bugyo_shinpan_kaisha_code",
        "�M�̎萔��": "bugyo_shinpan_tesuuryou",
        "�C�����t": "bugyo_shuusei_hiduke",
        "�C���Җ�": "bugyo_shuuseisha_mei",
        "�S���҃R�[�h": "bugyo_tantousha_code",
        "�E�v": "bugyo_tekiyou",
        "�E�v2": "bugyo_tekiyou2",
        "�E�v3": "bugyo_tekiyou3",
        "���Ӑ�R�[�h": "bugyo_tokuisaki_code",
        "���Ӑ於1": "bugyo_tokuisaki_mei1",
        "���Ӑ於2": "bugyo_tokuisaki_mei2",
        "���Ӑ�S����": "bugyo_tokuisaki_tantousha",
        "�o�^���t": "bugyo_touroku_hiduke",
        "�o�^�Җ�": "bugyo_tourokusha_mei",
        "������t": "bugyo_uriage_hiduke",
        "�Ŋz�ʒm�R�[�h": "bugyo_zeigaku_tsuuchi_code",
        "�`�[No.": "no_____",
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

# �o�b�`�𑗐M����֐�
def send_batch(deals_batch):
    url = 'https://api.hubapi.com/crm/v3/objects/deals/batch/create'
    headers = {
        'Authorization': 'Bearer xxxxxxxxxxxxxxxxxxxxxxxxxx',
        'Content-Type': 'application/json'
    }
    data = {"inputs": deals_batch}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()

# ���C���֐�
def main(file_path):
    df = read_csv(file_path)
    all_deals = transform_data(df)

    # �o�b�`����
    for i in range(0, len(all_deals), BATCH_SIZE):
        batch = all_deals[i:i+BATCH_SIZE]
        response = send_batch(batch)
        print(f"Batch {i//BATCH_SIZE + 1} response: ", response)

# ���s
if __name__ == "__main__":
    csv_file_path = 'C:\Users\Administrator\OneDrive - ������Ѓ��{��\OBCsalesdata/�V�K����4.csv'  # CSV�t�@�C���̃p�X���w��
    main(csv_file_path)
