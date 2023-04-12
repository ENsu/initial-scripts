import os
import re
import io
from dotenv import dotenv_values
import boto3
import pandas as pd
from publicsuffix2 import PublicSuffixList
from urllib.parse import urlparse
from datetime import datetime, timedelta
import requests
import random
import string
import numpy as np


secrets_env = dotenv_values(".secrets")


def get_env_var(key):
    value = secrets_env.get(key) or os.environ.get(key)
    assert value is not None, f"Failed to get value from {key}"
    return value


S3_KEY_ID = get_env_var("S3_KEY_ID")
S3_SECRET_KEY = get_env_var("S3_SECRET_KEY")
HL_S3_KEY_ID = get_env_var("HL_S3_KEY_ID")
HL_S3_SECRET_KEY = get_env_var("HL_S3_SECRET_KEY")


session = boto3.Session(aws_access_key_id=S3_KEY_ID, aws_secret_access_key=S3_SECRET_KEY)
s3_client = session.client('s3')
s3_bucket_name = 'infinity-airflow'

hl_session = boto3.Session(aws_access_key_id=HL_S3_KEY_ID, aws_secret_access_key=HL_S3_SECRET_KEY)
hl_s3_client = hl_session.client('s3')
hl_bucket_name = 'jp-tw-startup-crawl'

jp_postal_obj = s3_client.get_object(Bucket=s3_bucket_name, Key="jp_postal_code_to_geo_info.csv")
jp_postal_df = pd.read_csv(io.BytesIO(jp_postal_obj['Body'].read()), encoding='utf8')

get_postal_by_addr_re = r'\b(\d{3}\-\d{4})\b'


def get_postal_by_addr(addr):
    try:
        return re.search(get_postal_by_addr_re, addr).group(1)
    except (AttributeError, TypeError):
        # print("Found not postal from {}".format(addr))
        return None


def get_prefecture_by_addr(addr):
    global jp_postal_df
    postal = get_postal_by_addr(addr)
    if postal:
        postal = postal.replace("-", "")
        prefectures = jp_postal_df.loc[jp_postal_df['code'] == int(postal), 'prefecture'].values
        if prefectures.size > 0:
            return prefectures[0].capitalize()
    return None


def get_city_by_addr(addr):
    global jp_postal_df
    postal = get_postal_by_addr(addr)
    if postal:
        postal = postal.replace("-", "")
        cities = jp_postal_df.loc[jp_postal_df['code'] == int(postal), 'city'].values
        if cities.size > 0:
            return cities[0]
    return None


def cleanup_prefectur_info(prefecture_sereis):
    # location related preprocessing
    prefecture_sereis = prefecture_sereis.apply(lambda x: x.lower().replace("prefecture", "").strip().capitalize() if pd.notnull(x) else x)
    prefecture_name_mapping = {
        "東京都": "Tokyo",
        "大阪府": "Osaka",
        "神奈川県": "Kanagawa",
        "京都府": "Kyoto",
        "愛知県": "Aichi",
        "兵庫県": "Hyogo",
        "千葉県": "Chiba",
        "熊本県": "Kumamoto",
        "新潟県": "Niigata",
        "大分県": "Oita",
        "沖縄県": "Okinawa",
        "福井県": "Fukui",
        "北海道": "Hokkaido",
        "宮城県": "Miyagi",
        "岡山県": "Okayama",
    }
    return prefecture_sereis.apply(lambda x: prefecture_name_mapping.get(x, x))


psl = PublicSuffixList()


def get_base_domain(url: str) -> str:
    if pd.isnull(url):
        return None
    if not (url.startswith("http://") or url.startswith("https://")):
        url = "https://" + url
    hostname = urlparse(url).hostname
    return psl.get_public_suffix(hostname)


def _get_exch_rate(base_cur, to_cur, date=None):
    assert len(to_cur) == 3, f'Not valid currency: {to_cur}'
    assert len(base_cur) == 3, f'Not valid base currency: {base_cur}'

    date_str = 'latest' if date is None else str(date)
    url = f'https://api.exchangerate.host/{date_str}'  # HTTPS only for paid subscriptions
    params = {
        "base": base_cur,
        'symbols': to_cur,
        'amount': 1
    }

    resp = requests.get(url, params=params)
    if not resp.ok:
        resp.raise_for_status()

    data = resp.json()
    return data['rates'][to_cur]


forex_obj = s3_client.get_object(Bucket=s3_bucket_name, Key="forexrec.csv")
forex = pd.read_csv(io.BytesIO(forex_obj['Body'].read()), encoding='utf8')


def get_usd(cur, date, value):
    global forex
    if pd.isnull(value) or pd.isnull(date) or pd.isnull(cur):
        return None
    if value == 0:
        return 0
    value = float(value)
    date = max(datetime.strptime(date, "%Y-%m-%d"), datetime(1999, 2, 1))
    date = min(date, datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    rate_data = forex[(forex['base_cur']==cur) & (forex['date'] == date) & (forex['dest_cur'] == 'USD')]
    if len(rate_data) == 0:
        print(f"missing and get rate for JPY to USD in {date}")
        rate = _get_exch_rate(cur, 'USD', date=date)
        new_dict = {"date": date, "base_cur": cur, "dest_cur": "USD", "rate": rate}
        forex = pd.concat([forex, pd.DataFrame([new_dict])], axis=0, ignore_index=True)

        # save back to s3 file
        csv_buffer = io.StringIO()
        forex.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        s3_client.put_object(Bucket=s3_bucket_name, Body=csv_buffer.getvalue(), Key='forexrec.csv')
    elif len(rate_data) == 1:
        rate = rate_data['rate'].values[0]
    else:
        raise Exception(f"Get multiple rows with same cur: {cur}, date: {date}, data: {rate_data}")
    return value * rate


def split2row_with_index(df, colnames, show_index=False):

    def _split1col2row_with_index(df, colname, show_index):
        tmp = df[colname].apply(pd.Series, 1).stack().reset_index()
        tmp.index = tmp['level_0']
        tmp.index.name = ''
        tmp.drop(['level_0'], axis=1, inplace=True)
        tmp.columns = ['list_index', colname]
        if not show_index:
            tmp = tmp.drop('list_index', axis=1)
        return df.drop([colname], axis=1).join(tmp)

    def _transpose(x):
        x = np.transpose(np.array([np.array(i) for i in x]))
        return [tuple(i) for i in x]

    if isinstance(colnames, str):
        return _split1col2row_with_index(df, colnames, show_index)
    elif not isinstance(colnames, list):
        raise Exception(
            f"Colnames: {colnames} with type {type(colnames)} not permitted.")

    tmp = df.copy()
    rand_name = ''.join(random.choice(string.ascii_letters) for _ in range(10))
    tmp[rand_name] = tmp[colnames].apply(lambda x: _transpose(x), axis=1)
    tmp = _split1col2row_with_index(tmp.drop(colnames, axis=1), rand_name, show_index)
    for i, c in enumerate(colnames):
        tmp[c] = tmp[rand_name].apply(lambda x: x[i])
    return tmp.drop(rand_name, axis=1)


def json_col_to_df(df, col_name, prefix=True, keys_drop=[], chunk_size=int(1e5)):
    assert df.index.is_unique, "index is not unique"

    def _json_col_to_df(_df, col_name, prefix, keys_drop):
        json_df = _df[col_name].copy()
        _df = _df.drop([col_name], axis=1)
        if len(json_df[json_df.notnull()].values) > 0 and isinstance(json_df[json_df.notnull()].values[0], str):
            json_df.loc[json_df.notnull()] = json_df.loc[json_df.notnull()].swifter.apply(ast.literal_eval)
        json_df.loc[json_df.isnull()] = [{}] * json_df.isnull().sum()
        json_df = pd.DataFrame(data=json_df.tolist())
        if prefix is True:
            json_df.columns = [f"{col_name}_{c}" for c in json_df.columns]
        json_df_len = len(json_df)
        for c in json_df.columns:
            if c not in keys_drop:
                _df[c] = json_df[c].values
        assert json_df_len == len(_df), "df length changed!"
        return _df

    my_list = []
    for start in range(0, len(df), chunk_size):
        end = min(start+chunk_size, len(df))
        my_list.append(_json_col_to_df(df.iloc[start:end, :], col_name, prefix, keys_drop))

    return pd.concat(my_list, axis=0)


def export_to_s3(df, file_name):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3_client.put_object(Bucket=s3_bucket_name, Body=csv_buffer.getvalue(), Key=f'exports/{file_name}')
    hl_s3_client.put_object(Bucket=hl_bucket_name, Body=csv_buffer.getvalue(), Key=file_name)


def validate_datetime_str_format(date_str, blankable=False):
    if date_str == "" and blankable:
        return True
    try:
        datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        print(f"Incorrect data format: {date_str}, should be %Y-%m-%d %H:%M:%S")
        return False
    except TypeError:
        print(f"Incorrect data type: {type(date_str)}, should be str")
        return False
    

def validate_date_str_format(date_str, blankable=False):
    if date_str == "" and blankable:
        return True
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        print(f"Incorrect data format: {date_str}, should be %Y-%m-%d")
        return False
    except TypeError:
        print(f"Incorrect data type: {type(date_str)}, should be str")
        return False
