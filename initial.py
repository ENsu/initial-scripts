from lxml import etree
import pandas as pd


def _my_cleanup(text):
    return text.replace("\u200b", "").replace("\xa0", "").strip()


def _get_cell_text(cell):
    texts = cell.xpath(".//text()")
    return [_my_cleanup(i) for i in texts if _my_cleanup(i)]


def _get_cell_anchor(cell):
    anchors = cell.xpath(".//a")
    return [{
        "name": _my_cleanup(a.xpath(".//text()")[0]),
        "url":  a.xpath(".//@href")[0]
    } for a in anchors]


def _get_col_names(table):
    header_cells = table.xpath("thead/tr/th")
    return [".".join(hc.xpath(".//text()")) for hc in header_cells]


def _parse_table(table, anchor_cols=[]):

    col_names = _get_col_names(table)
    anchor_indexes = [col_names.index(a_col) for a_col in anchor_cols]

    row_dicts = []
    rows = table.xpath("tbody/tr")
    for row in rows:
        cells = row.xpath("td")
        content_list = []
        for i, cell in enumerate(cells):
            if i in anchor_indexes:
                content_list.append(_get_cell_anchor(cell))
            else:
                content_list.append(_get_cell_text(cell))
        row_dicts.append(dict(zip(col_names, content_list)))

    return pd.DataFrame(row_dicts)


def parse_company_page(html):
    ANCHOR_COLS = ["Company name"]

    root = etree.fromstring(html, etree.HTMLParser())
    if root is None:
        return pd.DataFrame()
    table = root.xpath("//table")[0]

    return _parse_table(table, ANCHOR_COLS)


def _remove_unnec_list_cols(df):
    for c in df.columns:
        if df[c].apply(lambda x: isinstance(x, list) and len(x) <= 1).all():
            df[c] = df[c].apply(lambda x: x[0] if len(x) == 1 else "")
    return df


def _remove_cols_without_colname(df):
    for c in df.columns:
        if not c:
            df = df.drop(columns=[c], axis=1)
    return df


def post_process_company_df(df):
    df = _remove_unnec_list_cols(df)
    df = _remove_cols_without_colname(df)

    df['Company url'] = df['Company name'].apply(lambda x: x['url'])
    df['Company name'] = df['Company name'].apply(lambda x: x['name'])

    return df.drop_duplicates("Company url")


def parse_round_page(html):
    ANCHOR_COLS = ["Company Name", "Investors"]
    root = etree.fromstring(html, etree.HTMLParser())
    table = root.xpath("//table")[0]

    return _parse_table(table, ANCHOR_COLS)


def post_process_round_df(df):
    df = _remove_unnec_list_cols(df)
    df = _remove_cols_without_colname(df)

    df['Round Url'] = df['Company Name'].apply(lambda x: x[0]['url'])
    df['Company Url'] = df['Round Url'].apply(lambda x: "/".join(x.split("/")[:3]))
    df['Company Name'] = df['Company Name'].apply(lambda x: x[0]['name'])

    return df.drop_duplicates("Round Url")


def parse_investor_page(html):
    ANCHOR_COLS = ["Investors"]
    root = etree.fromstring(html, etree.HTMLParser())
    table = root.xpath("//table")[0]

    return _parse_table(table, ANCHOR_COLS)


def post_process_investor_df(df):
    df = _remove_unnec_list_cols(df)
    df = _remove_cols_without_colname(df)

    df['Investor Url'] = df['Investors'].apply(lambda x: x['url'])
    df['Investor Name'] = df['Investors'].apply(lambda x: x['name'])
    return df.drop(columns=['Investors'], axis=1).drop_duplicates("Investor Url")


# Very different DOM structure from other pages
def parse_acquisition_page(html):

    # Parse News
    ANCHOR_COLS = ["Startup"]
    NEWS_TITLE_MAP = {"スタートアップ": "Startup",
                      "業種": "Industry",
                      "設立": "Founded Date",
                      "事業内容": "Description"
                }

    root = etree.fromstring(html, etree.HTMLParser())
    news = root.xpath("//div[contains(@class, 'finance-news')]")[0]
    title_list = [dt.xpath(".//text()")[0] for dt in news.xpath(".//dt")]
    title_list = [NEWS_TITLE_MAP.get(t, t) for t in title_list]
    anchor_indexes = [title_list.index(a_col) for a_col in ANCHOR_COLS]

    content_list = [[{
        "name": a.xpath(".//text()")[0],
        "url": a.xpath(".//@href")[0]
    } for a in dd.xpath(".//a")] if i in anchor_indexes else dd.xpath(".//text()") for i, dd in enumerate(news.xpath(".//dd"))]

    news_dict = dict(zip(title_list, content_list))

    # Parse Horizontal Table
    TABLE_TITLE_MAP = {
        "年月日": "Date",
        "買収額": "Acquisition amount",
        "買収先": "acquirer",
        "ニュースURL": "News URL"
    }
    ANCHOR_COLS = ["acquirer"]

    table_dict = {}
    for table in root.xpath("//table"):
        for row in table.xpath(".//tr"):
            tds = row.xpath(".//td")
            title = tds[0].xpath(".//text()")[0]
            title = TABLE_TITLE_MAP.get(title, title)
            if title in ANCHOR_COLS:
                table_dict[title] = [{
                    "name": a.xpath(".//text()")[0],
                    "url": a.xpath(".//@href")[0]
                } for a in tds[1].xpath(".//a")]
            else:
                table_dict[title] = tds[1].xpath(".//text()")

    return {**news_dict, **table_dict}


def post_process_acquisition_df(df):
    df = df[~df['acquirer'].isnull()].copy()
    df = _remove_unnec_list_cols(df)
    df = _remove_cols_without_colname(df)

    df['Startup Url'] = df['Startup'].apply(lambda x: x['url'])
    df['Startup Name'] = df['Startup'].apply(lambda x: x['name'])
    df = df.drop(columns=['Startup'], axis=1)

    df['Acquirer Url'] = df['acquirer'].apply(lambda x: x['url'])
    df['Acquirer Name'] = df['acquirer'].apply(lambda x: x['name'])

    # TODO. handle multiple language situation
    return df.drop(columns=['acquirer'], axis=1).drop_duplicates(["Startup Url", "Acquirer Url", "Date"])

STATUS_MAPPING = {
    "Under Investigation": "operating",
    "": "operating",
    "Preparation (company registration only)": "operating",
    "Unable to investigate (for reasons such as inability to access HP)": "operating",
    "Survey completed (other reasons)": "closed",
    "Survey completed (dissolution)": "closed",
    "Survey completed (subsidiary of listed company)": "acquired",
    "Survey completed (IPO domestic market)": "ipo",
    "Not subject to investigation (other reasons)": "closed",
    "Survey completed (merger)": "acquired",
    "Survey completed (disappeared)": "closed",
    "Not subject to survey (wholly owned subsidiary of listed company)": "acquired",
    "調査継続": "operating",
    "Under Investigation (energetic small and medium-sized enterprises)": "operating",
    "Survey completed (acquisition)": "acquired",
    "Survey completed (company dissolved due to business transfer)": "acquired",
    "未評価(会社登録のみ)": "operating",
    "Survey completed (consolidated merger)": "acquired",
    "調査終了(その他の事由)": "closed",
    "Not subject to survey (Overseas VB)": "acquired",
    "Survey completed (IPO overseas market)": "ipo",
    "調査不能(HPアクセス不能などの事由)": "operating",
    "調査終了(上場企業の子会社化)": "acquired"                                                          
}

def map_company_status(status):
    if pd.isnull(status):
        return "operating"
    if status in STATUS_MAPPING:
        return STATUS_MAPPING[status]
    else:
        raise Exception("Unseen status: {}".format(status))