import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import calendar
from ftplib import FTP
import io
import logging
from bs4 import BeautifulSoup
import re

# ロギング設定
logging.basicConfig(level=logging.INFO)

# --- 定数設定 ---
# SHOWROOM オーガナイザーページのライブKPI URL
SR_LIVE_KPI_URL = "https://www.showroom-live.com/organizer/live_kpi"

# --- ユーティリティ関数 ---

def parse_cookie_string(cookie_string: str) -> dict:
    """
    セミコロン区切りのクッキー文字列をrequests.Sessionが使用できる辞書形式に変換します。
    """
    cookies = {}
    if not cookie_string:
        return cookies
        
    for pair in cookie_string.split(';'):
        # ペアに'='が含まれているか確認し、最初の'='で分割
        if '=' in pair:
            key, value = pair.split('=', 1)
            cookies[key.strip()] = value.strip()
            
    return cookies

def get_target_months():
    """
    2023年9月以降の月を、現在の月までリストとして返します (マルチセレクト用)。
    """
    months = []
    # 要件: 2023年9月以降
    start_date = datetime(2023, 9, 1)
    # 現在の日付
    now = datetime.now()
    
    current_date = start_date
    while current_date <= now.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
        # YYYY/MM 形式のラベルと datetimeオブジェクトをタプルで保存
        label = current_date.strftime("%Y/%m")
        months.append((label, current_date))
        
        # 次の月に進む
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)
            
    # 新しい月が上に来るように逆順にする
    return months[::-1]

def get_month_start_end(dt: datetime):
    """
    指定された月の最初の日と最後の日を 'YYYY-MM-DD' 形式で返します。
    """
    year = dt.year
    month = dt.month
    
    # 開始日 (YYYY-MM-01)
    start_date_str = f"{year}-{month:02d}-01"
    
    # 終了日 (カレンダーに忠実に、月末の日付を取得)
    _, last_day = calendar.monthrange(year, month)
    end_date_str = f"{year}-{month:02d}-{last_day:02d}"
    
    return start_date_str, end_date_str

def parse_live_duration(duration_str: str) -> int:
    """
    (127m24s) のような文字列から配信時間(分)を抽出し、30秒で繰り上げ処理を行います。
    """
    match = re.search(r'\((\d+)m(\d+)s\)', duration_str)
    if not match:
        return 0
    
    minutes = int(match.group(1))
    seconds = int(match.group(2))
    
    # 30秒以降であれば繰り上げ
    if seconds >= 30:
        return minutes + 1
    else:
        return minutes

def scrape_kpi_data(session: requests.Session, month_dt: datetime) -> pd.DataFrame:
    """
    指定された月のライブKPIデータを最大5ページまでスクレイピングします。
    """
    month_label = month_dt.strftime("%Y/%m")
    start_date, end_date = get_month_start_end(month_dt)
    
    st.info(f"処理対象月: **{month_label}** ({start_date} - {end_date})")
    
    all_records = []
    MAX_PAGES = 5 # 要件により5ページ
    
    # CSV添付ファイルに基づいた、最終的な28列のヘッダーを定義
    CSV_HEADERS = [
        "アカウントID", "ルームID", "配信日時", "配信時間(分)", "連続配信日数", "ルーム名",
        "合計視聴数", "視聴会員数", "アクション会員数", "SPギフト使用会員率", "初ルーム来訪者数",
        "初SR来訪者数", "短時間滞在者数", "ルームレベル", "フォロワー数", "フォロワー増減数",
        "Post人数", "獲得支援point", "コメント数", "コメント人数", "初コメント人数", "ギフト数",
        "ギフト人数", "初ギフト人数", "期限あり/期限なしSGのギフティング数", 
        "期限あり/期限なしSGのギフティング人数", "期限あり/期限なしSG総額", 
        "2023年9月以前のおまけ分(無償SG RS外)"
    ]

    for page in range(1, MAX_PAGES + 1):
        url = f"{SR_LIVE_KPI_URL}?page={page}&room_id=&from_date={start_date}&to_date={end_date}"
        st.caption(f"-> ページ {page} のデータを取得中: {url}")
        
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            st.error(f"HTTPリクエストエラー (ページ {page}): {e}")
            break
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # データが存在するテーブルのtbodyを探す
        table_body = soup.find('table', {'class': 'table-striped'}).find('tbody')
        if not table_body:
            st.info(f"ページ {page}: テーブルボディが見つかりませんでした。配信データなしとみなし、スクレイピングを終了します。")
            break
            
        rows = table_body.find_all('tr')
        
        # 配信データ行を処理
        data_found = False
        for row in rows:
            cols = row.find_all('td', {'class': 'delim'})
            if len(cols) != 27: # HTMLの列数は27 (配信日時/時間含む)
                continue
            
            data_found = True
            record = {}
            col_data = [c.get_text(separator=' ', strip=True) for c in cols]
            
            # 0. アカウントID, 1. ルームID
            record[CSV_HEADERS[0]] = col_data[0].strip()
            record[CSV_HEADERS[1]] = col_data[1].strip()
            
            # 2. 配信日時【配信時間（分・秒）】の処理
            datetime_duration_str = col_data[2].strip() 
            
            # 配信日時 (開始時刻) の抽出
            datetime_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', datetime_duration_str)
            if datetime_match:
                start_datetime = datetime.strptime(datetime_match.group(1), '%Y-%m-%d %H:%M:%S')
                record[CSV_HEADERS[2]] = start_datetime.strftime('%Y/%m/%d %H:%M:%S')
            else:
                record[CSV_HEADERS[2]] = ""
            
            # 配信時間(分) の抽出と繰り上げ
            record[CSV_HEADERS[3]] = parse_live_duration(datetime_duration_str)
            
            # 4. 連続配信日数 から 26. 2023年9月以前のおまけ分(無償SG RS外) までの処理
            for i in range(3, len(col_data)):
                html_col_index = i
                csv_col_index = i + 1
                
                value = col_data[html_col_index]
                
                # ★★★ 修正箇所: マイナス符号 (-) の除去を停止 ★★★
                # 数値・パーセンテージデータのクリーンアップ
                # csv_col_index >= 6 (合計視聴数) からクリーニングを開始
                if csv_col_index >= 6 and csv_col_index <= 27: 
                    # カンマ(,)除去、パーセント(%)除去のみを行い、マイナス符号の除去は行わない
                    value = value.replace(',', '').replace('%', '')
                
                record[CSV_HEADERS[csv_col_index]] = value.strip()
            
            all_records.append(record)
            
        if not data_found:
             st.info(f"ページ {page}: 配信データが存在しないため、スクレイピングを終了します。")
             break
            
    if not all_records:
        st.warning(f"月間データが全く取得できませんでした: {month_label}")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    return df


def process_kpi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    データフレームの整形、重複削除、データ型の安全な変換を行います。
    ブランク、ハイフン、マイナス符号を元の意図通りに維持するよう修正。
    """
    if df.empty:
        return df

    # --- データ型の調整とクリーニング ---
    numeric_cols = [
        "配信時間(分)", "連続配信日数", "合計視聴数", "視聴会員数", "アクション会員数", 
        "初ルーム来訪者数", "初SR来訪者数", "短時間滞在者数", "ルームレベル", "フォロワー数", 
        "フォロワー増減数", "Post人数", "獲得支援point", "コメント数", "コメント人数", 
        "初コメント人数", "ギフト数", "ギフト人数", "初ギフト人数", "期限あり/期限なしSGのギフティング数", 
        "期限あり/期限なしSGのギフティング人数", "期限あり/期限なしSG総額", 
        "2023年9月以前のおまけ分(無償SG RS外)"
    ]
    
    # 1. SPギフト使用会員率 (%) の処理 (floatでNaNを許可)
    # ハイフンは欠損値として扱う
    df['SPギフト使用会員率'] = df['SPギフト使用会員率'].astype(str).str.strip()
    df['SPギフト使用会員率'] = pd.to_numeric(
        df['SPギフト使用会員率'].replace(['', '-'], pd.NA), errors='coerce'
    ).astype(float).round(1)
    
    # 2. 整数カラムの処理 (Int64を使用してブランク/欠損値を維持)
    for col in numeric_cols:
        # 文字列に変換し、カンマ(,)と全角スペースを削除（scrape_kpi_dataでカンマは削除済みだが念のため）
        cleaned_series = df[col].astype(str).str.strip()
        
        # ★★★ 修正済み: ハイフン(-)や空文字列を欠損値(NaN)として扱う ★★★
        # マイナス符号はここで削除されていないため、to_numericが負の数として処理
        cleaned_series = cleaned_series.replace(['', '-'], pd.NA)

        # to_numericで数値に変換し、Int64型に変換（NaN/NAを許容する整数型）
        df[col] = pd.to_numeric(cleaned_series, errors='coerce').astype('Int64')

    # --- 重複データの削除 ---
    dedupe_cols = ["アカウントID", "ルームID", "配信日時", "配信時間(分)"]
    initial_count = len(df)
    df.drop_duplicates(subset=dedupe_cols, keep='first', inplace=True)
    deduped_count = len(df)
    
    if initial_count > deduped_count:
        st.success(f"重複データを {initial_count - deduped_count} 件削除しました。")
    
    # 最終的なCSVの並び順にカラムを整理
    final_cols = [
        "アカウントID", "ルームID", "配信日時", "配信時間(分)", "連続配信日数", "ルーム名",
        "合計視聴数", "視聴会員数", "アクション会員数", "SPギフト使用会員率", "初ルーム来訪者数",
        "初SR来訪者数", "短時間滞在者数", "ルームレベル", "フォロワー数", "フォロワー増減数",
        "Post人数", "獲得支援point", "コメント数", "コメント人数", "初コメント人数", "ギフト数",
        "ギフト人数", "初ギフト人数", "期限あり/期限なしSGのギフティング数", 
        "期限あり/期限なしSGのギフティング人数", "期限あり/期限なしSG総額", 
        "2023年9月以前のおまけ分(無償SG RS外)"
    ]
    
    df_final = df[final_cols].copy()
    
    # 欠損値（pd.NA）を持つカラムをobject型に変換してから空文字列に置換し、TypeErrorを回避
    # これにより、CSV出力時にブランク（空文字列）が書き出されます。
    cols_with_na = df_final.columns[df_final.isna().any()].tolist()
    
    for col in cols_with_na:
        # Int64やfloatをobject型に変換してから置換を実行
        df_final[col] = df_final[col].astype('object')
        
    # 欠損値（pd.NA）を空文字列に変換
    df_final = df_final.replace({pd.NA: ''})

    return df_final


def upload_to_ftp(df: pd.DataFrame, month_dt: datetime):
    """
    データフレームをCSV形式に変換し、FTPサーバーへアップロードします。
    """
    if df.empty:
        st.warning("アップロードするデータがありません。FTPアップロードをスキップします。")
        return
        
    # SecretsからFTP接続情報を取得
    try:
        FTP_HOST = st.secrets["ftp"]["host"]
        FTP_USER = st.secrets["ftp"]["user"]
        FTP_PASS = st.secrets["ftp"]["password"]
        
        # Secretsからtarget_base_pathを読み込む
        FTP_BASE_PATH_FROM_SECRETS = st.secrets["ftp"]["target_base_path"]
        
    except KeyError:
        st.error("❌ Streamlit SecretsからFTP接続情報を読み込めませんでした。設定を確認してください。")
        return

    year_month = month_dt.strftime("%Y-%m")
    # ファイル名: YYYY-MM_all_all.csv
    filename = f"{year_month}_all_all.csv"
    
    # Secretsから読み込んだパスをそのまま使用
    ftp_path = f"{FTP_BASE_PATH_FROM_SECRETS}{filename}"
    
    # CSVデータをインメモリで作成 (UTF-8 with BOM)
    csv_buffer = io.StringIO()
    # 修正：df_finalはすでにpd.NAが''に変換されているため、そのままto_csvを使用
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    csv_data = csv_buffer.getvalue()

    try:
        # FTP接続
        with FTP(FTP_HOST) as ftp:
            ftp.encoding = 'utf-8'
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            
            # storlinesを使用して、改行コードの問題を回避しつつテキストとしてアップロード
            ftp.storlines(f'STOR {ftp_path}', io.BytesIO(csv_data.encode('utf-8-sig')))
            
        st.success(f"✅ FTPアップロード完了: **{ftp_path}**")

    except Exception as e:
        # エラーメッセージを詳細に表示
        st.error(f"❌ FTPアップロード中にエラーが発生しました: {e}")
        st.warning(f"接続情報 (Host: {FTP_HOST}, User: {FTP_USER}) が正しいか、およびパス **{ftp_path}** への書き込み権限を確認してください。")


# --- Streamlitメイン処理 ---

def main():
    st.set_page_config(page_title="SHOWROOM KPIデータ アップロードツール", layout="wide")
    st.title("ライバーKPIデータ 自動アップロードツール (ライブ配信KPI)")
    st.markdown("---")

    # --- Secretsから機密情報を読み込み ---
    try:
        AUTH_COOKIE_STRING = st.secrets["showroom"]["auth_cookie_string"]
        SESSION_COOKIE = parse_cookie_string(AUTH_COOKIE_STRING)
    except KeyError:
        st.error("❌ Streamlit Secretsファイル (.streamlit/secrets.toml) が見つからないか、[showroom]セクションの'auth_cookie_string'が不足しています。")
        return
        
    # 認証セッションの作成
    session = requests.Session()
    session.cookies.update(SESSION_COOKIE)

    # 1. 月選択プルダウンの作成
    month_options = get_target_months()
    month_labels = [label for label, _ in month_options]
    
    st.header("1. 対象月選択")
    
    # 複数月選択 (マルチセレクト)
    selected_labels = st.multiselect(
        "処理対象の配信月を選択してください (複数選択可能):",
        options=month_labels,
        default=month_labels[:1]
    )

    if not selected_labels:
        st.warning("処理対象の月を選択してください。")
        return
        
    # 選択されたラベルからdatetimeオブジェクトを抽出
    selected_months = [
        dt for label, dt in month_options if label in selected_labels
    ]
    
    st.info(f"選択された月: **{', '.join(selected_labels)}**")
    
    st.header("2. データ取得とアップロードの実行")
    
    # 3. 実行ボタン
    if st.button("🚀 KPIデータの全てを取得・FTPアップロードを実行", type="primary"):
        all_success = True
        with st.spinner("処理中: 選択された月のKPIデータを取得・整形しています..."):
            
            # 選択された月を順番に処理
            for month_dt in selected_months:
                st.subheader(f"📅 {month_dt.strftime('%Y/%m')} の処理を開始")
                
                # 1. データ取得
                raw_df = scrape_kpi_data(session, month_dt)
                
                # raw_dfが空ならすぐに次の月にスキップ
                if raw_df.empty:
                    st.warning(f"⚠️ {month_dt.strftime('%Y/%m')} のデータは取得できませんでした。処理をスキップします。")
                    all_success = False
                    st.markdown("---")
                    continue
                
                # 2. データ整形と重複削除
                processed_df = process_kpi_data(raw_df)
                
                # 整形後も空でないか確認（最終チェック）
                if not processed_df.empty:
                    # st.dataframe()は引き続きコメントアウトし、エラーを回避
                    st.success(f"データ ({len(processed_df)} 件) を正常に取得・整形しました。アップロードを開始します。")

                    # 3. FTPアップロード
                    upload_to_ftp(processed_df, month_dt)
                else:
                    st.warning(f"⚠️ {month_dt.strftime('%Y/%m')} のデータは、整形（重複削除など）後に残ったレコードが0件でした。アップロードをスキップします。")
                    all_success = False
                
                st.markdown("---") # 月の区切り線

        st.balloons()
        if all_success:
            st.success("🎉 全ての処理が完了しました！")
        else:
            st.info("処理は完了しましたが、一部の月でデータが見つからなかったか、エラーが発生しました。ログを確認してください。")
        
if __name__ == "__main__":
    main()