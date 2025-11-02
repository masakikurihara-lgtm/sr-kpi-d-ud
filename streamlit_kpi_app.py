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
        if '=' in pair:
            key, value = pair.split('=', 1)
            cookies[key.strip()] = value.strip()
            
    return cookies

def get_target_months():
    """
    2023年9月以降の月を、現在の月までリストとして返します (マルチセレクト用)。
    """
    months = []
    start_date = datetime(2023, 9, 1)
    now = datetime.now()
    
    current_date = start_date
    while current_date <= now.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
        label = current_date.strftime("%Y/%m")
        months.append((label, current_date))
        
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)
            
    return months[::-1]

def get_month_start_end(dt: datetime):
    """
    指定された月の最初の日と最後の日を 'YYYY-MM-DD' 形式で返します。
    """
    year = dt.year
    month = dt.month
    
    start_date_str = f"{year}-{month:02d}-01"
    
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
    
    if seconds >= 30:
        return minutes + 1
    else:
        return minutes

def scrape_kpi_data(session: requests.Session, month_dt: datetime) -> pd.DataFrame:
    """
    指定された月のライブKPIデータをスクレイピングし、配信時間(分)以外は全て文字列として保持します。
    （修正点：カンマ、ハイフン、ブランクを維持するため、数値クリーニングを停止）
    """
    month_label = month_dt.strftime("%Y/%m")
    start_date, end_date = get_month_start_end(month_dt)
    
    st.info(f"処理対象月: **{month_label}** ({start_date} - {end_date})")
    
    all_records = []
    MAX_PAGES = 5 
    
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
        
        table_body = soup.find('table', {'class': 'table-striped'}).find('tbody')
        if not table_body:
            st.info(f"ページ {page}: 配信データが存在しないため、スクレイピングを終了します。")
            break
            
        rows = table_body.find_all('tr')
        
        data_found = False
        for row in rows:
            cols = row.find_all('td', {'class': 'delim'})
            if len(cols) != 27:
                continue
            
            data_found = True
            record = {}
            col_data = [c.get_text(separator=' ', strip=True) for c in cols]
            
            # 0. アカウントID, 1. ルームID
            record[CSV_HEADERS[0]] = col_data[0].strip()
            record[CSV_HEADERS[1]] = col_data[1].strip()
            
            # 2. 配信日時【配信時間（分・秒）】の処理
            datetime_duration_str = col_data[2].strip() 
            
            # 配信日時 (開始時刻) の抽出と形式変換 (YYYY/MM/DD HH:MM:SS)
            datetime_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', datetime_duration_str)
            if datetime_match:
                start_datetime = datetime.strptime(datetime_match.group(1), '%Y-%m-%d %H:%M:%S')
                record[CSV_HEADERS[2]] = start_datetime.strftime('%Y/%m/%d %H:%M:%S')
            else:
                record[CSV_HEADERS[2]] = ""
            
            # 配信時間(分) の抽出と繰り上げ (数値として保持)
            record[CSV_HEADERS[3]] = parse_live_duration(datetime_duration_str)
            
            # 4. 連続配信日数 から 26. 2023年9月以前のおまけ分(無償SG RS外) までの処理
            for i in range(3, len(col_data)):
                csv_col_index = i + 1
                value = col_data[i]
                
                # HTMLから取得した値の周囲の空白を削除
                value = value.strip()
                
                # ★★★ 修正済み: カンマ、ハイフン、ブランクを維持するため、ここでは一切のクリーニングを行わない ★★★
                record[CSV_HEADERS[csv_col_index]] = value
            
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
    データフレームの重複削除と、最終的な整形のみを行います。
    （修正点：数値変換ロジックを全て削除し、カンマ、ハイフン、ブランクをそのまま維持します。）
    """
    if df.empty:
        return df
    
    # --- 重複データの削除 ---
    # 配信時間(分)は数値だが、重複判定には問題ないためそのまま使用
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
    
    # すべての文字列カラムについて、両端のスペースを確実に削除
    for col in df_final.columns:
        if df_final[col].dtype == 'object':
            # ブランク、ハイフン、カンマを維持しつつ、不要な両端のスペースのみを排除
            df_final[col] = df_final[col].astype(str).str.strip()
    
    # 数値型である「配信時間(分)」も、CSV出力時にカンマが入らないようint型に変換
    df_final['配信時間(分)'] = pd.to_numeric(df_final['配信時間(分)'], errors='coerce').fillna(0).astype(int)

    # df_finalは、文字列データ（カンマ・ハイフン含む）、数値データ（配信時間(分)）が混在した状態でCSV出力されます。
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
        
        # ★★★ 修正済み: Secretsからtarget_base_pathを読み込む ★★★
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
    # 数値カラムは数値として、文字列カラム（カンマ・ハイフン含む）は文字列としてto_csvで書き出し
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    csv_data = csv_buffer.getvalue()

    try:
        # FTP接続
        with FTP(FTP_HOST) as ftp:
            ftp.encoding = 'utf-8'
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            
            ftp.storlines(f'STOR {ftp_path}', io.BytesIO(csv_data.encode('utf-8-sig')))
            
        st.success(f"✅ FTPアップロード完了: **{ftp_path}**")

    except Exception as e:
        # エラーメッセージを詳細に表示
        st.error(f"❌ FTPアップロード中にエラーが発生しました: {e}")
        st.warning(f"接続情報 (Host: {FTP_HOST}, User: {FTP_USER}) が正しいか、およびパス **{ftp_path}** への書き込み権限を確認してください。")


# --- Streamlitメイン処理 ---

def main():
    st.set_page_config(page_title="SHOWROOM ライブKPIアップロード", layout="wide")
    #st.title("ライバーKPIデータ 自動アップロードツール (ライブ配信KPI)")
    st.markdown(
        "<h1 style='font-size:28px; text-align:left; color:#1f2937;'>SHOWROOM ライブKPIアップロード</h1>",
        unsafe_allow_html=True
    )  
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
    
    #st.header("1. 対象月選択")
    st.markdown("#### 1. 対象月選択")
    
    # 複数月選択 (マルチセレクト)
    selected_labels = st.multiselect(
        "処理対象の配信月を選択してください (複数選択可能):",
        options=month_labels,
        default=month_labels[:1]
    )

    if not selected_labels:
        st.warning("処理対象の月を選択してください。")
        return
        
    selected_months = [
        dt for label, dt in month_options if label in selected_labels
    ]
    
    st.info(f"選択された月: **{', '.join(selected_labels)}**")
    
    #st.header("2. データ取得とアップロードの実行")
    st.markdown("#### 2. データ取得とアップロードの実行")
    
    # 3. 実行ボタン
    if st.button("🚀 KPIデータの全てを取得・FTPアップロードを実行", type="primary"):
        all_success = True
        with st.spinner("処理中: 選択された月のKPIデータを取得・整形しています..."):
            
            for month_dt in selected_months:
                #st.subheader(f"📅 {month_dt.strftime('%Y/%m')} の処理を開始")
                st.markdown(f"##### 📅 {month_dt.strftime('%Y/%m')} の処理を開始")
                
                # 1. データ取得
                raw_df = scrape_kpi_data(session, month_dt)
                
                if raw_df.empty:
                    st.warning(f"⚠️ {month_dt.strftime('%Y/%m')} のデータは取得できませんでした。処理をスキップします。")
                    all_success = False
                    st.markdown("---")
                    continue
                
                # 2. データ整形と重複削除
                processed_df = process_kpi_data(raw_df)
                
                if not processed_df.empty:
                    # ★★★ 修正済み: StreamlitのTypeError回避のためコメントアウト ★★★
                    # st.dataframe(processed_df.head(), caption=f"{month_dt.strftime('%Y/%m')} データのプレビュー (全 {len(processed_df)} 件)", use_container_width=True)
                    st.success(f"データ ({len(processed_df)} 件) を正常に取得・整形しました。アップロードを開始します。")

                    # 3. FTPアップロード
                    upload_to_ftp(processed_df, month_dt)
                else:
                    st.warning(f"⚠️ {month_dt.strftime('%Y/%m')} のデータは、整形（重複削除など）後に残ったレコードが0件でした。アップロードをスキップします。")
                    all_success = False
                
                st.markdown("---")

        st.balloons()
        if all_success:
            st.success("🎉 全ての処理が完了しました！")
        else:
            st.info("処理は完了しましたが、一部の月でデータが見つからなかったか、エラーが発生しました。ログを確認してください。")
        
if __name__ == "__main__":
    main()