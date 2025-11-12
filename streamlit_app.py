import streamlit as st
import pandas as pd
import boto3
import io
import os
import csv 
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import re

# --- ページ設定 ---
st.set_page_config(page_title="SQL to S3 Uploader", layout="wide")
st.title("SQLクエリ実行 & S3アップロード 📤 (Snowflake)")
st.markdown("""
SQLを実行し、プレビューを確認してからS3にアップロードします。
（アップロード先はSnowflakeアカウントに応じて自動的に決定されます）
""")

# --- セッション状態の初期化 ---
if 'step' not in st.session_state:
    st.session_state.step = 1
if 'sql_query' not in st.session_state:
    st.session_state.sql_query = ""
if 'preview_df' not in st.session_state:
    st.session_state.preview_df = pd.DataFrame()
if 'preview_count' not in st.session_state:
    st.session_state.preview_count = 0
if 'has_more_than_100' not in st.session_state:
    st.session_state.has_more_than_100 = False

# =============================================================================
# ヘルパー関数 (変更なし)
# =============================================================================
def get_snowflake_account(conn):
    try:
        account_df = conn.query("SELECT CURRENT_ACCOUNT()", ttl=0)
        snowflake_account = account_df.iloc[0, 0]
        st.info(f"Snowflakeアカウント: `{snowflake_account}` として認識しています。")
        return snowflake_account
    except Exception as e:
        st.error(f"Snowflakeアカウント名の取得に失敗しました: {e}")
        return None

def get_s3_destination(snowflake_account):
    if not snowflake_account: return None, None
    # --- ▼▼▼ ここでアカウントごとの分岐ロジックを設定 ▼▼▼ ---
    if snowflake_account == "ACCOUNT_A":
        bucket = "my-admin-bucket"
        key_prefix = "reports/account_a/"
    elif snowflake_account == "ACCOUNT_B_PROD":
        bucket = "my-data-lake-prod"
        key_prefix = "analysis_results/prod/"
    else:
        bucket = "my-dev-sandbox-bucket"
        key_prefix = f"common_uploads/{snowflake_account.lower()}/"
    # --- ▲▲▲ 分岐ロジックの設定はここまで ▲▲▲ ---
    return bucket, key_prefix

def get_s3_client():
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=st.secrets["s3"]["aws_access_key_id"],
            aws_secret_access_key=st.secrets["s3"]["aws_secret_access_key"],
            region_name=st.secrets["s3"].get("region_name")
        )
        return s3_client
    except (KeyError, NoCredentialsError):
        st.error("S3の認証情報がSnowflake Secretsに正しく設定されていません。")
        return None
    except Exception as e:
        st.error(f"S3クライアント初期化エラー: {e}")
        return None

# =============================================================================
# ステップ 1: SQLの入力とプレビュー (変更なし)
# =============================================================================
def step1_sql_input():
    st.header("1. 実行するSQLクエリ")
    sql_input = st.text_area(
        "SQLを入力してください", 
        value=st.session_state.sql_query,
        height=150, 
        placeholder="例: SELECT * FROM my_table;"
    )
    
    st.session_state.sql_query = sql_input

    if st.button("次へ（プレビュー表示）", type="primary"):
        if not st.session_state.sql_query:
            st.warning("SQLクエリが入力されていません。")
            st.stop()
        
        try:
            conn = st.connection("snowflake")
            
            base_query = re.sub(r'LIMIT\s+\d+\s*$', '', st.session_state.sql_query, flags=re.IGNORECASE).strip().rstrip(';')
            preview_query = f"{base_query} LIMIT 101"
            
            with st.spinner("プレビューデータを取得中です..."):
                preview_df = conn.query(preview_query)
            
            count = len(preview_df)
            st.session_state.preview_df = preview_df 

            if count == 0:
                st.warning("クエリ結果は0件でした。アップロードするデータがありません。")
                st.session_state.preview_count = 0
                st.session_state.has_more_than_100 = False
                st.session_state.preview_df = pd.DataFrame()
            
            elif count <= 100:
                st.success(f"検証完了: **{count}** 件のレコードが見つかりました。（全件）")
                st.session_state.preview_count = count
                st.session_state.has_more_than_100 = False
                st.session_state.step = 2
                st.rerun()

            else: # count == 101
                st.success("検証完了: **100件を超えるデータ** が見つかりました。")
                st.session_state.preview_count = 101
                st.session_state.has_more_than_100 = True
                st.session_state.step = 2
                st.rerun()

        except Exception as e:
            st.error(f"SQL実行エラー: {e}")

# =============================================================================
# ステップ 2: 書式設定とS3アップロード (テキスト修正版)
# =============================================================================
def step2_format_and_upload():
    st.header("1. 実行するSQLクエリ（確認）")
    
    # --- ▼▼▼ 修正点 1: 件数表示の st.info(...) を削除 ▼▼▼ ---
    # (st.info(...) のブロックを削除)
    # --- ▲▲▲ 修正点 1 ▲▲▲ ---
        
    st.code(st.session_state.sql_query, language="sql")

    if not st.session_state.preview_df.empty:
        st.subheader("データプレビュー")
        if st.session_state.has_more_than_100:
            st.dataframe(st.session_state.preview_df.head(100))
        else:
            st.dataframe(st.session_state.preview_df)

    
    st.header("2. 出力ファイル書式")
    file_format = st.selectbox(
        "ファイル書式",
        ("CSV", "Excel", "TSV"),
        index=0
    )

    include_header = True
    quoting = "ALL" 

    if file_format in ("CSV", "TSV"):
        col_opt1, col_opt2 = st.columns(2)
        with col_opt1:
            header_option = st.radio(
                "ヘッダー", 
                ("あり", "なし"), 
                index=0
            )
            include_header = (header_option == "あり")

        with col_opt2:
            quote_option = st.radio(
                "ダブルクォート", 
                ("付与する (推奨)", "付与しない"), 
                index=0
            )
            quoting = csv.QUOTE_ALL if (quote_option == "付与する (推奨)") else csv.QUOTE_NONE
    else:
        st.caption("Excel形式では、ヘッダーは常に出力され、クォートは自動的に処理されます。")


    st.divider()

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("戻る（SQLを修正）"):
            st.session_state.step = 1
            st.session_state.preview_df = pd.DataFrame() 
            st.rerun()
            
    with col2:
        # --- ▼▼▼ 修正点 2: ボタンテキストを変更 ▼▼▼ ---
        if st.button("出力実行", type="primary"):
        # --- ▲▲▲ 修正点 2 ▲▲▲ ---
            
            try:
                # --- 1. S3クライアントと宛先の準備 ---
                s3_client = get_s3_client()
                if s3_client is None: st.stop()
                
                conn = st.connection("snowflake")
                account_name = get_snowflake_account(conn)
                s3_bucket, s3_key_prefix = get_s3_destination(account_name)
                
                if not s3_bucket:
                    st.error("S3のアップロード先を決定できませんでした。")
                    st.stop()

                # --- 2. SQLの全件取得 ---
                spinner_msg = "全件データをSnowflakeから取得中です..."
                if st.session_state.has_more_than_100:
                    spinner_msg = "全件データ（100件超）をSnowflakeから取得中です..."

                with st.spinner(spinner_msg):
                    df_to_upload = conn.query(st.session_state.sql_query)
                
                st.success(f"全 **{len(df_to_upload)}** 件のデータ取得完了。")

                # --- 3. 拡張子とファイル名の決定 ---
                if file_format == "CSV": extension = ".csv"
                elif file_format == "Excel": extension = ".xlsx"
                elif file_format == "TSV": extension = ".tsv"
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = f"sql_export_{timestamp}{extension}"
                s3_key = os.path.join(s3_key_prefix, file_name)

                # --- 4. データ変換 (オプション適用) ---
                with st.spinner(f"データを {file_format} 形式に変換中..."):
                    output_buffer = io.BytesIO()
                    
                    if file_format == "CSV":
                        df_to_upload.to_csv(
                            output_buffer, 
                            index=False, 
                            encoding='utf-8-sig',
                            header=include_header, 
                            quoting=quoting 
                        )
                    elif file_format == "TSV":
                        df_to_upload.to_csv(
                            output_buffer, 
                            sep='\t', 
                            index=False, 
                            encoding='utf-8-sig',
                            header=include_header, 
                            quoting=quoting 
                        )
                    elif file_format == "Excel":
                        with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                            df_to_upload.to_excel(writer, index=False, sheet_name='Sheet1')
                    
                    data_to_upload = output_buffer.getvalue()

                # --- 5. S3へアップロード ---
                s3_path_full = f"s3://{s3_bucket}/{s3_key}"
                with st.spinner(f"S3 ({s3_path_full}) へアップロード中です..."):
                    s3_client.put_object(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        Body=data_to_upload
                    )
                
                st.success(f"アップロード成功！ ✨")
                st.markdown(f"**ファイルパス:** `{s3_path_full}`")
                
                # セッションをリセット
                st.session_state.step = 1
                st.session_state.sql_query = ""
                st.session_state.preview_df = pd.DataFrame()
                st.session_state.preview_count = 0
                st.session_state.has_more_than_100 = False
                st.balloons() 

            except Exception as e:
                st.error(f"S3アップロードエラー: {e}")

# =============================================================================
# メインロジック
# =============================================================================

if st.session_state.step == 1:
    step1_sql_input()
elif st.session_state.step == 2:
    step2_format_and_upload()
