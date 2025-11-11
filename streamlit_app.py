# -*- coding: utf-8 -*-
# メイン画面中心UI + 権限ベースのテーブル/ビュー自動フィルタ（SHOW未使用） + 体感性能改善版
# 方針：
#  - Information Schema だけで権限を解決（ENABLED_ROLES / OBJECT_PRIVILEGES）
#  - ビューを含める、ロール継承を含む、ALLOWED_TABLESは使わない
#  - UIは「コマンドバー + タブ + 2カラム（設定/結果）」で一画面完結
#  - 当面は TEST_DB.TEST 固定。将来は TARGETS を増やすだけで拡張可能。

import streamlit as st
import pandas as pd
import datetime
import re
from io import BytesIO
import snowflake.connector
import zipfile
import csv
import io

# -------------------------------------------------
# ページ設定
# -------------------------------------------------
st.set_page_config(page_title="データ閲覧", layout="wide")

# 軽量テーマ（配色・フォントサイズ）
st.markdown("""
<style>
:root {
  --accent: #2563EB;     /* ボタン等のアクセント */
  --ok: #16A34A;         /* 成功 */
  --warn: #D97706;       /* 警告 */
  --muted: #6B7280;      /* サブテキスト */
}
.block-container { padding-top: 1rem; }
h1, h2, h3 { font-weight: 600; }
div[data-testid="stMarkdownContainer"] code, pre code { font-size: 0.95rem; }
div.stButton > button[kind="primary"] {
  background: var(--accent); border-color: var(--accent);
}
div.stButton > button[disabled] { opacity: 0.5; cursor: not-allowed; }
div.stTabs [data-baseweb="tab"] { font-size: 0.95rem; }
.small-muted { color: var(--muted); font-size: 0.9rem; }
.badge { display:inline-block; padding: 0.1rem .5rem; border-radius: .4rem; font-size:.8rem; }
.badge-ok { background:#DCFCE7; color:#166534; }
.badge-warn { background:#FEF9C3; color:#854D0E; }
.badge-run { background:#DBEAFE; color:#1E40AF; }
hr { margin: .8rem 0; }
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------
# 設定（拡張可能な対象スキーマ）
# -------------------------------------------------
TARGETS = [
    {"db": "TEST_DB", "schema": "TEST"},
]

# -------------------------------------------------
# 定数
# -------------------------------------------------
DELIMITER_COMMA = ','
DELIMITER_TAB = '\t'
STAGE_NAME = '@test_s3_stage'
S3_DirName = '/test/'
S3_FileName = 'testfilename'
CSV_MAX = 50000   # CSV/TSV のZIP分割行数
EXCEL_MAX = 50000 # Excelの最大行数

# -------------------------------------------------
# 接続・共通クエリ関数
# -------------------------------------------------
@st.cache_resource
def get_conn():
    """Snowflake接続をセッション内で再利用"""
    # 必要に応じて接続パラメータを指定してください
    # 例:
    # return snowflake.connector.connect(
    #     account="xxx",
    #     user="xxx",
    #     password="xxx",
    #     warehouse="xxx",
    #     role="xxx",
    #     database="xxx",
    #     schema="xxx",
    # )
    return snowflake.connector.connect()

def _normalize_params(params):
    """executeに渡すパラメータを正規化（空dictを渡さない：252004対策）"""
    if params is None:
        return None
    if isinstance(params, (list, tuple)):
        return params if len(params) > 0 else None
    if isinstance(params, dict):
        return list(params.values()) if len(params) > 0 else None
    return [params]

@st.cache_data(ttl=600)
def run_query(sql: str, params=None) -> pd.DataFrame:
    """
    SnowflakeにSQLを投げてDataFrameを返す。
    パラメータ無しは None を渡し、空dictは渡さない（252004対策）。
    """
    with get_conn().cursor() as cur:
        cur.execute(sql, _normalize_params(params))
        try:
            return cur.fetch_pandas_all()  # Arrowベースで高速
        except Exception:
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return pd.DataFrame(rows, columns=cols)

# -------------------------------------------------
# ユーティリティ
# -------------------------------------------------
@st.cache_data(ttl=3600)
def get_identity():
    acc = run_query("SELECT CURRENT_ACCOUNT() AS ACCOUNTNAME")
    usr = run_query("SELECT CURRENT_USER() AS USERNAME")
    return acc["ACCOUNTNAME"][0], usr["USERNAME"][0]

def sanitize_ident(s: str) -> str:
    """識別子サニタイズ（英数＋アンダースコアのみ）"""
    return re.sub(r"[^A-Za-z0-9_]", "", s or "")

# -------------------------------------------------
# SHOW 非依存のロール／権限解決（Information Schema）
# -------------------------------------------------
@st.cache_data(ttl=300)
def get_enabled_roles(target_db: str = "TEST_DB") -> list[str]:
    """
    現セッションで有効（継承含む）なロール。
    ENABLED_ROLES は各DBの INFORMATION_SCHEMA にあるアカウントレベルビュー。
    """
    target_db = sanitize_ident(target_db)
    df = run_query(f"SELECT ROLE_NAME FROM {target_db}.INFORMATION_SCHEMA.ENABLED_ROLES")
    return df["ROLE_NAME"].tolist() if not df.empty else []

@st.cache_data(ttl=300)
def get_effective_select_objects(
    target_db: str,
    target_schema: str,
    include_views: bool = True,
    include_materialized_views: bool = False
) -> list[str]:
    """
    Information Schemaのみで判定：
      - 有効ロール: ENABLED_ROLES
      - 権限     : OBJECT_PRIVILEGES (SELECT / OWNERSHIP)
    対象DB/スキーマ内で SELECT 可能な TABLE/VIEW（＋任意で MATERIALIZED VIEW）名を返す。
    """
    target_db = sanitize_ident(target_db)
    target_schema = sanitize_ident(target_schema)

    roles = get_enabled_roles(target_db)
    if not roles:
        return []

    obj_types = ["TABLE"]
    if include_views:
        obj_types.append("VIEW")
    if include_materialized_views:
        obj_types.append("MATERIALIZED VIEW")

    obj_types_sql = ", ".join(f"'{t}'" for t in obj_types)
    roles_sql = ", ".join(f"'{r}'" for r in roles)

    q = f"""
        SELECT DISTINCT OBJECT_NAME
        FROM {target_db}.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
        WHERE OBJECT_SCHEMA = '{target_schema}'
          AND OBJECT_TYPE IN ({obj_types_sql})
          AND PRIVILEGE_TYPE IN ('SELECT','OWNERSHIP')
          AND GRANTEE IN ({roles_sql})
        ORDER BY OBJECT_NAME
    """
    df = run_query(q)
    return df["OBJECT_NAME"].tolist() if not df.empty else []

@st.cache_data(ttl=300)
def get_allowed_tables() -> list[str]:
    """
    権限ベースで参照可能な TABLE/VIEW の一覧。
    将来 TARGETS に複数スキーマを並べた場合は和集合を返す。
    """
    all_effective = set()
    for t in TARGETS:
        objs = get_effective_select_objects(
            target_db=t["db"], target_schema=t["schema"],
            include_views=True, include_materialized_views=False
        )
        all_effective.update(objs)
    return sorted(all_effective)

# -------------------------------------------------
# ダウンロード / S3 / ストリーミング
# -------------------------------------------------
def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    df_to_save = df.copy()
    for col in df_to_save.select_dtypes(include=['datetimetz']).columns:
        df_to_save[col] = df_to_save[col].dt.tz_localize(None)
    try:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_to_save.to_excel(writer, index=False, sheet_name="データ")
    except Exception:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_to_save.to_excel(writer, index=False, sheet_name="データ")
    return output.getvalue()

def generate_download(df: pd.DataFrame, filetype: str = "csv", quote_option='"', split_limit: int = CSV_MAX):
    if filetype in ["csv", "tsv"]:
        sep = "\t" if filetype == "tsv" else ","
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for i, start in enumerate(range(0, len(df), split_limit)):
                part = df.iloc[start:start + split_limit].copy()
                output = io.StringIO()
                if quote_option == 'なし':
                    part.to_csv(output, index=False, sep=sep, quoting=csv.QUOTE_NONE, escapechar='\\')
                else:
                    part.to_csv(output, index=False, sep=sep, quotechar=quote_option, quoting=csv.QUOTE_ALL)
                filename = f"part{i + 1}.{filetype}"
                zf.writestr(filename, output.getvalue().encode("utf-8"))
        zip_buffer.seek(0)
        return zip_buffer
    elif filetype == "excel":
        if len(df) > EXCEL_MAX:
            return None
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
        return output.getvalue()

def stream_query_to_zip(sql: str, sep: str = ",", quotechar: str = '"', split_limit: int = CSV_MAX) -> io.BytesIO:
    """
    Snowflake -> fetchmany -> 逐次CSV書き出し -> ZIP（分割）
    DataFrameを経由しないため高速・省メモリ
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        with get_conn().cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            part_no, written = 1, 0
            out = io.StringIO()
            writer = csv.writer(out, delimiter=sep, quotechar=quotechar, quoting=csv.QUOTE_ALL)
            writer.writerow(cols)
            while True:
                rows = cur.fetchmany(10_000)  # バッチサイズは調整可
                if not rows:
                    break
                for row in rows:
                    if written and written % split_limit == 0:
                        zf.writestr(f"part{part_no}.csv", out.getvalue().encode("utf-8"))
                        part_no += 1
                        out = io.StringIO()
                        writer = csv.writer(out, delimiter=sep, quotechar=quotechar, quoting=csv.QUOTE_ALL)
                        writer.writerow(cols)
                    writer.writerow(row)
                    written += 1
            if written == 0:
                zf.writestr("part1.csv", out.getvalue().encode("utf-8"))
            else:
                zf.writestr(f"part{part_no}.csv", out.getvalue().encode("utf-8"))
    buf.seek(0)
    return buf

def S3_upload(query: str, delimiter: str, filename: str):
    with st.spinner("S3アップロード中..."):
        run_query(f"""
COPY INTO {STAGE_NAME}{S3_DirName}{S3_FileName}{filename}
FROM ({query})
FILE_FORMAT = (
  TYPE = CSV
  FIELD_DELIMITER = '{delimiter}'
  FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
  COMPRESSION = GZIP
  HEADER = TRUE
)
OVERWRITE = TRUE
SINGLE = TRUE;
""")
    st.success("S3アップロード完了しました")

def show_download_ui(df: pd.DataFrame, table_name: str, key_prefix: str = "download"):
    if df.empty:
        st.warning("ダウンロード可能なデータがありません。")
        return
    today_str = datetime.date.today().strftime("%Y%m%d")
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        quote_option = st.selectbox("囲い文字", ['"', "'", 'なし'], index=0, key=f"{key_prefix}_quote")
    with col2:
        ft = st.selectbox("形式", ["csv", "tsv", "excel"], index=0, key=f"{key_prefix}_fmt")
    with col3:
        if ft in ("csv","tsv"):
            data = generate_download(df, filetype=ft, quote_option=quote_option)
            st.download_button(
                label="📥 ZIPダウンロード",
                data=data,
                file_name=f"{table_name}_{today_str}_{ft.upper()}.zip",
                mime="application/zip",
                key=f"{key_prefix}_dlzip"
            )
        else:
            data = generate_download(df, filetype="excel")
            if data:
                st.download_button(
                    label="📥 Excelダウンロード",
                    data=data,
                    file_name=f"{table_name}_{today_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"{key_prefix}_xldl"
                )
            else:
                st.warning("Excelは50,000件を超えるためダウンロードできません。")

def download_ready_ui(df_preview: pd.DataFrame, table_name: str, sql_all_func):
    """
    従来の「全件をDFに積んでからDL」＋ 高速ストリーミングDL（DF不要）の両対応。
    """
    if df_preview.empty:
        st.warning("ダウンロード可能なデータがありません。")
        return
    colA, colB, colC = st.columns([1.2, 1.2, 2])
    with colA:
        if st.button("📥 ダウンロード準備（全件取得）"):
            with st.spinner("全件データを取得中..."):
                st.session_state.df_for_download = sql_all_func()
            st.success(f"全件データを取得しました ({len(st.session_state.df_for_download)}件)")
    with colB:
        sep_choice = st.radio("区切り", ["CSV", "TSV"], horizontal=True, key="fast_sep")
        sep = "\t" if sep_choice == "TSV" else ","
        quote_option = st.selectbox("囲い文字", ['"', "'"], index=0, key="fast_quote")
        if st.button("📥 高速ZIPダウンロード（ストリーミング）"):
            with st.spinner("ZIP生成中..."):
                data = stream_query_to_zip(st.session_state.get("last_query",""), sep=sep, quotechar=quote_option)
            st.download_button(
                "📥 ダウンロード開始",
                data=data,
                file_name=f"{table_name}_{datetime.date.today():%Y%m%d}_server.zip",
                mime="application/zip",
                key="fast_zip_dl"
            )
    with colC:
        st.caption("S3 アップロード")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("⤴ CSVをS3へ"):
                S3_upload(st.session_state.get("last_query",""), DELIMITER_COMMA, "_data_CSV")
        with c2:
            if st.button("⤴ TSVをS3へ"):
                S3_upload(st.session_state.get("last_query",""), DELIMITER_TAB, "_data_TSV")

# -------------------------------------------------
# SQL整形
# -------------------------------------------------
_LIMIT_PATTERN = re.compile(r"(?i)LIMIT\\s+\\d+")

def remove_limit(sql: str) -> str:
    return _LIMIT_PATTERN.sub("", sql).strip()

def clean_sql(sql: str) -> str:
    if not sql:
        return ""
    return sql.strip().rstrip(";")

def get_full_data() -> pd.DataFrame:
    sql = st.session_state.get("last_query", "")
    if not sql:
        st.warning("実行対象のSQLがありません。")
        return pd.DataFrame()
    sql_full = clean_sql(remove_limit(sql))
    return run_query(sql_full)

# -------------------------------------------------
# SQL保存
# -------------------------------------------------
def save_sql_to_log(user: str, sql_text: str, sql_name: str):
    """
    SQLを {TARGETS[0]} の SQL_LOG に保存。将来は格納先も可変にできます。
    """
    if not sql_name or not sql_name.strip():
        st.warning("保存名を入力してください。")
        return
    target_db = sanitize_ident(TARGETS[0]["db"])
    target_schema = sanitize_ident(TARGETS[0]["schema"])
    try:
        sql_to_save = sql_text.replace("'", "''")
        name_to_save = sql_name.replace("'", "''")
        insert_sql = f"""
INSERT INTO {target_db}.{target_schema}.sql_log (user_ID, sql_name, exec_query, save_date)
VALUES ('{user}', '{name_to_save}', '{sql_to_save}', CURRENT_TIMESTAMP)
"""
        with get_conn().cursor() as cur:
            cur.execute(insert_sql)
            get_conn().commit()
        st.session_state["sql_saved_message"] = f"SQLを保存しました！（{sql_name}）"
    except Exception as e:
        st.session_state["sql_saved_message"] = f"保存に失敗しました: {e}"

def show_sql_save_message():
    if "sql_saved_message" in st.session_state:
        st.success(st.session_state["sql_saved_message"])
        del st.session_state["sql_saved_message"]

# -------------------------------------------------
# 画面上部：ヘッダ / コマンドバー / タブ
# -------------------------------------------------
current_account, current_user = get_identity()

st.markdown(f"### 📊 データ閲覧 & ダウンロード")
st.markdown(
    f"<span class='small-muted'>アカウント: <b>{current_account}</b> / ユーザー: <b>{current_user}</b></span>",
    unsafe_allow_html=True
)

# コマンドバー（DB/スキーマは当面固定）
target_db = sanitize_ident(TARGETS[0]["db"])
target_schema = sanitize_ident(TARGETS[0]["schema"])

# 権限ベースの候補一覧
all_tables = get_allowed_tables()

cb_left, cb_mid, cb_right = st.columns([1.2, 2.4, 2])
with cb_left:
    st.markdown(f"**対象**: `{target_db}.{target_schema}`")
with cb_mid:
    selected_table = st.selectbox("テーブル/ビューを選択", all_tables, index=0 if all_tables else None, placeholder="選択してください")
with cb_right:
    col_exec, col_save, col_dl = st.columns([1, 1, 1])
    with col_exec:
        exec_clicked = st.button("実行 ▶", type="primary", help="直近の設定でプレビューを再実行")
    with col_save:
        save_clicked = st.button("💾 保存")
    with col_dl:
        dl_clicked = st.button("📥 DL準備")

st.markdown("<hr/>", unsafe_allow_html=True)
tabs = st.tabs(["① プレビュー & フィルタ", "② 複数テーブル結合", "③ 保存したSQL", "④ メタ情報"])

# -------------------------------------------------
# ① プレビュー & フィルタ タブ
# -------------------------------------------------
with tabs[0]:
    if not selected_table:
        st.info("上のコマンドバーでテーブル／ビューを選んでください。")
    else:
        left, right = st.columns([1, 1.6])

        # カラム情報
        tname = sanitize_ident(selected_table)
        df_columns = run_query(f"""
            SELECT COLUMN_NAME, DATA_TYPE, COMMENT
            FROM {target_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='{target_schema}' AND TABLE_NAME='{tname}'
            ORDER BY ORDINAL_POSITION
        """)
        columns = df_columns["COLUMN_NAME"].tolist()

        with left:
            st.subheader("設定")
            with st.form(key="filter_form", clear_on_submit=False):
                default_cols = st.session_state.get("selected_columns", columns[: min(5, len(columns))])
                selected_columns = st.multiselect("表示するカラム", columns, default=default_cols)

                # 基本フィルタ（簡易）
                st.markdown("**基本フィルタ（部分一致）**")
                basic_filters = []
                for col in selected_columns:
                    val = st.text_input(f"{col}", key=f"like_{col}")
                    if val.strip():
                        basic_filters.append(f'"{col}" LIKE \'%{val.strip()}%\'')

                # 詳細フィルタ（折りたたみ）
                with st.expander("詳細条件（数値・日付など）", expanded=False):
                    adv_filters = []
                    for col in selected_columns:
                        dtype = df_columns.loc[df_columns["COLUMN_NAME"] == col, "DATA_TYPE"].iloc[0].upper()
                        if any(t in dtype for t in ["NUMBER", "INT", "FLOAT", "DECIMAL", "DOUBLE"]):
                            c1, c2 = st.columns(2)
                            with c1:
                                minv = st.text_input(f"{col} 最小値", key=f"min_{col}")
                            with c2:
                                maxv = st.text_input(f"{col} 最大値", key=f"max_{col}")
                            if minv.strip(): adv_filters.append(f'"{col}" >= {minv.strip()}')
                            if maxv.strip(): adv_filters.append(f'"{col}" <= {maxv.strip()}')
                        elif "DATE" in dtype or "TIME" in dtype:
                            c1, c2 = st.columns(2)
                            with c1:
                                date_from = st.date_input(f"{col} 以降", value=None, key=f"from_{col}")
                            with c2:
                                date_to = st.date_input(f"{col} 以前", value=None, key=f"to_{col}")
                            if date_from: adv_filters.append(f'"{col}" >= \'{date_from}\'')
                            if date_to:   adv_filters.append(f'"{col}" <= \'{date_to}\'')

                submitted = st.form_submit_button("適用（プレビュー100件）", type="primary")
                if submitted:
                    st.session_state.selected_columns = selected_columns
                    clauses = basic_filters + adv_filters
                    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
                    quoted_cols = [f'"{c}"' for c in selected_columns] if selected_columns else ['*']
                    fq_table = f"{target_db}.{target_schema}.{tname}"
                    sql = f'SELECT {", ".join(quoted_cols)} FROM {fq_table} {where_sql} LIMIT 100'
                    try:
                        df = run_query(sql)
                        st.session_state.df = df
                        st.session_state.last_query = f'SELECT {", ".join(quoted_cols)} FROM {fq_table} {where_sql}'
                        st.success(f"プレビュー {len(df)} 件を取得しました。")
                    except Exception as e:
                        st.error("取得に失敗しました。")
                        st.write(e)

        with right:
            st.subheader("結果")
            if exec_clicked:
                # 直近のlast_queryがあれば再実行（LIMIT 100）
                if st.session_state.get("last_query"):
                    try:
                        df = run_query(st.session_state["last_query"] + " LIMIT 100")
                        st.session_state.df = df
                        st.success("再実行しました。")
                    except Exception as e:
                        st.error("再実行に失敗しました。")
                        st.write(e)
                else:
                    st.warning("実行可能なクエリがありません。左側で条件を設定して適用してください。")

            if st.session_state.get("last_query"):
                st.markdown("**実行されたSQL（プレビュー用）**")
                st.code(st.session_state["last_query"] + " LIMIT 100", language="sql")

            if isinstance(st.session_state.get("df"), pd.DataFrame) and not st.session_state.df.empty:
                st.dataframe(st.session_state.df.head(50), use_container_width=True)

                # 件数チェック
                with st.container():
                    show_count = st.checkbox("件数を計算する（フィルタ後）", value=False)
                    if show_count:
                        # last_query から FROM ... WHERE ... をそのまま使う
                        from_where = st.session_state["last_query"].split("FROM", 1)[1]
                        cnt_sql = "SELECT COUNT(*) AS cnt FROM " + from_where
                        try:
                            total = run_query(cnt_sql).iloc[0, 0]
                            st.markdown(f"<span class='badge badge-run'>件数: {total} 件</span>", unsafe_allow_html=True)
                        except Exception as e:
                            st.warning("件数計算に失敗しました。")
                            st.write(e)

                # ダウンロード準備・S3・高速ZIP
                def get_full_table():
                    sql = st.session_state.get("last_query", "")
                    return run_query(sql) if sql else pd.DataFrame()
                if dl_clicked:
                    with st.spinner("全件データを取得中..."):
                        st.session_state.df_for_download = get_full_table()
                    st.success(f"全件 {len(st.session_state.df_for_download)} 件を取得しました。")
                download_ready_ui(st.session_state.df, selected_table, get_full_table)

            # 保存（名前入力）
            if save_clicked:
                st.info("このSQLを保存します。保存名を入力して「保存実行」を押してください。")
                c1, c2 = st.columns([2, 1])
                with c1:
                    save_name = st.text_input("保存名", key="save_sql_name_ui")
                with c2:
                    if st.button("保存実行"):
                        if st.session_state.get("last_query"):
                            save_sql_to_log(current_user, st.session_state["last_query"], save_name)
                        else:
                            st.warning("保存対象のSQLがありません。")
                show_sql_save_message()

# -------------------------------------------------
# ② 複数テーブル結合 タブ
# -------------------------------------------------
with tabs[1]:
    st.subheader("複数テーブル結合プレビュー")

    # セッション状態
    if "chain_base_table" not in st.session_state:
        st.session_state.chain_base_table = selected_table if selected_table else ""
    if "chain_steps" not in st.session_state:
        st.session_state.chain_steps = []
    if "chain_preview" not in st.session_state:
        st.session_state.chain_preview = None
    if "chain_total_count" not in st.session_state:
        st.session_state.chain_total_count = None
    if "chain_sql" not in st.session_state:
        st.session_state.chain_sql = None
    if "chain_download_ready" not in st.session_state:
        st.session_state.chain_download_ready = False
    if "chain_df_for_download" not in st.session_state:
        st.session_state.chain_df_for_download = None

    uiL, uiR = st.columns([1, 1.6])

    with uiL:
        base_table = st.selectbox(
            "主テーブル/ビュー",
            [""] + all_tables,
            index=([""] + all_tables).index(st.session_state.chain_base_table)
                  if st.session_state.chain_base_table in ([""] + all_tables) else 0
        )
        if base_table != st.session_state.chain_base_table:
            st.session_state.chain_base_table = base_table
            st.session_state.chain_preview = None
            st.session_state.chain_total_count = None
            st.session_state.chain_sql = None
            st.session_state.chain_download_ready = False

        select_all_cols = st.checkbox("プレビューで全列を表示（遅くなります）", value=False)

        c_add, c_clear = st.columns(2)
        with c_add:
            if st.button("＋ 結合ステップを追加"):
                st.session_state.chain_steps.append({
                    "right_table": "",
                    "left_key": [],
                    "right_key": [],
                    "how": "INNER"
                })
                st.session_state.chain_preview = None
                st.session_state.chain_total_count = None
                st.session_state.chain_sql = None
                st.session_state.chain_download_ready = False
        with c_clear:
            if st.button("🧹 すべてクリア"):
                st.session_state.chain_steps = []
                st.session_state.chain_preview = None
                st.session_state.chain_total_count = None
                st.session_state.chain_sql = None
                st.session_state.chain_download_ready = False

        # ステップ設定
        remove_index = None
        for i, step in enumerate(st.session_state.chain_steps):
            st.markdown(f"**Step {i+1}**")
            lt = base_table if i == 0 else st.session_state.chain_steps[i-1]["right_table"]

            # キー候補
            def cols_of(tbl):
                if not tbl:
                    return []
                dfc = run_query(f"""
                    SELECT COLUMN_NAME
                    FROM {target_db}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA='{target_schema}' AND TABLE_NAME='{sanitize_ident(tbl)}'
                    ORDER BY ORDINAL_POSITION
                """)
                return dfc["COLUMN_NAME"].tolist()

            left_cols = cols_of(lt)
            options = [""] + [t for t in all_tables if t != lt] if lt else [""] + all_tables
            step["right_table"] = st.selectbox(
                f"結合先テーブル/ビュー (Step {i+1})", options,
                index=options.index(step.get("right_table","")) if step.get("right_table","") in options else 0,
                key=f"rt_{i}"
            )
            step["how"] = st.selectbox(
                "結合方法", ["INNER","LEFT","RIGHT","FULL"],
                index=["INNER","LEFT","RIGHT","FULL"].index(step.get("how","INNER")),
                key=f"how_{i}"
            )

            right_cols = cols_of(step["right_table"])
            c1, c2, c3 = st.columns([1, 1, .3])
            with c1:
                step["left_key"] = st.multiselect(f"左キー（{lt or '未選択'}）", left_cols, default=step.get("left_key", []), key=f"lk_{i}")
            with c2:
                step["right_key"] = st.multiselect(f"右キー（{step['right_table'] or '未選択'}）", right_cols, default=step.get("right_key", []), key=f"rk_{i}")
            with c3:
                if st.button("削除", key=f"rm_{i}"):
                    remove_index = i

            if step["left_key"] and step["right_key"] and len(step["left_key"]) != len(step["right_key"]):
                st.warning("⚠ 左右のキー数が一致していません。")

        if remove_index is not None:
            st.session_state.chain_steps.pop(remove_index)
            st.session_state.chain_preview = None
            st.session_state.chain_total_count = None
            st.session_state.chain_sql = None
            st.session_state.chain_download_ready = False

        def build_from_clause(base: str, steps: list) -> str | None:
            if not base:
                return None
            clause = f'{target_db}.{target_schema}.{sanitize_ident(base)}'
            current_left = sanitize_ident(base)
            for s in steps:
                rt = sanitize_ident(s["right_table"]) if s["right_table"] else ""
                lks = s["left_key"]
                rks = s["right_key"]
                how = s["how"]
                if not (rt and lks and rks and how and len(lks)==len(rks)):
                    return None
                on_clause = " AND ".join([f'{current_left}."{lk}" = {rt}."{rk}"' for lk, rk in zip(lks, rks)])
                clause += f' {how} JOIN {target_db}.{target_schema}.{rt} ON {on_clause}'
                current_left = rt
            return clause

        def build_select_clause(base: str, steps: list) -> str:
            tables = [sanitize_ident(base)] + [sanitize_ident(s["right_table"]) for s in steps if s["right_table"]]
            select_cols = []
            for t in tables:
                dfc = run_query(f"""
                    SELECT COLUMN_NAME
                    FROM {target_db}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA='{target_schema}' AND TABLE_NAME='{t}'
                    ORDER BY ORDINAL_POSITION
                """)
                cols = dfc["COLUMN_NAME"].tolist()
                if not select_all_cols:
                    cols = cols[:20]
                for c in cols:
                    select_cols.append(f'{t}."{c}" AS "{t}_{c}"')
            return ", ".join(select_cols)

        if st.button("データを取得して表示（プレビュー100件）"):
            from_clause = build_from_clause(st.session_state.chain_base_table, st.session_state.chain_steps)
            if not from_clause:
                st.error("未設定のステップ（テーブル/キー/方法）があります。")
            else:
                select_clause = build_select_clause(st.session_state.chain_base_table, st.session_state.chain_steps)
                sql = f"SELECT {select_clause} FROM {from_clause}"
                try:
                    df_preview = run_query(sql + " LIMIT 100")
                    cnt = run_query(f"SELECT COUNT(*) AS cnt FROM {from_clause}").iloc[0, 0]
                    st.session_state.chain_sql = sql
                    st.session_state.chain_preview = df_preview
                    st.session_state.chain_total_count = cnt
                    st.session_state.chain_download_ready = True
                    st.session_state.show_save_ui = True
                except Exception as e:
                    st.error("SQL実行でエラーが発生しました。")
                    st.write(e)
                    st.session_state.chain_download_ready = False

    with uiR:
        if st.session_state.get("chain_sql"):
            st.markdown("**実行されたSQL**")
            st.code(st.session_state.chain_sql, language="sql")
        if st.session_state.get("chain_preview") is not None:
            st.dataframe(st.session_state.chain_preview, use_container_width=True)
            st.markdown(f"<span class='badge badge-run'>全件数: {st.session_state.chain_total_count} 件</span>", unsafe_allow_html=True)

            # DL準備（全件）/ 高速ZIP / S3
            cA, cB = st.columns([1.2, 1])
            with cA:
                if st.button("📥 ダウンロード準備（全件取得）", key="chain_full"):
                    with st.spinner("全件データを取得中..."):
                        df_all = run_query(st.session_state.chain_sql)
                        st.session_state.chain_df_for_download = df_all
                    st.success(f"全件データを取得しました ({len(st.session_state.chain_df_for_download)}件)")
            with cB:
                # 高速ZIP
                sep_choice = st.radio("区切り", ["CSV", "TSV"], horizontal=True, key="chain_sep")
                sep = "\t" if sep_choice == "TSV" else ","
                quote_option = st.selectbox("囲い文字", ['"', "'"], index=0, key="chain_quote")
                if st.button("📥 高速ZIPダウンロード", key="chain_zip"):
                    data = stream_query_to_zip(st.session_state.chain_sql, sep=sep, quotechar=quote_option)
                    st.download_button(
                        "📥 ダウンロード開始",
                        data=data,
                        file_name=f"{st.session_state.chain_base_table}_{datetime.date.today():%Y%m%d}_server.zip",
                        mime="application/zip",
                        key="chain_zip_dl"
                    )
            # S3
            cS1, cS2 = st.columns(2)
            with cS1:
                if st.button("⤴ S3アップロード(CSV)", key="chain_s3_csv"):
                    S3_upload(st.session_state.chain_sql, DELIMITER_COMMA, "_join_CSV")
            with cS2:
                if st.button("⤴ S3アップロード(TSV)", key="chain_s3_tsv"):
                    S3_upload(st.session_state.chain_sql, DELIMITER_TAB, "_join_TSV")

            # DL（DF準備済み）
            if st.session_state.get("chain_df_for_download") is not None and not st.session_state.chain_df_for_download.empty:
                show_download_ui(
                    st.session_state.chain_df_for_download,
                    table_name="_".join([st.session_state.chain_base_table] + [s["right_table"] for s in st.session_state.chain_steps if s["right_table"]]),
                    key_prefix="chain_dl"
                )

        # SQL保存
        if st.session_state.get("chain_sql"):
            with st.expander("💾 このSQLを保存", expanded=False):
                save_name = st.text_input("保存名", key="save_chain_sql_name")
                if st.button("保存実行", key="save_chain_sql_btn"):
                    save_sql_to_log(current_user, st.session_state.chain_sql, save_name)
                show_sql_save_message()

# -------------------------------------------------
# ③ 保存したSQL タブ
# -------------------------------------------------
with tabs[2]:
    st.subheader("保存したSQLの再実行")
    df_sql_log = run_query(f"""
SELECT "LOG_ID", "SQL_NAME", "EXEC_QUERY", "SAVE_DATE"
FROM {target_db}.{target_schema}.SQL_LOG
WHERE "USER_ID" = '{current_user}'
ORDER BY "SAVE_DATE" DESC
""")
    if df_sql_log.empty:
        st.info("まだ保存されたSQLはありません。")
    else:
        selected_id = st.selectbox(
            "再実行したいSQLを選択",
            options=df_sql_log["LOG_ID"],
            format_func=lambda x: f"{df_sql_log.loc[df_sql_log['LOG_ID']==x, 'SQL_NAME'].values[0]} ({df_sql_log.loc[df_sql_log['LOG_ID']==x, 'SAVE_DATE'].values[0]})"
        )
        if st.session_state.get("last_selected_id") != selected_id:
            st.session_state.df_preview = None
            st.session_state.df_for_download = None
            st.session_state.last_selected_id = selected_id

        selected_sql = df_sql_log.loc[df_sql_log["LOG_ID"] == selected_id, "EXEC_QUERY"].values[0]
        cleaned_sql = clean_sql(selected_sql)
        preview_sql = cleaned_sql
        if not re.search(r"(?i)LIMIT\\s+\\d+", selected_sql):
            preview_sql += " LIMIT 100"

        st.markdown("**SQLプレビュー**")
        st.code(preview_sql, language="sql")

        c1, c2 = st.columns([1, 2])
        with c1:
            if st.button("このSQLを実行（プレビュー100件）"):
                try:
                    df_preview = run_query(preview_sql)
                    st.session_state.df_preview = df_preview
                    st.success(f"プレビュー取得完了 ({len(df_preview)}件)")
                    st.session_state.last_query = cleaned_sql  # LIMITなしを保持
                except Exception as e:
                    st.error(f"プレビュー取得に失敗しました: {e}")

        if st.session_state.get("df_preview") is not None:
            st.dataframe(st.session_state.df_preview, use_container_width=True)

            # DL準備
            def get_full_data_saved() -> pd.DataFrame:
                sql = st.session_state.get("last_query", "")
                if not sql:
                    st.warning("実行対象のSQLがありません。")
                    return pd.DataFrame()
                sql_full = clean_sql(remove_limit(sql))
                return run_query(sql_full)

            download_ready_ui(
                df_preview=st.session_state.df_preview,
                table_name=f"savedSQL_{selected_id}",
                sql_all_func=get_full_data_saved
            )

# -------------------------------------------------
# ④ メタ情報 タブ
# -------------------------------------------------
with tabs[3]:
    st.subheader("メタ情報")
    if not selected_table:
        st.info("コマンドバーでテーブル／ビューを選択してください。")
    else:
        # テーブルコメント
        df_comment = run_query(f"""
            SELECT COMMENT
            FROM {target_db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='{target_schema}' AND TABLE_NAME='{sanitize_ident(selected_table)}'
        """)
        comment = df_comment.iloc[0,0] if (not df_comment.empty and df_comment.iloc[0,0]) else "(説明なし)"
        st.markdown(f"**テーブル説明:** {comment}")

        # カラム辞書（CODE_M）
        st.markdown("**カラム辞書（CODE_M）サンプル**")
        df_cols = run_query(f"""
            SELECT COLUMN_NAME, DATA_TYPE, COMMENT
            FROM {target_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='{target_schema}' AND TABLE_NAME='{sanitize_ident(selected_table)}'
            ORDER BY ORDINAL_POSITION
        """)
        cols = df_cols["COLUMN_NAME"].tolist()
        if cols:
            col_list = "', '".join(cols)
            code_df = run_query(f"""
SELECT "カラム名", "コード値", "コード値名称"
FROM {target_db}.{target_schema}.CODE_M
WHERE "カラム名" IN ('{col_list}')
""")
            if code_df.empty:
                st.caption("CODE_M に対応エントリはありません。")
            else:
                st.dataframe(code_df.head(200), use_container_width=True)
