# -*- coding: utf-8 -*-
# UI改善版 (v3)
# - タブ①（フィルタ）とタブ②（結合）を「SQLビルダー」タブに統合
# - SQL保存機能、S3アップロード機能を削除し、UIを簡素化
# - 操作フロー: [Step 1: テーブル定義] -> [Step 2: 条件指定] -> [Step 3: 実行]
# - フィルタ条件を動的に追加・削除できるUIに変更
# - フィルタUIのラベルを非表示化
# - (v3) フィルタ条件フォームを横並びに変更
# - (v3) 結合ステップの削除ボタンを右端に配置
# - (v3) SELECT句もst.expanderで囲む

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
CSV_MAX = 50000   # CSV/TSV のZIP分割行数
EXCEL_MAX = 50000 # Excelの最大行数

# -------------------------------------------------
# セッションステート初期化
# -------------------------------------------------
# SQLビルダータブの状態
if "builder_base_table" not in st.session_state:
    st.session_state.builder_base_table = ""
if "builder_join_steps" not in st.session_state:
    st.session_state.builder_join_steps = []
if "builder_available_columns" not in st.session_state:
    # (例: [{"fq_name": "TBL.COL", "table": "TBL", "column": "COL", "dtype": "VARCHAR"}, ...])
    st.session_state.builder_available_columns = []
if "builder_where_conditions" not in st.session_state: # 動的フィルタ用
    # (例: [{"id": 1, "column": "TBL.COL", "operator": "=", "value": "abc"}, ...])
    st.session_state.builder_where_conditions = []
if "builder_where_next_id" not in st.session_state: # 動的フィルタ用
    st.session_state.builder_where_next_id = 0
if "builder_selected_columns" not in st.session_state:
    st.session_state.builder_selected_columns = []
if "builder_sql" not in st.session_state:
    st.session_state.builder_sql = ""
if "builder_df_preview" not in st.session_state:
    st.session_state.builder_df_preview = pd.DataFrame()
if "builder_df_for_download" not in st.session_state:
    st.session_state.builder_df_for_download = pd.DataFrame()


# -------------------------------------------------
# 接続・共通クエリ関数
# -------------------------------------------------
@st.cache_resource
def get_conn():
    """Snowflake接続をセッション内で再利用"""
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
    return sorted(list(all_effective))

@st.cache_data(ttl=300)
def get_columns_for_table(target_db: str, target_schema: str, table_name: str) -> list[dict]:
    """指定されたテーブルのカラム情報（名前、型）を取得"""
    tname = sanitize_ident(table_name)
    target_db = sanitize_ident(target_db)
    target_schema = sanitize_ident(target_schema)
    if not tname:
        return []
    
    df_columns = run_query(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM {target_db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='{target_schema}' AND TABLE_NAME='{tname}'
        ORDER BY ORDINAL_POSITION
    """)
    # 辞書のリストとして返す
    return df_columns.to_dict('records')


# -------------------------------------------------
# ダウンロード / ストリーミング
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
            elif out.tell() > 0: # 最後のバッチを書き込む
                zf.writestr(f"part{part_no}.csv", out.getvalue().encode("utf-8"))
    buf.seek(0)
    return buf

def show_download_ui(df: pd.DataFrame, file_name_prefix: str, key_prefix: str = "download"):
    """ダウンロードUI（DataFrame準備済み用）"""
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
        st.write("") # ボタンを中央揃えにするためのダミー
        st.write("")
        if ft in ("csv","tsv"):
            data = generate_download(df, filetype=ft, quote_option=quote_option)
            st.download_button(
                label="📥 ZIPダウンロード",
                data=data,
                file_name=f"{file_name_prefix}_{today_str}_{ft.upper()}.zip",
                mime="application/zip",
                key=f"{key_prefix}_dlzip"
            )
        else:
            data = generate_download(df, filetype="excel")
            if data:
                st.download_button(
                    label="📥 Excelダウンロード",
                    data=data,
                    file_name=f"{file_name_prefix}_{today_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"{key_prefix}_xldl"
                )
            else:
                st.warning(f"Excelは{EXCEL_MAX}件を超えるためダウンロードできません。")

def download_ready_ui(df_preview: pd.DataFrame, table_name: str, sql_all_func, sql_query: str):
    """
    従来の「全件をDFに積んでからDL」＋ 高速ストリーミングDL（DF不要）の両対応。
    （S3機能は削除）
    """
    if df_preview.empty:
        st.warning("ダウンロード可能なデータがありません。")
        return
        
    st.markdown("**ダウンロード**")
    colA, colB = st.columns(2)
    
    with colA:
        st.caption("A: 全件取得してからダウンロード")
        if st.button("📥 ダウンロード準備（全件取得）"):
            with st.spinner("全件データを取得中..."):
                st.session_state.builder_df_for_download = sql_all_func()
            st.success(f"全件データを取得しました ({len(st.session_state.builder_df_for_download)}件)")
        
        # 全件DFが準備できたらDLボタンを表示
        if not st.session_state.builder_df_for_download.empty:
            show_download_ui(
                st.session_state.builder_df_for_download,
                file_name_prefix=table_name,
                key_prefix="builder_dl_full"
            )

    with colB:
        st.caption("B: 高速ストリーミング（大容量向け）")
        sep_choice = st.radio("区切り", ["CSV", "TSV"], horizontal=True, key="fast_sep")
        sep = "\t" if sep_choice == "TSV" else ","
        quote_option = st.selectbox("囲い文字", ['"', "'"], index=0, key="fast_quote")
        
        if st.button("📥 高速ZIPダウンロード（ストリーミング）"):
            if not sql_query:
                st.error("実行対象のSQLがありません。")
                return

            with st.spinner("ZIP生成中..."):
                data = stream_query_to_zip(sql_query, sep=sep, quotechar=quote_option)
            
            st.download_button(
                "📥 ダウンロード開始",
                data=data,
                file_name=f"{table_name}_{datetime.date.today():%Y%m%d}_server.zip",
                mime="application/zip",
                key="fast_zip_dl"
            )

# -------------------------------------------------
# SQL整形
# -------------------------------------------------
_LIMIT_PATTERN = re.compile(r"(?i)LIMIT\s+\d+")

def remove_limit(sql: str) -> str:
    return _LIMIT_PATTERN.sub("", sql).strip()

def clean_sql(sql: str) -> str:
    if not sql:
        return ""
    return sql.strip().rstrip(";")

def get_full_data_builder() -> pd.DataFrame:
    """SQLビルダーのセッションから全件取得"""
    sql = st.session_state.get("builder_sql", "")
    if not sql:
        st.warning("実行対象のSQLがありません。")
        return pd.DataFrame()
    sql_full = clean_sql(remove_limit(sql))
    return run_query(sql_full)

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

# コマンドバーは対象表示のみに簡素化
st.markdown(f"**対象**: `{target_db}.{target_schema}`")

st.markdown("<hr/>", unsafe_allow_html=True)
tabs = st.tabs(["① SQLビルダー (フィルタ & 結合)", "② メタ情報"])


# -------------------------------------------------
# ① SQLビルダー タブ
# -------------------------------------------------
with tabs[0]:
    st.subheader("SQLビルダー")
    
    # -----------------
    # Step 1: テーブル定義 (FROM / JOIN)
    # -----------------
    st.markdown("#### Step 1: テーブル定義 (FROM / JOIN)")
    
    # ベーステーブル
    base_table_options = [""] + all_tables
    try:
        base_table_index = base_table_options.index(st.session_state.builder_base_table)
    except ValueError:
        base_table_index = 0
        
    base_table = st.selectbox(
        "主テーブル/ビュー",
        base_table_options,
        index=base_table_index,
        key="builder_base_table_select" # st.session_state.builder_base_table と連動させる
    )
    if base_table != st.session_state.builder_base_table:
        st.session_state.builder_base_table = base_table
        st.session_state.builder_join_steps = [] # ベース変更でリセット
        st.session_state.builder_available_columns = []
        st.rerun()

    # 結合ステップ
    c_add, c_clear = st.columns(2)
    with c_add:
        if st.button("＋ 結合ステップを追加"):
            st.session_state.builder_join_steps.append({
                "right_table": "",
                "left_key": [],
                "right_key": [],
                "how": "INNER"
            })
    with c_clear:
        if st.button("🧹 結合ステップをクリア"):
            st.session_state.builder_join_steps = []
            st.rerun()

    # 結合ステップUI
    remove_index = None
    all_join_tables_valid = True
    
    # カラム取得関数（キャッシュ活用）
    @st.cache_data(ttl=300)
    def get_cols(tbl_name):
        if not tbl_name: return []
        cols_data = get_columns_for_table(target_db, target_schema, tbl_name)
        return [c["COLUMN_NAME"] for c in cols_data]

    current_left_table = st.session_state.builder_base_table
    
    for i, step in enumerate(st.session_state.builder_join_steps):
        with st.container(border=True):
            # (修正) ヘッダー行にタイトルと削除ボタンを配置
            col_title, col_del_btn = st.columns([1, 0.1])
            with col_title:
                st.markdown(f"**Join Step {i+1}**")
            with col_del_btn:
                if st.button("✕", key=f"rm_{i}", help="このステップを削除"):
                    remove_index = i

            left_cols = get_cols(current_left_table)
            options = [""] + [t for t in all_tables if t != current_left_table] if current_left_table else [""] + all_tables
            
            try:
                rt_index = options.index(step.get("right_table",""))
            except ValueError:
                rt_index = 0
            
            step["right_table"] = st.selectbox(
                f"結合先テーブル/ビュー (Step {i+1})", options,
                index=rt_index,
                key=f"rt_{i}"
            )
            step["how"] = st.selectbox(
                "結合方法", ["INNER","LEFT","RIGHT","FULL"],
                index=["INNER","LEFT","RIGHT","FULL"].index(step.get("how","INNER")),
                key=f"how_{i}"
            )

            right_cols = get_cols(step["right_table"])
            
            # (修正) 削除ボタンをヘッダーに移動したため、キー入力エリアのレイアウト変更
            c1, c2 = st.columns(2) 
            with c1:
                step["left_key"] = st.multiselect(f"左キー（{current_left_table or '未選択'}）", left_cols, default=step.get("left_key", []), key=f"lk_{i}")
            with c2:
                step["right_key"] = st.multiselect(f"右キー（{step['right_table'] or '未選択'}）", right_cols, default=step.get("right_key", []), key=f"rk_{i}")

            if step["left_key"] and step["right_key"] and len(step["left_key"]) != len(step["right_key"]):
                st.warning("⚠ 左右のキー数が一致していません。")
            
            if not step["right_table"] or not step["left_key"] or not step["right_key"]:
                all_join_tables_valid = False

            current_left_table = step["right_table"] # 次のステップの左側は、今のステップの右側

    if remove_index is not None:
        st.session_state.builder_join_steps.pop(remove_index)
        st.rerun()

    # -----------------
    # Step 2: 条件指定 (WHERE / SELECT)
    # -----------------
    st.markdown("#### Step 2: 条件指定 (WHERE / SELECT)")

    # Step 1 の定義から利用可能なカラム一覧を生成
    if st.button("テーブル定義を確定（Step 2 のカラムを更新）"):
        st.session_state.builder_available_columns = []
        st.session_state.builder_where_conditions = [] # 動的UI用リセット
        st.session_state.builder_where_next_id = 0 # 動的UI用リセット
        st.session_state.builder_selected_columns = []
        
        tables_in_use = {} # 重複テーブルにエイリアスを付与
        
        def add_cols(tbl_name, alias):
            cols_data = get_columns_for_table(target_db, target_schema, tbl_name)
            for c in cols_data:
                st.session_state.builder_available_columns.append({
                    "fq_name": f"{alias}.{c['COLUMN_NAME']}",
                    "table_alias": alias,
                    "table_name": tbl_name,
                    "column": c['COLUMN_NAME'],
                    "dtype": c['DATA_TYPE'].upper()
                })

        if st.session_state.builder_base_table:
            base_alias = sanitize_ident(st.session_state.builder_base_table)
            tables_in_use[base_alias] = 1
            add_cols(st.session_state.builder_base_table, base_alias)

        for step in st.session_state.builder_join_steps:
            if step["right_table"]:
                alias = sanitize_ident(step["right_table"])
                if alias in tables_in_use:
                    tables_in_use[alias] += 1
                    alias = f"{alias}_{tables_in_use[alias]}" # TBL_2
                else:
                    tables_in_use[alias] = 1
                add_cols(step["right_table"], alias)
        
        st.success(f"{len(st.session_state.builder_available_columns)} 件のカラムを読込みました。")

    if st.session_state.builder_available_columns:
        
        # --- フィルタ (WHERE) ---
        # st.form の外で動的に管理
        with st.expander("フィルタ条件 (WHERE)", expanded=True):
            all_cols_fq_names = [c["fq_name"] for c in st.session_state.builder_available_columns]
            
            # 条件追加ボタン
            if st.button("＋ フィルタ条件を追加"):
                new_id = st.session_state.builder_where_next_id
                st.session_state.builder_where_conditions.append({
                    "id": new_id,
                    "column": all_cols_fq_names[0] if all_cols_fq_names else "",
                    "operator": "LIKE",
                    "value": ""
                })
                st.session_state.builder_where_next_id += 1
                st.rerun()

            # 既存の条件をループ表示
            indices_to_remove = []
            for i, condition in enumerate(st.session_state.builder_where_conditions):
                # ユニークキーのため id を使用
                condition_id = condition["id"]
                
                # (修正) フォームを横並びに
                c1, c2, c3, c4 = st.columns([3, 2, 3, 1])
                
                # 1. カラム選択
                with c1:
                    try:
                        col_index = all_cols_fq_names.index(condition["column"])
                    except ValueError:
                        col_index = 0
                    condition["column"] = st.selectbox(
                        "カラム",
                        all_cols_fq_names, 
                        index=col_index, 
                        key=f"where_col_{condition_id}",
                        label_visibility="collapsed"
                    )
                
                # 2. 演算子選択
                with c2:
                    operators = ["LIKE", "=", "!=", ">", ">=", "<", "<=", "IS NULL", "IS NOT NULL"]
                    try:
                        op_index = operators.index(condition["operator"])
                    except ValueError:
                        op_index = 0
                    condition["operator"] = st.selectbox(
                        "演算子",
                        operators, 
                        index=op_index, 
                        key=f"where_op_{condition_id}",
                        label_visibility="collapsed"
                    )

                # 3. 値入力
                is_null_op = condition["operator"] in ["IS NULL", "IS NOT NULL"]
                with c3:
                    condition["value"] = st.text_input(
                        "値",
                        value=condition["value"], 
                        key=f"where_val_{condition_id}",
                        disabled=is_null_op,
                        placeholder="値 (IS NULL/NOT NULL は空欄)",
                        label_visibility="collapsed"
                    )

                # 4. 削除ボタン
                with c4:
                    if st.button("削除", key=f"where_del_{condition_id}"):
                        indices_to_remove.append(i)


            # 削除処理（ループの外で実行）
            if indices_to_remove:
                # 後ろのインデックスから削除する
                for index in sorted(indices_to_remove, reverse=True):
                    st.session_state.builder_where_conditions.pop(index)
                st.rerun()


        # --- フォーム (SELECT と 実行) ---
        with st.form(key="select_form", clear_on_submit=False):
            
            # --- 表示カラム (SELECT) ---
            # (修正) st.expander で囲む
            with st.expander("表示カラム (SELECT)", expanded=True):
                default_cols = st.session_state.builder_selected_columns or [c["fq_name"] for c in st.session_state.builder_available_columns]
                selected_columns = st.multiselect(
                    "表示するカラムを選択", 
                    [c["fq_name"] for c in st.session_state.builder_available_columns], 
                    default=default_cols,
                    key="builder_select_multiselect",
                    label_visibility="collapsed" # ラベルを非表示
                )
                st.session_state.builder_selected_columns = selected_columns

            # --- 実行 ---
            submitted = st.form_submit_button("適用（プレビュー100件）", type="primary")
            if submitted:
                # SQLを構築
                try:
                    # SELECT句
                    if not st.session_state.builder_selected_columns:
                        st.error("表示カラムを1つ以上選択してください。")
                    else:
                        select_cols = [f'"{c["table_alias"]}"."{c["column"]}" AS "{c["fq_name"]}"' 
                                       for c in st.session_state.builder_available_columns 
                                       if c["fq_name"] in st.session_state.builder_selected_columns]
                        select_sql = f"SELECT {', '.join(select_cols)}"

                        # FROM / JOIN句
                        from_sql = ""
                        tables_in_use = {} # エイリアス管理
                        fq_db_schema = f"{target_db}.{target_schema}"

                        def get_alias(tbl_name):
                            alias = sanitize_ident(tbl_name)
                            if alias in tables_in_use:
                                tables_in_use[alias] += 1
                                alias = f"{alias}_{tables_in_use[alias]}"
                            else:
                                tables_in_use[alias] = 1
                            return alias

                        base_table = st.session_state.builder_base_table
                        if not base_table:
                            raise ValueError("主テーブルが選択されていません。")
                        
                        base_alias = get_alias(base_table)
                        from_sql = f'FROM {fq_db_schema}."{base_table}" AS "{base_alias}"'
                        
                        current_left_alias = base_alias

                        for step in st.session_state.builder_join_steps:
                            if not (step["right_table"] and step["left_key"] and step["right_key"] and len(step["left_key"]) == len(step["right_key"])):
                                raise ValueError("結合ステップの設定が不完全です。")
                            
                            right_alias = get_alias(step["right_table"])
                            how = step["how"]
                            
                            on_clauses = []
                            for lk, rk in zip(step["left_key"], step["right_key"]):
                                on_clauses.append(f'"{current_left_alias}"."{lk}" = "{right_alias}"."{rk}"')
                            on_sql = " AND ".join(on_clauses)
                            
                            from_sql += f' {how} JOIN {fq_db_schema}."{step["right_table"]}" AS "{right_alias}" ON {on_sql}'
                            
                            current_left_alias = right_alias # 連結

                        # WHERE句
                        where_clauses = []
                        
                        # (変更) builder_where_conditions から構築
                        for condition in st.session_state.builder_where_conditions:
                            col_fq_name = condition["column"]
                            operator = condition["operator"]
                            value = condition["value"]

                            col_info = next((c for c in st.session_state.builder_available_columns if c["fq_name"] == col_fq_name), None)
                            if not col_info: continue
                            
                            col_sql = f'"{col_info["table_alias"]}"."{col_info["column"]}"'
                            col_dtype = col_info["dtype"]

                            # IS NULL / IS NOT NULL
                            if operator in ["IS NULL", "IS NOT NULL"]:
                                where_clauses.append(f"{col_sql} {operator}")
                                continue

                            # 値が空の場合はスキップ
                            if not value:
                                continue

                            # 演算子と型に応じて句を構築
                            if operator == "LIKE":
                                where_clauses.append(f"{col_sql} LIKE '%{value}%'")
                            else:
                                # 数値型か？
                                is_numeric_type = any(t in col_dtype for t in ["NUMBER", "INT", "FLOAT", "DECIMAL", "DOUBLE"])
                                if is_numeric_type:
                                    # 値が数値として妥当か（簡易チェック）
                                    if re.fullmatch(r"-?\d+(\.\d+)?", value):
                                        where_clauses.append(f"{col_sql} {operator} {value}")
                                    else:
                                        st.warning(f"警告: カラム {col_fq_name} の値 '{value}' は数値として無効なためスキップされました。")
                                else:
                                    # 文字列・日付・時刻型は ' で囲む
                                    # (注: Snowflakeは日付や時刻も ' で囲む)
                                    value_escaped = value.replace("'", "''") # シングルクォートをエスケープ
                                    where_clauses.append(f"{col_sql} {operator} '{value_escaped}'")
                        
                        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
                        
                        # SQL結合
                        final_sql = f"{select_sql} {from_sql} {where_sql}"
                        st.session_state.builder_sql = final_sql
                        
                        # 実行
                        df_preview = run_query(final_sql + " LIMIT 100")
                        st.session_state.builder_df_preview = df_preview
                        st.session_state.builder_df_for_download = pd.DataFrame() # プレビュー実行でリセット
                        st.success(f"プレビュー {len(df_preview)} 件を取得しました。")

                except Exception as e:
                    st.error(f"SQLの構築または実行に失敗しました: {e}")
                    st.session_state.builder_sql = ""
                    st.session_state.builder_df_preview = pd.DataFrame()

    # -----------------
    # Step 3: 結果表示
    # -----------------
    st.markdown("#### Step 3: 結果表示")
    
    if st.session_state.builder_sql:
        st.markdown("**実行されたSQL（プレビュー用）**")
        st.code(st.session_state.builder_sql + " LIMIT 100", language="sql")
    
    if not st.session_state.builder_df_preview.empty:
        st.dataframe(st.session_state.builder_df_preview.head(50), use_container_width=True)

        # 件数チェック
        with st.container():
            show_count = st.checkbox("件数を計算する（フィルタ後）", value=False)
            if show_count:
                if st.session_state.builder_sql:
                    # SELECT ... FROM ... -> SELECT COUNT(*) FROM ...
                    from_where = st.session_state.builder_sql.split("FROM", 1)[1]
                    cnt_sql = "SELECT COUNT(*) AS cnt FROM " + from_where
                    try:
                        total = run_query(cnt_sql).iloc[0, 0]
                        st.markdown(f"<span class='badge badge-run'>件数: {total} 件</span>", unsafe_allow_html=True)
                    except Exception as e:
                        st.warning("件数計算に失敗しました。")
                        st.write(e)
                else:
                    st.warning("件数計算の元となるSQLがありません。")

        # ダウンロード準備
        download_ready_ui(
            df_preview=st.session_state.builder_df_preview,
            table_name=st.session_state.builder_base_table or "query",
            sql_all_func=get_full_data_builder,
            sql_query=st.session_state.builder_sql
        )
    
    elif st.session_state.builder_available_columns:
        st.info("上記フォームで条件を指定し、「適用（プレビュー100件）」を押してください。")
    else:
        st.info("Step 1 でテーブルを定義し、「Step 2 のカラムを更新」を押してください。")


# -------------------------------------------------
# ② メタ情報 タブ
# -------------------------------------------------
with tabs[1]:
    st.subheader("メタ情報")
    
    # ユーザーにテーブルを選択させる
    selected_table_meta = st.selectbox(
        "メタ情報を表示するテーブル/ビューを選択", 
        all_tables, 
        index=0 if all_tables else None, 
        placeholder="選択してください",
        key="meta_table_select"
    )

    if not selected_table_meta:
        st.info("テーブル／ビューを選択してください。")
    else:
        # テーブルコメント
        df_comment = run_query(f"""
            SELECT COMMENT
            FROM {target_db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='{target_schema}' AND TABLE_NAME='{sanitize_ident(selected_table_meta)}'
        """)
        comment = df_comment.iloc[0,0] if (not df_comment.empty and df_comment.iloc[0,0]) else "(説明なし)"
        st.markdown(f"**テーブル説明:** {comment}")

        # カラム辞書（CODE_M）
        st.markdown("**カラム辞書（CODE_M）サンプル**")
        
        # カラム一覧取得
        cols_data = get_columns_for_table(target_db, target_schema, selected_table_meta)
        
        if not cols_data:
            st.warning("このテーブルのカラム情報を取得できませんでした。")
        else:
            # カラム一覧をDataFrameで表示
            st.markdown(f"**{selected_table_meta} のカラム一覧**")
            st.dataframe(pd.DataFrame(cols_data), use_container_width=True)

            # CODE_M検索
            cols_list_for_sql = "', '".join([c['COLUMN_NAME'] for c in cols_data])
            code_df = run_query(f"""
SELECT "カラム名", "コード値", "コード値名称"
FROM {target_db}.{target_schema}.CODE_M
WHERE "カラム名" IN ('{cols_list_for_sql}')
""")
            if code_df.empty:
                st.caption("CODE_M に対応エントリはありません。")
            else:
                st.dataframe(code_df.head(200), use_container_width=True)
