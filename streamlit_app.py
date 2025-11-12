# -*- coding: utf-8 -*-
# UI改善版 (v12.1) - IndentationError の修正
# - (修正) SQL構築ロジック (v12.0) で発生した IndentationError を修正 (if col_info: の次行)

import streamlit as st
import pandas as pd
import datetime
import re
import snowflake.connector
import uuid 

# -------------------------------------------------
# ページ設定
# -------------------------------------------------
st.set_page_config(page_title="データ閲覧", layout="wide")

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
.small-muted { color: var(--muted); font-size: 0.9rem; }
.badge { display:inline-block; padding: 0.1rem .5rem; border-radius: .4rem; font-size:.8rem; }
.badge-ok { background:#DCFCE7; color:#166534; }
.badge-warn { background:#FEF9C3; color:#854D0E; }
.badge-run { background:#DBEAFE; color:#1E40AF; }
hr { margin: 1.5rem 0; }

.chip-container {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
    padding-top: 10px;
    padding-bottom: 10px;
}
.chip-container div[data-testid="stMarkdown"] p {
    background-color: #f3f4f6; /* 薄いグレー */
    padding: 0.1rem 0.5rem;
    border-radius: 0.5rem;
    display: inline-block;
    margin: 2px;
}

/* カラム選択モーダルのボタン調整 */
div[data-testid="stDialog"] div[data-testid="stButton"] button {
    font-size: 0.9rem; /* ボタン内のフォントを少し小さく */
    text-align: left;  /* ボタンのテキストを左寄せ */
    justify-content: flex-start; /* ボタンのflexコンテナを左寄せ */
    padding: 0.25rem 0.5rem;
}
/* 選択済みカラムの「✕」ボタン */
div[data-testid="stDialog"] div[data-testid="stButton"] button[kind="secondary"] {
    color: var(--muted);
    border-color: #e5e7eb; /* 薄いグレー */
}

</style>
""", unsafe_allow_html=True)

# -------------------------------------------------
# 設定（拡張可能な対象スキーマ）
# -------------------------------------------------
TARGETS = [
    {"db": "TEST_DB", "schema": "TEST"},
    {"db": "TEST_DB", "schema": "SALES"},
    {"db": "TEST_DB", "schema": "MARKETING"},
    {"db": "PROD_DB", "schema": "MARKETING"}, 
]

# -------------------------------------------------
# 定数
# -------------------------------------------------
AGG_FUNCTIONS = ["COUNT", "SUM", "AVG", "MAX", "MIN"]

# -------------------------------------------------
# セッションステート初期化
# -------------------------------------------------
if "current_step" not in st.session_state:
    st.session_state.current_step = 1 # 1:テーブル選択, 2:条件指定, 3:集計・並べ替え, 4:結果

if "builder_base_table" not in st.session_state:
    st.session_state.builder_base_table = ""
if "builder_base_db" not in st.session_state:
    st.session_state.builder_base_db = ""
if "builder_base_schema" not in st.session_state:
    st.session_state.builder_base_schema = ""

if "builder_join_steps" not in st.session_state:
    st.session_state.builder_join_steps = []
if "builder_available_columns" not in st.session_state:
    st.session_state.builder_available_columns = []
if "builder_where_conditions" not in st.session_state:
    st.session_state.builder_where_conditions = []
if "builder_where_next_id" not in st.session_state:
    st.session_state.builder_where_next_id = 0
if "builder_selected_columns" not in st.session_state:
    st.session_state.builder_selected_columns = []
if "builder_sql" not in st.session_state:
    st.session_state.builder_sql = ""
if "builder_df_preview" not in st.session_state:
    st.session_state.builder_df_preview = pd.DataFrame()

if "show_table_modal" not in st.session_state:
    st.session_state.show_table_modal = False
if "table_selection_context" not in st.session_state:
    st.session_state.table_selection_context = None 
if "table_modal_search" not in st.session_state:
    st.session_state.table_modal_search = ""

if "show_column_modal" not in st.session_state:
    st.session_state.show_column_modal = False
if "column_modal_search" not in st.session_state:
    st.session_state.column_modal_search = ""

if "builder_aggregation_steps" not in st.session_state:
    st.session_state.builder_aggregation_steps = []
if "builder_aggregation_next_id" not in st.session_state:
    st.session_state.builder_aggregation_next_id = 0
if "builder_order_by_steps" not in st.session_state:
    st.session_state.builder_order_by_steps = []
if "builder_order_by_next_id" not in st.session_state:
    st.session_state.builder_order_by_next_id = 0


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
    """
    target_db = sanitize_ident(target_db)
    try:
        df = run_query(f"SELECT ROLE_NAME FROM {target_db}.INFORMATION_SCHEMA.ENABLED_ROLES")
        return df["ROLE_NAME"].tolist() if not df.empty else []
    except snowflake.connector.errors.ProgrammingError as e:
        st.warning(f"ロール情報が取得できません (DB: {target_db})。権限がない可能性があります。")
        print(f"Role query failed for {target_db}: {e}") 
        return []

@st.cache_data(ttl=300)
def get_effective_select_objects(
    target_db: str,
    target_schema: str,
    include_views: bool = True,
    include_materialized_views: bool = False
) -> pd.DataFrame:
    """
    対象DB/スキーマ内で SELECT 可能な TABLE/VIEW 等をDataFrameで返す。
    """
    target_db = sanitize_ident(target_db)
    target_schema = sanitize_ident(target_schema)

    roles = get_enabled_roles(target_db) 
    if not roles:
        return pd.DataFrame(columns=["DB", "SCHEMA", "TABLE"])

    obj_types = ["TABLE"]
    if include_views:
        obj_types.append("VIEW")
    if include_materialized_views:
        obj_types.append("MATERIALIZED VIEW")

    obj_types_sql = ", ".join(f"'{t}'" for t in obj_types)
    roles_sql = ", ".join(f"'{r}'" for r in roles)

    q = f"""
        SELECT DISTINCT 
            OBJECT_CATALOG AS DB, 
            OBJECT_SCHEMA AS SCHEMA, 
            OBJECT_NAME AS "TABLE"
        FROM {target_db}.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
        WHERE OBJECT_SCHEMA = '{target_schema}'
          AND OBJECT_TYPE IN ({obj_types_sql})
          AND PRIVILEGE_TYPE IN ('SELECT','OWNERSHIP')
          AND GRANTEE IN ({roles_sql})
        ORDER BY DB, SCHEMA, "TABLE"
    """
    try:
        df = run_query(q)
        return df
    except snowflake.connector.errors.ProgrammingError as e:
        st.warning(f"オブジェクト情報が取得できません (DB: {target_db})。権限がない可能性があります。")
        print(f"Object query failed for {target_db}: {e}") 
        return pd.DataFrame(columns=["DB", "SCHEMA", "TABLE"])

@st.cache_data(ttl=300)
def get_allowed_objects_structured() -> pd.DataFrame:
    """
    権限ベースで参照可能な TABLE/VIEW の一覧を構造化されたDataFrameで返す。
    """
    all_effective_dfs = []
    for t in TARGETS:
        df = get_effective_select_objects(
            target_db=t["db"], target_schema=t["schema"],
            include_views=True, include_materialized_views=False
        )
        if not df.empty:
            all_effective_dfs.append(df)
    
    if not all_effective_dfs:
        return pd.DataFrame(columns=["DB", "SCHEMA", "TABLE"])
        
    final_df = pd.concat(all_effective_dfs).drop_duplicates().reset_index(drop=True)
    return final_df


@st.cache_data(ttl=300)
def get_columns_for_table(target_db: str, target_schema: str, table_name: str) -> list[dict]:
    """指定されたテーブルのカラム情報（名前、型、コメント）を取得"""
    tname = sanitize_ident(table_name)
    target_db_sanitized = sanitize_ident(target_db) 
    target_schema_sanitized = sanitize_ident(target_schema) 
    
    if not tname or not target_db_sanitized or not target_schema_sanitized:
        return []
    
    df_columns = run_query(f"""
        SELECT COLUMN_NAME, DATA_TYPE, COMMENT
        FROM "{target_db_sanitized}".INFORMATION_SCHEMA."COLUMNS"
        WHERE TABLE_SCHEMA = '{target_schema_sanitized}' 
          AND TABLE_NAME = '{tname}'
        ORDER BY ORDINAL_POSITION
    """)
    return df_columns.to_dict('records')


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

def format_sql(sql: str) -> str:
    """SQLを見やすく改行する"""
    if not sql:
        return ""
    
    sql = sql.strip()
    sql = sql.replace(" FROM ", "\nFROM ")
    sql = sql.replace(" LEFT JOIN ", "\nLEFT JOIN ")
    sql = sql.replace(" INNER JOIN ", "\nINNER JOIN ")
    sql = sql.replace(" RIGHT JOIN ", "\nRIGHT JOIN ")
    sql = sql.replace(" FULL JOIN ", "\nFULL JOIN ")
    sql = sql.replace(" WHERE ", "\nWHERE ")
    sql = sql.replace(" GROUP BY ", "\nGROUP BY ")
    sql = sql.replace(" ORDER BY ", "\nORDER BY ")
    sql = sql.replace(" AND ", "\n  AND ")
    sql = sql.replace(" ON ", "\n  ON ")
    return sql


# -------------------------------------------------
# 画面上部：ヘッダ
# -------------------------------------------------
st.markdown(f"### 📊 データ閲覧 & ダウンロード")

# 権限ベースの候補一覧 (構造化データ)
structured_tables_df = get_allowed_objects_structured()


# -------------------------------------------------
# ① SQLビルダー
# -------------------------------------------------
    
# -----------------
# Step 1: テーブル選択
# -----------------
st.markdown("#### テーブル選択")

with st.expander("主テーブル選択", expanded=True):
    
    if st.session_state.builder_base_table:
        st.markdown(
            f"> **{st.session_state.builder_base_db}.{st.session_state.builder_base_schema}.{st.session_state.builder_base_table}**"
        )
    else:
        st.info("テーブルが選択されていません。")

    if st.button("テーブルを選択", key="select_base_table_btn"):
        st.session_state.show_table_modal = True
        st.session_state.table_selection_context = "base"
        st.session_state.table_modal_search = ""
        st.rerun()

with st.expander("テーブル結合（オプション）", expanded=False):
    
    if st.button("＋ 結合を追加"):
        st.session_state.builder_join_steps.insert(0, {
            "id": str(uuid.uuid4()),
            "right_table": "",
            "db": "", 
            "schema": "",
            "left_key": [],
            "right_key": [],
            "how": "INNER"
        })
        st.session_state.current_step = 1
        st.rerun()

    remove_index = None
    all_join_tables_valid = True
    
    @st.cache_data(ttl=300) 
    def get_cols(tbl_name):
        if not tbl_name: return []
        tbl_info = structured_tables_df[structured_tables_df["TABLE"] == tbl_name]
        if tbl_info.empty:
            if tbl_name == st.session_state.builder_base_table:
                    tbl_db = st.session_state.builder_base_db
                    tbl_schema = st.session_state.builder_base_schema
            else:
                return []
        else:
            tbl_db = tbl_info.iloc[0]["DB"]
            tbl_schema = tbl_info.iloc[0]["SCHEMA"]
            
        cols_data = get_columns_for_table(tbl_db, tbl_schema, tbl_name)
        return [c["COLUMN_NAME"] for c in cols_data]

    current_left_table = st.session_state.builder_base_table
    
    for i_reversed, step in enumerate(reversed(st.session_state.builder_join_steps)):
        i_actual = len(st.session_state.builder_join_steps) - 1 - i_reversed
        
        with st.container(border=True):
            col_title, col_del_btn = st.columns([1, 0.1])
            with col_title:
                st.markdown(f"**結合 {i_reversed + 1}**")
            with col_del_btn:
                if st.button("✕", key=f"rm_{i_actual}", help="このステップを削除"):
                    remove_index = i_actual
            
            if step["right_table"]:
                st.markdown(f"> `{step.get('db','')}.{step.get('schema','')}.{step['right_table']}`")
            
            if st.button("テーブルを選択", key=f"select_join_tbl_{i_actual}"):
                st.session_state.show_table_modal = True
                st.session_state.table_selection_context = i_actual
                st.session_state.table_modal_search = ""
                st.rerun()

            step["how"] = st.selectbox(
                "結合方法", ["INNER","LEFT","RIGHT","FULL"],
                index=["INNER","LEFT","RIGHT","FULL"].index(step.get("how","INNER")),
                key=f"how_{i_actual}"
            )

            left_cols = get_cols(current_left_table)
            right_cols = get_cols(step["right_table"])
            
            c1, c2 = st.columns(2) 
            with c1:
                step["left_key"] = st.multiselect(f"左キー（{current_left_table or '未選択'}）", left_cols, default=step.get("left_key", []), key=f"lk_{i_actual}")
            with c2:
                step["right_key"] = st.multiselect(f"右キー（{step['right_table'] or '未選択'}）", right_cols, default=step.get("right_key", []), key=f"rk_{i_actual}")

            if step["left_key"] and step["right_key"] and len(step["left_key"]) != len(step["right_key"]):
                st.warning("⚠ 左右のキー数が一致していません。")
            
            if not step["right_table"] or not step["left_key"] or not step["right_key"]:
                all_join_tables_valid = False

            current_left_table = step["right_table"]

    if remove_index is not None:
            st.session_state.builder_join_steps.pop(remove_index)
            st.session_state.current_step = 1
            st.rerun()

st.write("") 

if st.button("次へ", help="テーブル定義を確定し、条件指定ステップへ進みます", type="primary", use_container_width=True):
    st.session_state.builder_available_columns = []
    st.session_state.builder_where_conditions = []
    st.session_state.builder_where_next_id = 0
    st.session_state.builder_selected_columns = [] 
    
    st.session_state.builder_aggregation_steps = []
    st.session_state.builder_order_by_steps = []
    
    tables_in_use = {} 
    
    def add_cols(tbl_db, tbl_schema, tbl_name, alias):
        cols_data = get_columns_for_table(tbl_db, tbl_schema, tbl_name)
        for c in cols_data:
            st.session_state.builder_available_columns.append({
                "fq_name": f"{alias}.{c['COLUMN_NAME']}",
                "table_alias": alias,
                "table_name": tbl_name,
                "column": c['COLUMN_NAME'],
                "dtype": c['DATA_TYPE'].upper(),
                "comment": c.get('COMMENT', '') or '' 
            })

    if st.session_state.builder_base_table and st.session_state.builder_base_db:
        base_alias = sanitize_ident(st.session_state.builder_base_table)
        tables_in_use[base_alias] = 1
        add_cols(
            st.session_state.builder_base_db, 
            st.session_state.builder_base_schema, 
            st.session_state.builder_base_table, 
            base_alias
        )

        for step in st.session_state.builder_join_steps:
            if step["right_table"]:
                tbl_info = structured_tables_df[structured_tables_df["TABLE"] == step["right_table"]]
                if not tbl_info.empty:
                    step["db"] = tbl_info.iloc[0]["DB"]
                    step["schema"] = tbl_info.iloc[0]["SCHEMA"]
                    
                    alias = sanitize_ident(step["right_table"])
                    if alias in tables_in_use:
                        tables_in_use[alias] += 1
                        alias = f"{alias}_{tables_in_use[alias]}"
                    else:
                        tables_in_use[alias] = 1
                    add_cols(step["db"], step["schema"], step["right_table"], alias)
                else:
                    st.warning(f"テーブル {step['right_table']} の情報が見つかりません。")

        
        st.session_state.current_step = 2
        st.success(f"{len(st.session_state.builder_available_columns)} 件のカラムを読込みました。")
        st.rerun()
    else:
        st.error("主テーブル/ビューを選択してください。")


# -----------------
# Step 2: 条件指定 (WHERE / SELECT)
# -----------------
if st.session_state.current_step >= 2:
    st.markdown("---")
    st.markdown("#### 条件指定")
    
    if st.session_state.builder_available_columns:
        
        with st.expander("抽出条件", expanded=True):
            all_cols_fq_names = [c["fq_name"] for c in st.session_state.builder_available_columns]
            
            if st.button("＋ 条件を追加"):
                new_id = st.session_state.builder_where_next_id
                st.session_state.builder_where_conditions.append({
                    "id": new_id,
                    "column": "", 
                    "operator": "LIKE",
                    "value": ""
                })
                st.session_state.builder_where_next_id += 1
                st.rerun()

            indices_to_remove = []
            for i, condition in enumerate(st.session_state.builder_where_conditions):
                condition_id = condition["id"]
                c1, c2, c3, c4 = st.columns([3, 2, 3, 1])
                
                with c1:
                    all_cols_with_empty = [""] + all_cols_fq_names
                    try:
                        col_index = all_cols_with_empty.index(condition["column"])
                    except ValueError:
                        col_index = 0
                    condition["column"] = st.selectbox(
                        "カラム", all_cols_with_empty, index=col_index, 
                        key=f"where_col_{condition_id}", label_visibility="collapsed",
                        placeholder="カラムを選択..."
                    )
                
                with c2:
                    operators = ["LIKE", "=", "!=", ">", ">=", "<", "<=", "IS NULL", "IS NOT NULL"]
                    try:
                        op_index = operators.index(condition["operator"])
                    except ValueError:
                        op_index = 0
                    condition["operator"] = st.selectbox(
                        "演算子", operators, index=op_index, 
                        key=f"where_op_{condition_id}", label_visibility="collapsed"
                    )

                is_null_op = condition["operator"] in ["IS NULL", "IS NOT NULL"]
                with c3:
                    condition["value"] = st.text_input(
                        "値", value=condition["value"], key=f"where_val_{condition_id}",
                        disabled=is_null_op, placeholder="値 (IS NULL/NOT NULL は空欄)",
                        label_visibility="collapsed"
                    )

                with c4:
                    if st.button("×", key=f"where_del_{condition_id}", help="この条件を削除"):
                        indices_to_remove.append(i)

            if indices_to_remove:
                for index in sorted(indices_to_remove, reverse=True):
                    st.session_state.builder_where_conditions.pop(index)
                st.rerun()

        
        # --- 表示カラム (SELECT) ---
        with st.expander("抽出対象", expanded=True):
            if st.button("出力する列を選択"):
                st.session_state.show_column_modal = True
                st.session_state.column_modal_search = ""
                st.rerun()

            st.markdown(f"**現在 {len(st.session_state.builder_selected_columns)} 件の列を選択中:**")
            if st.session_state.builder_selected_columns:
                st.markdown('<div class="chip-container">', unsafe_allow_html=True)
                for col_name in st.session_state.builder_selected_columns:
                    st.markdown(f"`{col_name}`")
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("出力する列が選択されていません。")

        
        st.write("") 

        if st.button("次へ", help="条件指定を確定し、集計・並べ替えステップへ進みます", type="primary", use_container_width=True):
            st.session_state.current_step = 3
            st.rerun()
            
    elif st.session_state.current_step == 2:
            st.info("条件を指定し、「次へ」を押してください。")

# -----------------
# Step 3: 集計・並べ替え (GROUP BY / ORDER BY)
# -----------------
if st.session_state.current_step >= 3:
    st.markdown("---")
    st.markdown("#### 集計・並べ替え")

    # --- 集計 (GROUP BY) ---
    with st.expander("集計（オプション）", expanded=False):
        
        st.info("集計関数を1つ以上追加すると、自動的に「抽出対象」の列でグループ化 (GROUP BY) されます。")

        st.markdown("---")
        st.markdown("**集計関数 (COUNT, SUM など)**")
        
        if st.button("＋ 集計を追加"):
            new_id = st.session_state.builder_aggregation_next_id
            st.session_state.builder_aggregation_steps.append({
                "id": new_id,
                "function": "COUNT",
                "column": "*", 
                "alias": f"COUNT_ALL"
            })
            st.session_state.builder_aggregation_next_id += 1
            st.rerun()

        agg_indices_to_remove = []
        all_cols_fq_names = [c["fq_name"] for c in st.session_state.builder_available_columns]
        all_cols_with_wildcard = ["*"] + all_cols_fq_names

        for i, agg_step in enumerate(st.session_state.builder_aggregation_steps):
            agg_id = agg_step["id"]
            c1, c2, c3, c4 = st.columns([2, 3, 3, 1])

            with c1:
                agg_step["function"] = st.selectbox(
                    "関数", AGG_FUNCTIONS, 
                    index=AGG_FUNCTIONS.index(agg_step["function"]),
                    key=f"agg_func_{agg_id}", label_visibility="collapsed"
                )

            with c2:
                options = all_cols_with_wildcard if agg_step["function"] == "COUNT" else all_cols_fq_names
                try:
                    col_index = options.index(agg_step["column"])
                except ValueError:
                    col_index = 0
                agg_step["column"] = st.selectbox(
                    "対象カラム", options, index=col_index,
                    key=f"agg_col_{agg_id}", label_visibility="collapsed"
                )

            with c3:
                default_alias = f"{agg_step['function']}_{agg_step['column']}".replace("*", "ALL")
                if agg_step.get("alias", "") == "" or "COUNT_ALL" in agg_step.get("alias", ""):
                        agg_step["alias"] = default_alias
                        
                agg_step["alias"] = st.text_input(
                    "別名 (AS)", value=agg_step["alias"],
                    key=f"agg_alias_{agg_id}", label_visibility="collapsed"
                )

            with c4:
                if st.button("×", key=f"agg_del_{agg_id}", help="この集計を削除"):
                    agg_indices_to_remove.append(i)

        if agg_indices_to_remove:
            for index in sorted(agg_indices_to_remove, reverse=True):
                st.session_state.builder_aggregation_steps.pop(index)
            st.rerun()

    # --- 並べ替え (ORDER BY) ---
    with st.expander("並べ替え（オプション）", expanded=False):
        
        if st.button("＋ 並べ替え条件を追加"):
            new_id = st.session_state.builder_order_by_next_id
            st.session_state.builder_order_by_steps.append({
                "id": new_id,
                "column": "",
                "direction": "ASC"
            })
            st.session_state.builder_order_by_next_id += 1
            st.rerun()

        order_by_options = st.session_state.builder_selected_columns + [
            step["alias"] for step in st.session_state.builder_aggregation_steps if step["alias"]
        ]
        
        order_indices_to_remove = []
        
        for i, order_step in enumerate(st.session_state.builder_order_by_steps):
            order_id = order_step["id"]
            c1, c2, c3 = st.columns([5, 2, 1])

            with c1:
                options_with_empty = [""] + order_by_options
                try:
                    col_index = options_with_empty.index(order_step["column"])
                except ValueError:
                    col_index = 0
                order_step["column"] = st.selectbox(
                    "対象カラム", options_with_empty, index=col_index,
                    key=f"order_col_{order_id}", label_visibility="collapsed",
                    placeholder="並べ替え対象のカラムを選択..."
                )
            
            with c2:
                order_step["direction"] = st.selectbox(
                    "順序", ["ASC", "DESC"],
                    index=["ASC", "DESC"].index(order_step["direction"]),
                    key=f"order_dir_{order_id}", label_visibility="collapsed"
                )

            with c3:
                if st.button("×", key=f"order_del_{order_id}", help="この並べ替え条件を削除"):
                    order_indices_to_remove.append(i)

        if order_indices_to_remove:
            for index in sorted(order_indices_to_remove, reverse=True):
                st.session_state.builder_order_by_steps.pop(index)
            st.rerun()


    st.write("") 
    
    submitted = st.button("SQL生成", type="primary", use_container_width=True)
    
    if submitted:
        try:
            # -----------------
            # SQL構築 (v12.1)
            # -----------------
            
            # (1) SELECT句 と GROUP BY句
            select_clauses = []
            group_by_clauses = []
            
            group_by_cols = st.session_state.builder_selected_columns 
            agg_steps = st.session_state.builder_aggregation_steps
            
            is_aggregation_enabled = bool(agg_steps)
            
            if is_aggregation_enabled:
                # --- 集計クエリ ---
                for col_name in group_by_cols:
                    col_info = next((c for c in st.session_state.builder_available_columns if c["fq_name"] == col_name), None)
                    if col_info:
                        col_sql = f'"{col_info["table_alias"]}"."{col_info["column"]}"'
                        select_clauses.append(f'{col_sql} AS "{col_info["fq_name"]}"')
                        group_by_clauses.append(col_sql) 
                
                for step in agg_steps:
                    func = step["function"]
                    col_name = step["column"]
                    alias = sanitize_ident(step["alias"])
                    
                    if not alias:
                        raise ValueError("集計関数の別名 (AS) が必要です。")
                    
                    if col_name == "*":
                        if func == "COUNT":
                            select_clauses.append(f'COUNT(*) AS "{alias}"')
                        else:
                            raise ValueError(f"関数 {func} は * (ワイルドカード) では使用できません。")
                    else:
                        col_info = next((c for c in st.session_state.builder_available_columns if c["fq_name"] == col_name), None)
                        if col_info:
                            col_sql = f'"{col_info["table_alias"]}"."{col_info["column"]}"'
                            select_clauses.append(f'{func}({col_sql}) AS "{alias}"')
                        else:
                            raise ValueError(f"集計対象のカラム '{col_name}' が見つかりません。")

                if not select_clauses:
                    if not group_by_clauses:
                         st.error("集計クエリが有効ですが、SELECT対象（グループ化カラムまたは集計関数）がありません。")
                         raise ValueError("SELECT対象がありません。")

            else:
                # --- 通常クエリ (集計なし) ---
                if not st.session_state.builder_selected_columns:
                    st.error("表示カラムを1つ以上選択してください。")
                    raise ValueError("表示カラムがありません。")
                
                for col_name in group_by_cols: 
                        col_info = next((c for c in st.session_state.builder_available_columns if c["fq_name"] == col_name), None)
                        if col_info:
                            # (★修正) IndentationError を修正 (if文の中に入れる)
                            select_clauses.append(f'"{col_info["table_alias"]}"."{col_info["column"]}" AS "{col_info["fq_name"]}"')

            select_sql = f"SELECT {', '.join(select_clauses)}"

            # (2) FROM / JOIN句
            from_sql = ""
            tables_in_use = {}
            
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
            from_sql = f'FROM "{st.session_state.builder_base_db}"."{st.session_state.builder_base_schema}"."{base_table}" AS "{base_alias}"'
            
            join_aliases = {}
            temp_tables_in_use = {base_alias: 1}
            
            for step in reversed(st.session_state.builder_join_steps):
                if step["right_table"]:
                    alias = sanitize_ident(step["right_table"])
                    if alias in temp_tables_in_use:
                        temp_tables_in_use[alias] += 1
                        alias = f"{alias}_{temp_tables_in_use[alias]}"
                    else:
                        temp_tables_in_use[alias] = 1
                    join_aliases[step['id']] = alias 

            current_left_alias = base_alias
            for step in reversed(st.session_state.builder_join_steps):
                if not (step["right_table"] and step["left_key"] and step["right_key"] and len(step["left_key"]) == len(step["right_key"])):
                    raise ValueError("結合ステップの設定が不完全です。")
                if not (step.get("db") and step.get("schema")):
                    raise ValueError(f"結合テーブル {step['right_table']} のDB/スキーマ情報がありません。'次へ'ボタンを再度押してください。")
                
                join_db, join_schema = step["db"], step["schema"]
                right_alias = join_aliases[step['id']] 
                how = step["how"]
                
                on_clauses = [f'"{current_left_alias}"."{lk}" = "{right_alias}"."{rk}"' 
                                for lk, rk in zip(step["left_key"], step["right_key"])]
                on_sql = " AND ".join(on_clauses)
                
                from_sql += f' {how} JOIN "{join_db}"."{join_schema}"."{step["right_table"]}" AS "{right_alias}" ON {on_sql}'
                current_left_alias = right_alias

            # (3) WHERE句
            where_clauses = []
            for condition in st.session_state.builder_where_conditions:
                col_fq_name = condition["column"]
                if not col_fq_name: continue

                operator = condition["operator"]
                value = condition["value"]
                col_info = next((c for c in st.session_state.builder_available_columns if c["fq_name"] == col_fq_name), None)
                if not col_info: continue
                
                col_sql = f'"{col_info["table_alias"]}"."{col_info["column"]}"'
                col_dtype = col_info["dtype"]

                if operator in ["IS NULL", "IS NOT NULL"]:
                    where_clauses.append(f"{col_sql} {operator}")
                    continue
                if not value: continue

                if operator == "LIKE":
                    value_escaped = value.replace("'", "''").replace("%", "\\%").replace("_", "\\_")
                    where_clauses.append(f"{col_sql} LIKE '%{value_escaped}%' ESCAPE '\\'")
                else:
                    is_numeric_type = any(t in col_dtype for t in ["NUMBER", "INT", "FLOAT", "DECIMAL", "DOUBLE"])
                    if is_numeric_type:
                        if re.fullmatch(r"-?\d+(\.\d+)?", value):
                            where_clauses.append(f"{col_sql} {operator} {value}")
                        else:
                            st.warning(f"警告: カラム {col_fq_name} の値 '{value}' は数値として無効なためスキップされました。")
                    else:
                        value_escaped = value.replace("'", "''") 
                        where_clauses.append(f"{col_sql} {operator} '{value_escaped}'")
            
            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            # (4) GROUP BY句 (構築済み)
            group_by_sql = f"GROUP BY {', '.join(group_by_clauses)}" if group_by_clauses else ""
            
            # (5) ORDER BY句
            order_by_clauses = []
            for step in st.session_state.builder_order_by_steps:
                col_name = step["column"]
                direction = step["direction"]
                if col_name:
                    order_by_clauses.append(f'"{col_name}" {direction}')
            
            order_by_sql = f"ORDER BY {', '.join(order_by_clauses)}" if order_by_clauses else ""

            # SQL結合
            final_sql = f"{select_sql} {from_sql} {where_sql} {group_by_sql} {order_by_sql}"
            st.session_state.builder_sql = final_sql
            
            # 実行
            df_preview = run_query(final_sql + " LIMIT 100")
            st.session_state.builder_df_preview = df_preview
            
            st.session_state.current_step = 4
            st.success(f"プレビュー {len(df_preview)} 件を取得しました。")
            st.rerun()

        except Exception as e:
            st.error(f"SQLの構築または実行に失敗しました: {e}")
            st.session_state.builder_sql = ""
            st.session_state.builder_df_preview = pd.DataFrame()
            st.session_state.current_step = 3

    elif st.session_state.current_step == 3:
            st.info("集計・並べ替え条件を指定するか、そのまま「SQL生成」を押してください。")

# -----------------
# Step 4: 結果表示
# -----------------
if st.session_state.current_step == 4:
    st.markdown("---")
    st.markdown("#### 結果")
    
    if st.session_state.builder_sql:
        st.markdown("**実行されたSQL（プレビュー用）**")
        st.code(format_sql(st.session_state.builder_sql) + "\nLIMIT 100", language="sql")
    
    if not st.session_state.builder_df_preview.empty:
        st.dataframe(st.session_state.builder_df_preview.head(50), use_container_width=True)

        with st.container():
            show_count = st.checkbox("件数を計算する（フィルタ後）", value=False)
            if show_count:
                if st.session_state.builder_sql:
                    is_aggregation_enabled_display = bool(st.session_state.builder_aggregation_steps)
                    if is_aggregation_enabled_display:
                        st.warning("集計クエリの件数計算は現在サポートされていません。")
                    else:
                        from_where_group = st.session_state.builder_sql.split("FROM", 1)[1].split("ORDER BY")[0]
                        cnt_sql = "SELECT COUNT(*) AS cnt FROM " + from_where_group
                        try:
                            total = run_query(cnt_sql).iloc[0, 0]
                            st.markdown(f"<span class='badge badge-run'>件数: {total} 件</span>", unsafe_allow_html=True)
                        except Exception as e:
                            st.warning("件数計算に失敗しました。")
                            st.write(e)
                else:
                    st.warning("件数計算の元となるSQLがありません。")

    
    elif st.session_state.builder_available_columns:
        st.info("上記フォームで条件を指定し、「SQL生成」を押してください。")
    else:
        st.info("Step 1 でテーブルを定義し、「次へ」を押してください。")


# -------------------------------------------------
# テーブル選択ダイアログ (st.dialog)
# -------------------------------------------------
if st.session_state.get("show_table_modal", False):
    
    @st.dialog("テーブルを選択", width="large")
    def table_selection_dialog():
        st.session_state.table_modal_search = st.text_input(
            "検索 (テーブル名, スキーマ名, DB名)", 
            value=st.session_state.table_modal_search
        )
        search_term = st.session_state.table_modal_search.lower()

        if search_term:
            filtered_df = structured_tables_df[
                structured_tables_df["TABLE"].str.lower().str.contains(search_term, na=False) |
                structured_tables_df["SCHEMA"].str.lower().str.contains(search_term, na=False) |
                structured_tables_df["DB"].str.lower().str.contains(search_term, na=False)
            ]
        else:
            filtered_df = structured_tables_df

        if filtered_df.empty:
            st.warning("一致するテーブルがありません。")
            return

        with st.container(height=450):
            grouped = filtered_df.groupby(["DB", "SCHEMA"])
            
            for (db, schema), group_df in grouped:
                st.markdown(f"**{db}.{schema}**")
                
                cols = st.columns(3)
                
                for i, table in enumerate(group_df["TABLE"]):
                    if cols[i % 3].button(table, key=f"select_tbl_{db}_{schema}_{table}", use_container_width=True):
                        context = st.session_state.table_selection_context
                        
                        if context == "base":
                            st.session_state.builder_base_table = table
                            st.session_state.builder_base_db = db
                            st.session_state.builder_base_schema = schema
                            st.session_state.builder_join_steps = []
                            st.session_state.builder_available_columns = []
                            st.session_state.builder_selected_columns = [] 
                            st.session_state.current_step = 1
                        
                        elif isinstance(context, int):
                            if context < len(st.session_state.builder_join_steps):
                                st.session_state.builder_join_steps[context]["right_table"] = table
                                st.session_state.builder_join_steps[context]["db"] = db
                                st.session_state.builder_join_steps[context]["schema"] = schema
                            
                        st.session_state.show_table_modal = False
                        st.session_state.table_selection_context = None
                        st.rerun()

    table_selection_dialog()


# -------------------------------------------------
# カラム選択ダイアログ (st.dialog) - 2ペイン構成
# -------------------------------------------------
if st.session_state.get("show_column_modal", False):
    
    @st.dialog("出力する列を選択", width="large")
    def column_selection_dialog():
        
        try:
            all_cols_df = pd.DataFrame(st.session_state.builder_available_columns)
            selected_set = set(st.session_state.builder_selected_columns)
        except Exception as e:
            st.error(f"カラムデータの読み込みに失敗しました: {e}")
            return

        if all_cols_df.empty:
            st.warning("利用可能なカラムがありません。")
            return

        col1, col2 = st.columns(2)

        # -----------------
        # 左カラム (未選択)
        # -----------------
        with col1:
            st.markdown("##### 未選択 (クリックして追加)")
            st.session_state.column_modal_search = st.text_input(
                "検索 (カラム名, コメント)", 
                value=st.session_state.column_modal_search,
                key="col_search_input"
            )
            search_term = st.session_state.column_modal_search.lower()

            unselected_df = all_cols_df[~all_cols_df['fq_name'].isin(selected_set)]

            if search_term:
                filtered_df = unselected_df[
                    unselected_df["fq_name"].str.lower().str.contains(search_term, na=False) |
                    unselected_df["comment"].str.lower().str.contains(search_term, na=False)
                ]
            else:
                filtered_df = unselected_df.copy()

            with st.container(height=400):
                if filtered_df.empty:
                    st.caption("一致するカラムがありません。")
                
                for _, row in filtered_df.iterrows():
                    if st.button(
                        f"＋ {row['fq_name']}", 
                        key=f"add_col_{row['fq_name']}", 
                        use_container_width=True,
                        help=f"{row['comment']} ({row['dtype']})"
                    ):
                        st.session_state.builder_selected_columns.append(row['fq_name'])
                        st.rerun() 

        # -----------------
        # 右カラム (選択済み)
        # -----------------
        with col2:
            st.markdown("##### 選択済み (クリックして削除)")
            
            with st.container(height=450):
                if not st.session_state.builder_selected_columns:
                    st.caption("カラムが選択されていません。")
                
                for col_name in st.session_state.builder_selected_columns:
                    if st.button(
                        f"✕ {col_name}", 
                        key=f"rem_col_{col_name}", 
                        use_container_width=True,
                        type="secondary"
                    ):
                        st.session_state.builder_selected_columns.remove(col_name)
                        st.rerun() 

        st.markdown("---")
        if st.button("閉じる"):
            st.session_state.show_column_modal = False
            st.rerun()

    column_selection_dialog()
