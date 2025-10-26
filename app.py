import os
import datetime as dt
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

from sqlalchemy import create_engine, text
from pymongo import MongoClient

load_dotenv()

#Postgres schema helper
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")   # CHANGE: "public" to your own schema name
def qualify(sql: str) -> str:
    # Replace occurrences of {S}.<table> with <schema>.<table>
    return sql.replace("{S}.", f"{PG_SCHEMA}.")

# CONFIG: Postgres and Mongo Queries
    # 修改PostgreSQL查询部分（仅展示调整的关键查询）
CONFIG = {
    "postgres": {
        "enabled": True,
        "uri": os.getenv("PG_URI", "postgresql+psycopg2://postgres:password@localhost:5432/eldercare_db"),
        "queries": {
            # 医生角色：调整患者ID范围（假设实际数据doctor_id为1-5）
            "Doctor: patients under my care (table)": {
                "sql": """
                    SELECT p.patient_id, p.name AS patient, p.age, p.room_no
                    FROM {S}.patients p
                    WHERE p.doctor_id = :doctor_id 
                      AND p.patient_id <= 100  # 限制在500条数据的患者ID范围内
                    ORDER BY p.name;
                """,
                "chart": {"type": "table"},
                "tags": ["doctor"],
                "params": ["doctor_id"]
            },
            
            # 护士角色：调整今日任务查询（假设数据中treatment_time有近7天记录）
            "Nurse: today’s tasks (treatments to administer) (table)": {
                "sql": """
                    SELECT p.name AS patient, t.treatment_type, t.treatment_time
                    FROM {S}.treatments t
                    JOIN {S}.patients p ON t.patient_id = p.patient_id
                    WHERE t.nurse_id = :nurse_id
                      AND t.treatment_time::date BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE  # 扩大时间范围避免空结果
                    ORDER BY t.treatment_time;
                """,
                "chart": {"type": "table"},
                "tags": ["nurse"],
                "params": ["nurse_id"]
            },
            
            # 药剂师角色：调整药品库存阈值（适配500条数据中的库存规模）
            "Pharmacist: medicines to reorder (bar)": {
                "sql": """
                    SELECT m.name, m.quantity
                    FROM {S}.medicine_stock m
                    WHERE m.quantity < :reorder_threshold  # 假设实际库存多为10-50，阈值设为15
                    ORDER BY m.quantity ASC;
                """,
                "chart": {"type": "bar", "x": "name", "y": "quantity"},
                "tags": ["pharmacist"],
                "params": ["reorder_threshold"]
            },
            
            # 管理者角色：简化患者统计（适配小数据量）
            "Mgr: total patients & average age (table)": {
                "sql": """
                    SELECT COUNT(*)::int AS total_patients, 
                           AVG(age)::numeric(10,1) AS avg_age,
                           MIN(age) AS min_age,  # 新增小数据量下更有意义的统计
                           MAX(age) AS max_age
                    FROM {S}.patients;
                """,
                "chart": {"type": "table"},
                "tags": ["manager"]
            }
            # 其他查询保持结构，根据实际表字段名微调（如实际字段名不同需同步修改）
        }
    
    # ... 其他配置保持不变
},


    "mongo": {
    "enabled": True,
    "uri": os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    "db_name": os.getenv("MONGO_DB", "eldercare_telemetry"),
    "queries": {
        # 调整心率查询的时间范围（适配小数据量）
        "TS: Hourly avg heart rate (resident 501, last 24h)": {
            "collection": "bracelet_readings_ts",
            "aggregate": [
                {"$match": {
                    "meta.resident_id": 501,  # 假设500条数据中存在resident_id=501的记录
                    "ts": {"$gte": dt.datetime.utcnow() - dt.timedelta(hours=48)}  # 扩大时间范围
                }},
                {"$project": {
                    "hour": {"$dateTrunc": {"date": "$ts", "unit": "hour"}},
                    "hr": "$heart_rate_bpm"
                }},
                {"$group": {"_id": "$hour", "avg_hr": {"$avg": "$hr"}, "n": {"$count": {}}}},
                {"$sort": {"_id": 1}}
            ],
            "chart": {"type": "line", "x": "_id", "y": "avg_hr"}
        },
        
        # 调整设备电池状态查询（适配少量设备数据）
        "Telemetry: Battery status distribution": {
            "collection": "bracelet_data",
            "aggregate": [
                {"$project": {
                    "battery": {"$ifNull": ["$battery_pct", None]},
                    "bucket": {
                        "$switch": {
                            "branches": [
                                {"case": {"$gte": ["$battery_pct", 60]}, "then": "60+"},  # 简化分桶（数据量少）
                                {"case": {"$gte": ["$battery_pct", 30]}, "then": "30-59"},
                            ],
                            "default": "<30 or null"
                        }
                    }
                }},
                {"$group": {"_id": "$bucket", "cnt": {"$count": {}}}},
                {"$sort": {"cnt": -1}}
            ],
            "chart": {"type": "pie", "names": "_id", "values": "cnt"}
        }
        # 其他Mongo查询类似调整
    }
}

}

# The following block of code will create a simple Streamlit dashboard page
st.set_page_config(page_title="Old-Age Home DB Dashboard", layout="wide")
st.title("Old-Age Home | Mini Dashboard (Postgres + MongoDB)")

def metric_row(metrics: dict):
    cols = st.columns(len(metrics))
    for (k, v), c in zip(metrics.items(), cols):
        c.metric(k, v)

@st.cache_resource
def get_pg_engine(uri: str):
    return create_engine(uri, pool_pre_ping=True, future=True)

@st.cache_data(ttl=60)
def run_pg_query(_engine, sql: str, params: dict | None = None):
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

@st.cache_resource
def get_mongo_client(uri: str):
    return MongoClient(uri)

def mongo_overview(client: MongoClient, db_name: str):
    info = client.server_info()
    db = client[db_name]
    colls = db.list_collection_names()
    stats = db.command("dbstats")
    total_docs = sum(db[c].estimated_document_count() for c in colls) if colls else 0
    return {
        "DB": db_name,
        "Collections": f"{len(colls):,}",
        "Total docs (est.)": f"{total_docs:,}",
        "Storage": f"{round(stats.get('storageSize',0)/1024/1024,1)} MB",
        "Version": info.get("version", "unknown")
    }

@st.cache_data(ttl=60)
def run_mongo_aggregate(_client, db_name: str, coll: str, stages: list):
    db = _client[db_name]
    docs = list(db[coll].aggregate(stages, allowDiskUse=True))
    return pd.json_normalize(docs) if docs else pd.DataFrame()

def render_chart(df: pd.DataFrame, spec: dict):
    if df.empty:
        st.info("No rows.")
        return
    ctype = spec.get("type", "table")
    # light datetime parsing for x axes
    for c in df.columns:
        if df[c].dtype == "object":
            try:
                df[c] = pd.to_datetime(df[c])
            except Exception:
                pass

    if ctype == "table":
        st.dataframe(df, use_container_width=True)
    elif ctype == "line":
        st.plotly_chart(px.line(df, x=spec["x"], y=spec["y"]), use_container_width=True)
    elif ctype == "bar":
        st.plotly_chart(px.bar(df, x=spec["x"], y=spec["y"]), use_container_width=True)
    elif ctype == "pie":
        st.plotly_chart(px.pie(df, names=spec["names"], values=spec["values"]), use_container_width=True)
    elif ctype == "heatmap":
        pivot = pd.pivot_table(df, index=spec["rows"], columns=spec["cols"], values=spec["values"], aggfunc="mean")
        st.plotly_chart(px.imshow(pivot, aspect="auto", origin="upper",
                                  labels=dict(x=spec["cols"], y=spec["rows"], color=spec["values"])),
                        use_container_width=True)
    elif ctype == "treemap":
        st.plotly_chart(px.treemap(df, path=spec["path"], values=spec["values"]), use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)

# The following block of code is for the dashboard sidebar, where you can pick your users, provide parameters, etc.
with st.sidebar:
    st.header("Connections")
    # These fields are pre-filled from .env file
    pg_uri = st.text_input("Postgres URI", CONFIG["postgres"]["uri"])     
    mongo_uri = st.text_input("Mongo URI", CONFIG["mongo"]["uri"])        
    mongo_db = st.text_input("Mongo DB name", CONFIG["mongo"]["db_name"]) 
    st.divider()
    auto_run = st.checkbox("Auto-run on selection change", value=False, key="auto_run_global")

    st.header("Role & Parameters")
    role = st.selectbox("User role", ["doctor","nurse","pharmacist","guardian","manager","all"], index=5)
    doctor_id = st.number_input("doctor_id", min_value=1, value=2, step=1)  # 假设实际医生ID为1-5
    nurse_id = st.number_input("nurse_id", min_value=1, value=3, step=1)    # 假设实际护士ID为1-10
    patient_name = st.text_input("patient_name", value="John Doe")  # 改为实际存在的患者姓名
    age_threshold = st.number_input("age_threshold", min_value=60, value=75, step=1)  # 适配老年患者年龄
    days = st.slider("last N days", 1, 30, 5)  # 缩小时间范围（数据量少）
    med_low_threshold = st.number_input("med_low_threshold", min_value=0, value=3, step=1)  # 适配小库存
    reorder_threshold = st.number_input("reorder_threshold", min_value=0, value=8, step=1)
    PARAMS_CTX = {
        "doctor_id": int(doctor_id),
        "nurse_id": int(nurse_id),
        "patient_name": patient_name,
        "age_threshold": int(age_threshold),
        "days": int(days),
        "med_low_threshold": int(med_low_threshold),
        "reorder_threshold": int(reorder_threshold),
    }

#Postgres part of the dashboard
st.subheader("Postgres")
try:
    
    eng = get_pg_engine(pg_uri)

    with st.expander("Run Postgres query", expanded=True):
        # The following will filter queries by role
        def filter_queries_by_role(qdict: dict, role: str) -> dict:
            def ok(tags):
                t = [s.lower() for s in (tags or ["all"])]
                return "all" in t or role.lower() in t
            return {name: q for name, q in qdict.items() if ok(q.get("tags"))}

        pg_all = CONFIG["postgres"]["queries"]
        pg_q = filter_queries_by_role(pg_all, role)

        names = list(pg_q.keys()) or ["(no queries for this role)"]
        sel = st.selectbox("Choose a saved query", names, key="pg_sel")

        if sel in pg_q:
            q = pg_q[sel]
            sql = qualify(q["sql"])   
            st.code(sql, language="sql")

            run  = auto_run or st.button("▶ Run Postgres", key="pg_run")
            if run:
                wanted = q.get("params", [])
                params = {k: PARAMS_CTX[k] for k in wanted}
                df = run_pg_query(eng, sql, params=params)
                render_chart(df, q["chart"])
        else:
            st.info("No Postgres queries tagged for this role.")
except Exception as e:
    st.error(f"Postgres error: {e}")

# Mongo panel
if CONFIG["mongo"]["enabled"]:
    st.subheader("🍃 MongoDB")
    try:
        mongo_client = get_mongo_client(mongo_uri)   
        metric_row(mongo_overview(mongo_client, mongo_db))

        with st.expander("Run Mongo aggregation", expanded=True):
            mongo_query_names = list(CONFIG["mongo"]["queries"].keys())
            selm = st.selectbox("Choose a saved aggregation", mongo_query_names, key="mongo_sel")
            q = CONFIG["mongo"]["queries"][selm]
            st.write(f"**Collection:** `{q['collection']}`")
            st.code(str(q["aggregate"]), language="python")
            runm = auto_run or st.button("▶ Run Mongo", key="mongo_run")
            if runm:
                dfm = run_mongo_aggregate(mongo_client, mongo_db, q["collection"], q["aggregate"])
                render_chart(dfm, q["chart"])
    except Exception as e:
        st.error(f"Mongo error: {e}")
