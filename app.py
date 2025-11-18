import os
import datetime as dt
import streamlit as st
import psycopg2
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ===== Database connection configuration =====
DB_CONFIG = {
    "postgres": {
        "uri": os.getenv("PG_URI", "postgresql+psycopg2://postgres:password@localhost:5432/gym_db"),
        "schema": os.getenv("PG_SCHEMA", "public")
    },
    "mongo": {
        "uri": os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        "db_name": os.getenv("MONGO_DB", "gym_telemetry")
    }
}

# ===== Page configuration =====
st.set_page_config(page_title="Smart Gym Dashboard", layout="wide")
st.title("📊 Smart Gym Operation Dashboard")


# ===== Database connection functions =====
@st.cache_resource
def get_pg_connection():
    """Create PostgreSQL connection (cached to avoid repeated connections)"""
    try:
        conn = psycopg2.connect(DB_CONFIG["postgres"]["uri"])
        st.success("✅ PostgreSQL database connected successfully")
        return conn
    except Exception as e:
        st.error(f"❌ PostgreSQL connection failed: {str(e)}")
        return None


@st.cache_resource
def get_mongo_client():
    """Create MongoDB connection (for real-time data if needed)"""
    try:
        client = MongoClient(DB_CONFIG["mongo"]["uri"])
        client.admin.command("ping")  # Test connection
        st.success("✅ MongoDB database connected successfully")
        return client
    except Exception as e:
        st.error(f"❌ MongoDB connection failed: {str(e)}")
        return None


# ===== Core query logic (adapted to 500 records) =====
def run_pg_query(conn, query, params=None):
    """Execute PostgreSQL query and return DataFrame"""
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            # Replace schema placeholder {S}
            query = query.replace("{S}", DB_CONFIG["postgres"]["schema"])
            cur.execute(query, params or {})
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        return None


# ===== Dashboard query configuration (role-based) =====
QUERIES = {
    "Admin": [
        # 1. Member statistics (adapted to 150 member records)
        {
            "name": "Member Count by Membership Type",
            "query": """
                SELECT 
                    membership_type,
                    COUNT(*) AS member_count,
                    ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM {S}.member), 1) AS percentage
                FROM {S}.member
                GROUP BY membership_type
                ORDER BY member_count DESC;
            """,
            "chart_type": "bar",
            "x": "membership_type",
            "y": "member_count"
        },
        # 2. Equipment usage rate (adapted to 30 equipment records)
        {
            "name": "Fitness Equipment Status Distribution",
            "query": """
                SELECT 
                    status,
                    COUNT(*) AS equipment_count,
                    location
                FROM {S}.fitness_equipment
                GROUP BY status, location
                ORDER BY location, status;
            """,
            "chart_type": "pie",
            "names": "status",
            "values": "equipment_count"
        }
    ],
    "Trainer": [
        # 1. Personal training statistics (adapted to 100 training records)
        {
            "name": "My Scheduled Sessions (Next 7 Days)",
            "query": """
                SELECT 
                    m.name AS member_name,
                    t.training_date,
                    t.start_time,
                    t.end_time,
                    t.status
                FROM {S}.personal_training t
                JOIN {S}.member m ON t.member_id = m.member_id
                WHERE t.trainer_id = :trainer_id
                  AND t.training_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
                ORDER BY t.training_date, t.start_time;
            """,
            "chart_type": "table",
            "params": {"trainer_id": 1}  # Default query for trainer ID 1
        },
        # 2. Member workout trends (adapted to 500 workout records)
        {
            "name": "Member Average Workout Duration (by Equipment Type)",
            "query": """
                SELECT 
                    e.equipment_type,
                    AVG(w.duration_minutes) AS avg_duration,
                    COUNT(w.record_id) AS record_count
                FROM {S}.workout_record w
                JOIN {S}.fitness_equipment e ON w.equipment_id = e.equipment_id
                WHERE w.member_id = :member_id
                GROUP BY e.equipment_type
                HAVING COUNT(w.record_id) > 0;
            """,
            "chart_type": "bar",
            "x": "equipment_type",
            "y": "avg_duration",
            "params": {"member_id": 10}  # Default query for member ID 10
        }
    ],
    "Member": [
        # 1. Personal workout records (adapted to 500 workout records)
        {
            "name": "My Workout History (Last 30 Days)",
            "query": """
                SELECT 
                    e.equipment_name,
                    w.start_time::date AS workout_date,
                    w.duration_minutes,
                    w.calories_burned,
                    w.heart_rate_avg
                FROM {S}.workout_record w
                JOIN {S}.fitness_equipment e ON w.equipment_id = e.equipment_id
                WHERE w.member_id = :member_id
                  AND w.start_time >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                ORDER BY w.start_time DESC;
            """,
            "chart_type": "line",
            "x": "workout_date",
            "y": "calories_burned",
            "params": {"member_id": 20}  # Default query for member ID 20
        },
        # 2. Personal training history
        {
            "name": "My Personal Training Records",
            "query": """
                SELECT 
                    tr.name AS trainer_name,
                    t.training_date,
                    t.start_time,
                    t.status
                FROM {S}.personal_training t
                JOIN {S}.trainer tr ON t.trainer_id = tr.trainer_id
                WHERE t.member_id = :member_id
                ORDER BY t.training_date DESC;
            """,
            "chart_type": "table",
            "params": {"member_id": 20}
        }
    ]
}


# ===== Sidebar parameter configuration (matching actual data range) =====
with st.sidebar:
    st.header("👤 Role & Parameters")
    role = st.selectbox("Select Role", ["Admin", "Trainer", "Member"])
    
    # Dynamic parameters (show different parameters based on role)
    params = {}
    if role == "Trainer":
        trainer_id = st.number_input("Trainer ID", min_value=1, max_value=10, value=1, step=1)
        params["trainer_id"] = trainer_id
        member_id = st.number_input("Member ID to Query", min_value=1, max_value=150, value=10, step=1)
        params["member_id"] = member_id
    elif role == "Member":
        member_id = st.number_input("My Member ID", min_value=1, max_value=150, value=20, step=1)
        params["member_id"] = member_id
    
    st.divider()
    st.header("⏱️ Time Range")
    time_range = st.slider("Query Data for Last N Days", 7, 90, 30)


# ===== Execute queries and display results =====
def main():
    # Establish database connections
    pg_conn = get_pg_connection()
    # mongo_client = get_mongo_client()  # Enable if MongoDB data is needed

    # Display query results for selected role
    st.subheader(f"📌 {role} Perspective Data")
    for query_info in QUERIES[role]:
        with st.expander(f"View {query_info['name']}"):
            # Execute query
            df = run_pg_query(pg_conn, query_info["query"], query_info.get("params", params))
            if df is None or df.empty:
                st.warning("No data found. Please check if parameter ranges are correct.")
                continue
            
            # Display data and charts
            st.dataframe(df, use_container_width=True)
            if query_info["chart_type"] == "bar":
                st.bar_chart(df, x=query_info["x"], y=query_info["y"])
            elif query_info["chart_type"] == "line":
                st.line_chart(df, x=query_info["x"], y=query_info["y"])
            elif query_info["chart_type"] == "pie":
                st.pie_chart(df, names=query_info["names"], values=query_info["values"])


if __name__ == "__main__":
    main()
