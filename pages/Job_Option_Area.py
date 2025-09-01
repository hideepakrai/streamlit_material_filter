import streamlit as st
import pandas as pd
from sqlalchemy import text
from lib.db import get_engine

st.set_page_config(page_title="Job Area Materials - Usage", page_icon="🎨", layout="wide")
st.title("🎨 Job Area Materials - Usage")

engine = get_engine()

# ── Filters
with st.sidebar:
    st.header("Filters")
    search = st.text_input("Search material_pid (LIKE)")  # matches integer id as string
    page_size = st.selectbox("Per page", [25, 50, 100, 200, 500], index=2)

# ── Base query
base_sql = """
    SELECT 
        m.photo,
        m.title,
        mo.material_pid,
        COUNT(*) AS count,
        GROUP_CONCAT(mo.id ORDER BY mo.id) AS job_ids
    FROM job_area_materials mo
    JOIN materials m
        ON m.id = mo.material_pid
    WHERE mo.material_pid IS NOT NULL
"""

where = []
params = {}
if search:
    where.append("CAST(mo.material_pid AS CHAR) LIKE :q")
    params["q"] = f"%{search}%"

if where:
    base_sql += " AND " + " AND ".join(where)

group_order_sql = """
    GROUP BY mo.material_pid
    ORDER BY count DESC
"""

# ── Get total rows after grouping
count_sql = f"SELECT COUNT(*) FROM ({base_sql} {group_order_sql}) sub"

with engine.connect() as conn:
    total = int(conn.execute(text(count_sql), params).scalar_one())

page_count = max((total - 1) // page_size + 1, 1)
page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
st.caption(f"{total} unique materials • {page_size} per page")
offset = (page - 1) * page_size

# ── Final query with pagination
final_sql = f"""
    {base_sql}
    {group_order_sql}
    LIMIT :lim OFFSET :off
"""

with engine.connect() as conn:
    df = pd.read_sql(
        text(final_sql),
        conn,
        params={**params, "lim": page_size, "off": offset}
    )

# ── Add full photo URL
if "photo" in df.columns:
    df["photo"] = df["photo"].map(
        lambda u: f"https://dzinlyv2.s3.us-east-2.amazonaws.com/liv/materials/{u}"
        if u else ""
    )

# ── Show with thumbnails
st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "photo": st.column_config.ImageColumn("Photo", width="small"),
        "material_pid": st.column_config.TextColumn("Material ID"),
        "count": st.column_config.NumberColumn("Usage Count"),
        "job_ids": st.column_config.TextColumn("Job IDs"),
    },
)
