import streamlit as st
import pandas as pd
from sqlalchemy import text
from lib.db import get_engine

st.set_page_config(page_title="Job Area Materials (raw)", page_icon="📄", layout="wide")
st.title("📄 Job Area Materials (raw)")
st.markdown("Tables used = job_area_materials + materials_option + materials")

engine = get_engine()

with st.sidebar:
    st.header("Filters")
    opt_id = st.text_input("Filter by material_option_id (exact)")
    mat_id = st.text_input("Filter by material_id (via join)")
    page_size = st.selectbox("Per page", [1000,2500,5000], index=2)

where = ["1=1"]
params = {}
if opt_id.strip():
    where.append("jam.material_option_id = :moid")
    params["moid"] = int(opt_id)
if mat_id.strip():
    where.append("mo.material_id = :mid")
    params["mid"] = int(mat_id)

with engine.connect() as conn:
    total = int(conn.execute(text(f"""
        SELECT COUNT(*)
        FROM job_area_materials jam
        JOIN material_options mo ON mo.id = jam.material_option_id
        WHERE {' AND '.join(where)}
    """), params).scalar_one())

page_count = max((total - 1) // page_size + 1, 1)
page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
st.caption(f"{total} rows • {page_size} per page")
offset = (page - 1) * page_size


sql = text(f"""
    SELECT
      jam.id, jam.material_option_id, jam.created, jam.updated,
      mo.material_id,
      m.title AS material_title
    FROM job_area_materials jam
    JOIN material_options mo ON mo.id = jam.material_option_id
    LEFT JOIN materials m ON m.id = mo.material_id
    WHERE {' AND '.join(where)}
    ORDER BY jam.id DESC
    LIMIT :lim OFFSET :off
""")


with engine.connect() as conn:
    df = pd.read_sql(sql, conn, params={**params, "lim": page_size, "off": offset})

st.dataframe(df, use_container_width=True, hide_index=True)
