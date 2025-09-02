import streamlit as st
import pandas as pd
from sqlalchemy import text
from lib.db import get_engine

st.set_page_config(page_title="Tmp Project Elevations (raw)", page_icon="🏗️", layout="wide")
st.title("🏗️ Tmp Project Elevations (raw)")

engine = get_engine()

with st.sidebar:
    st.header("Filters")
    search = st.text_input("Search in existing_material_ids (LIKE)")
    page_size = st.selectbox("Per page", [25, 50, 100, 200, 500], index=2)

where = ["1=1"]
params = {}
if search:
    where.append("existing_material_ids LIKE :q")
    params["q"] = f"%{search}%"

with engine.connect() as conn:
    total = int(conn.execute(text(f"SELECT COUNT(*) FROM tmp_project_elevations WHERE {' AND '.join(where)}"), params).scalar_one())

page_count = max((total - 1) // page_size + 1, 1)
page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
st.caption(f"{total} rows • {page_size} per page")
offset = (page - 1) * page_size

sql = text(f"""
    SELECT id, existing_material_ids, created, modified
    FROM tmp_project_elevations
    WHERE {' AND '.join(where)}
    ORDER BY id DESC
    LIMIT :lim OFFSET :off
""")
with engine.connect() as conn:
    df = pd.read_sql(sql, conn, params={**params, "lim": page_size, "off": offset})

st.dataframe(df, use_container_width=True, hide_index=True)
