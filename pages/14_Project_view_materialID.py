import streamlit as st
import pandas as pd
from sqlalchemy import text
from lib.db import get_engine

st.set_page_config(page_title="Project Views - Material Usage", page_icon="📐", layout="wide")
st.title("📐 Project Views - Material Usage")
st.markdown("Project View with Materials id listing in an comma seprated project_view_ids")


engine = get_engine()

# ── Filters
with st.sidebar:
    st.header("Filters")
    search = st.text_input("Search material_id (LIKE)")
    page_size = st.selectbox("Per page", [25, 50, 100, 200, 500], index=2)

# ── Base query
base_sql = """
    SELECT
        m.photo, 
        jt.material_id,
        COUNT(*) AS count,
        GROUP_CONCAT(DISTINCT pv.id ORDER BY pv.id) AS project_view_ids
    FROM project_views pv
    JOIN JSON_TABLE(
        pv.existing_material_ids,
        '$[*]' COLUMNS (material_id VARCHAR(50) PATH '$')
    ) jt
    LEFT JOIN materials m
    on jt.material_id=m.id
    WHERE pv.existing_material_ids IS NOT NULL
      AND JSON_VALID(pv.existing_material_ids)
"""

where = []
params = {}
if search:
    where.append("jt.material_id LIKE :q")
    params["q"] = f"%{search}%"

if where:
    base_sql += " AND " + " AND ".join(where)

group_order_sql = """
    GROUP BY jt.material_id
    ORDER BY count DESC
"""

# ── Get total count of rows after grouping
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


if "photo" in df.columns:
            df["photo"] = df["photo"].map(
                lambda u: f"https://dzinlyv2.s3.us-east-2.amazonaws.com/liv/materials/{u}"
                if u else ""
)

        # Show with thumbnails in dataframe
st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "photo": st.column_config.ImageColumn("Photo", width="small")
            },
)


st.markdown("""SELECT
        m.photo, 
        jt.material_id,
        COUNT(*) AS count,
        GROUP_CONCAT(DISTINCT pv.id ORDER BY pv.id) AS project_view_ids
    FROM project_views pv
    JOIN JSON_TABLE(
        pv.existing_material_ids,
        '$[*]' COLUMNS (material_id VARCHAR(50) PATH '$')
    ) jt
    LEFT JOIN materials m
    on jt.material_id=m.id
    WHERE pv.existing_material_ids IS NOT NULL
      AND JSON_VALID(pv.existing_material_ids)""")