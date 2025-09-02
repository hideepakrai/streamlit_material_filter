import streamlit as st
import pandas as pd
from sqlalchemy import text
from lib.db import get_engine

st.set_page_config(page_title="Project Views (raw)", page_icon="🧾", layout="wide")
st.title("🧾 Project Views (raw)")
st.markdown("Project View Table Showcase")


engine = get_engine()

with st.sidebar:
    st.header("Filters")
    has_col = True
    with engine.connect() as conn:
        has_col = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME='project_views'
              AND COLUMN_NAME='existing_material_ids'
        """)).scalar_one() > 0
    if not has_col:
        st.info("This database does not have `project_views.existing_material_ids`. Page will show basic columns only.")
    search = st.text_input("Search in existing_material_ids (LIKE)")
    page_size = st.selectbox("Per page", [1000,2500,5000], index=2)

where = ["1=1"]
params = {}
if has_col and search:
    where.append("existing_material_ids LIKE :q")
    params["q"] = f"%{search}%"

with engine.connect() as conn:
    total = int(conn.execute(text(f"SELECT COUNT(*) FROM project_views WHERE {' AND '.join(where)}"), params).scalar_one())

page_count = max((total - 1) // page_size + 1, 1)
page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
st.caption(f"{total} rows • {page_size} per page")
offset = (page - 1) * page_size

cols = "id, project_id, user_id, dp, created, modified"
if has_col:
    cols = "id, project_id, user_id, dp, existing_material_ids, created, modified"

sql = text(f"""
    SELECT {cols}
    FROM project_views
    WHERE {' AND '.join(where)}
    ORDER BY id DESC
    LIMIT :lim OFFSET :off
""")
with engine.connect() as conn:
    df = pd.read_sql(sql, conn, params={**params, "lim": page_size, "off": offset})

# st.dataframe(df, use_container_width=True, hide_index=True)


if "dp" in df.columns:
    df["dp"] = df["dp"].map(
        lambda u: f"https://dzinlyv2.s3.us-east-2.amazonaws.com/liv/{u}"
        if u else ""
    )

# ── Show with thumbnails
st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "dp": st.column_config.ImageColumn("dp", width="small"),
    },
)

