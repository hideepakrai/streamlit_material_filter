import streamlit as st
import pandas as pd
from sqlalchemy import text
from lib.db import get_engine

st.set_page_config(page_title="Unused Materials", page_icon="🧹", layout="wide")
st.title("🧹 Unused Materials")

engine = get_engine()

with st.sidebar:
    st.header("Filters")
    q = st.text_input("Search by Title")
    with engine.connect() as conn:
        cats = ["Any"] + [r[0] for r in conn.execute(text("SELECT title FROM material_categories WHERE status=1 ORDER BY title")).fetchall()]
        brands = ["Any"] + [r[0] for r in conn.execute(text("SELECT title FROM material_brands WHERE status=1 ORDER BY title")).fetchall()]
        styles = ["Any"] + [r[0] for r in conn.execute(text("SELECT title FROM material_brand_styles WHERE status=1 ORDER BY title")).fetchall()]
    cat = st.selectbox("Categories", cats)
    brand = st.selectbox("Brands", brands)
    style = st.selectbox("Styles", styles)
    sort_by = st.selectbox("Sort by", ["last_used", "title", "created", "modified"])
    sort_dir = st.radio("Direction", ["desc", "asc"], horizontal=True)
    page_size = st.selectbox("Per page", [1000,2500,5000], index=1)

where = ["um.reason_all_unused = 1"]
params = {}
if q:
    where.append("LOWER(m.title) LIKE :q")
    params["q"] = f"%{q.lower()}%"
if cat and cat != "Any":
    where.append("mc.title = :cat")
    params["cat"] = cat
if brand and brand != "Any":
    where.append("mb.title = :brand")
    params["brand"] = brand
if style and style != "Any":
    where.append("mbs.title = :style")
    params["style"] = style

order_col = {
    "last_used": "COALESCE(um.last_used, m.modified)",
    "title": "m.title",
    "created": "m.created",
    "modified": "m.modified",
}[sort_by]

with engine.connect() as conn:
    total = int(conn.execute(text(f"""
        SELECT COUNT(*)
        FROM van_unused_materials um
        JOIN materials m ON m.id = um.material_id
        LEFT JOIN material_categories mc ON mc.id = m.material_category_id
        LEFT JOIN material_brands mb ON mb.id = m.material_brand_id
        LEFT JOIN material_brand_styles mbs ON mbs.id = m.material_brand_style_id
        WHERE {' AND '.join(where)}
    """), params).scalar_one())

page_count = max((total - 1) // page_size + 1, 1)
page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
st.caption(f"{total} items • {page_size} per page")
offset = (page - 1) * page_size

data_sql = text(f"""
    SELECT
      m.id, m.photo, m.title,
      mc.title  AS category,
      mb.title  AS brand,
      mbs.title AS style,
      m.status,
      COALESCE(um.last_used, m.modified) AS last_used,
      m.created, m.modified
    FROM van_unused_materials um
    JOIN materials m ON m.id = um.material_id
    LEFT JOIN material_categories mc   ON mc.id  = m.material_category_id
    LEFT JOIN material_brands mb       ON mb.id  = m.material_brand_id
    LEFT JOIN material_brand_styles mbs ON mbs.id = m.material_brand_style_id
    WHERE {' AND '.join(where)}
    ORDER BY {order_col} {sort_dir.upper()}
    LIMIT :limit OFFSET :offset
""")
with engine.connect() as conn:
    df = pd.read_sql(data_sql, conn, params={**params, "limit": page_size, "offset": offset})

if "photo" in df.columns:
    df["photo"] = df["photo"].map(lambda u: f"[image]({u})" if u else "")

st.dataframe(df, use_container_width=True, hide_index=True)