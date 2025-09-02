import streamlit as st
import pandas as pd
from sqlalchemy import text
from lib.db import get_engine

st.set_page_config(page_title="Materials", page_icon="📦", layout="wide")
st.title("📦 Materials")

engine = get_engine()

# ── Filters
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
    status_map = {"All": None, "Active": 1, "Inactive": 0}
    status_label = st.selectbox("Status", list(status_map.keys()))
    
    usage_filter = st.selectbox("Usage Filter", [
        "All Materials", "Used Materials", "Unused Materials",
        "Used in Job Areas", "Used in Elevations", "Used in Project Views"
    ])
    sort_by = st.selectbox("Sort by", [
        "last_used", "total_uses", "job_area_uses", "elevation_uses", 
        "project_view_uses", "title", "created", "modified"
    ])
    sort_dir = st.radio("Direction", ["desc", "asc"], horizontal=True)
    page_size = st.selectbox("Per page", [1000, 2500, 5000], index=0)

where = ["1=1"]
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

s = status_map[status_label]
if s is not None:
    where.append("m.status = :status")
    params["status"] = s

# Usage filter
if usage_filter == "Used Materials":
    where.append("COALESCE(mus.total_uses, 0) > 0")
elif usage_filter == "Unused Materials":
    where.append("COALESCE(mus.total_uses, 0) = 0")
elif usage_filter == "Used in Job Areas":
    where.append("COALESCE(mus.used_job_areas, 0) > 0")
elif usage_filter == "Used in Elevations":
    where.append("COALESCE(mus.used_elevations, 0) > 0")
elif usage_filter == "Used in Project Views":
    where.append("COALESCE(mus.used_project_views, 0) > 0")

order_col = {
    "last_used": "COALESCE(mus.last_used, m.modified)",
    "total_uses": "COALESCE(mus.total_uses, 0)",
    "job_area_uses": "COALESCE(mus.used_job_areas, 0)",
    "elevation_uses": "COALESCE(mus.used_elevations, 0)",
    "project_view_uses": "COALESCE(mus.used_project_views, 0)",
    "title": "m.title",
    "created": "m.created",
    "modified": "m.modified",
}[sort_by]

# counts
with engine.connect() as conn:
    total = int(conn.execute(text(f"""
    SELECT COUNT(DISTINCT m.id)
    FROM materials m
    LEFT JOIN material_categories mc ON mc.id = m.material_category_id
    LEFT JOIN material_brands mb ON mb.id = m.material_brand_id
    LEFT JOIN material_brand_styles mbs ON mbs.id = m.material_brand_style_id
    LEFT JOIN van_material_usage_summary mus ON mus.material_id = m.id
    WHERE {' AND '.join(where)}
    """), params).scalar_one())


page_count = max((total - 1) // page_size + 1, 1)
page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
st.caption(f"{total} items • {page_size} per page")

# Usage statistics summary
with engine.connect() as conn:
    usage_stats = conn.execute(text(f"""
        SELECT 
            COUNT(CASE WHEN mus.used_job_areas > 0 THEN 1 END) as materials_in_job_areas,
            COUNT(CASE WHEN mus.used_elevations > 0 THEN 1 END) as materials_in_elevations,
            COUNT(CASE WHEN mus.used_project_views > 0 THEN 1 END) as materials_in_project_views,
            COUNT(CASE WHEN mus.total_uses > 0 THEN 1 END) as materials_used,
            COUNT(CASE WHEN mus.total_uses = 0 OR mus.total_uses IS NULL THEN 1 END) as materials_unused,
            SUM(COALESCE(mus.used_job_areas, 0)) as total_job_area_uses,
            SUM(COALESCE(mus.used_elevations, 0)) as total_elevation_uses,
            SUM(COALESCE(mus.used_project_views, 0)) as total_project_view_uses
        FROM materials m
        LEFT JOIN material_categories mc ON mc.id = m.material_category_id
        LEFT JOIN material_brands mb ON mb.id = m.material_brand_id
        LEFT JOIN material_brand_styles mbs ON mbs.id = m.material_brand_style_id
        LEFT JOIN van_material_usage_summary mus ON mus.material_id = m.id
        WHERE {' AND '.join(where)}
    """), params).fetchone()

if usage_stats:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Used Materials", f"{usage_stats[3]:,}", f"{usage_stats[4]:,} unused")
    with col2:
        st.metric("Job Area Uses", f"{usage_stats[5]:,}", f"{usage_stats[0]:,} materials")
    with col3:
        st.metric("Elevation Uses", f"{usage_stats[6]:,}", f"{usage_stats[1]:,} materials")
    with col4:
        st.metric("Project View Uses", f"{usage_stats[7]:,}", f"{usage_stats[2]:,} materials")

offset = (page - 1) * page_size

# data
data_sql = text(f"""
    SELECT
      m.id, m.photo, m.title,
      mc.title  AS category,
      mb.title  AS brand,
      mbs.title AS style,
      m.status,
      COALESCE(mus.used_job_areas, 0) AS job_area_uses,
      COALESCE(mus.used_elevations, 0) AS elevation_uses,
      COALESCE(mus.used_project_views, 0) AS project_view_uses,
      COALESCE(mus.total_uses, 0) AS total_uses,
      COALESCE(mus.last_used, m.modified) AS last_used,
      m.created, m.modified
    FROM materials m
    LEFT JOIN material_categories mc   ON mc.id  = m.material_category_id
    LEFT JOIN material_brands mb       ON mb.id  = m.material_brand_id
    LEFT JOIN material_brand_styles mbs ON mbs.id = m.material_brand_style_id
    LEFT JOIN van_material_usage_summary mus ON mus.material_id = m.id
    WHERE {' AND '.join(where)}
    ORDER BY {order_col} {sort_dir.upper()}
    LIMIT :limit OFFSET :offset
""")

with engine.connect() as conn:
    df = pd.read_sql(data_sql, conn, params={**params, "limit": page_size, "offset": offset})
    
print(df.info())

# Add usage indicators
if not df.empty:
    df["usage_indicator"] = df.apply(
        lambda row: "🟢" if row["total_uses"] > 10 
        else "🟡" if row["total_uses"] > 0 
        else "🔴", axis=1
    )
    
    # Reorder columns to put usage indicator first
    cols = ["usage_indicator"] + [col for col in df.columns if col != "usage_indicator"]
    df = df[cols]
    
    
# if "photo" in df.columns:
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
                "usage_indicator": st.column_config.TextColumn(
                    "Usage",
                    help="🟢 High use (>10), 🟡 Some use (1-10), 🔴 Unused",
                    width="small"
                ),
                "photo": st.column_config.ImageColumn("Photo", width="small"),
                "job_area_uses": st.column_config.NumberColumn(
                    "Job Areas", 
                    help="Number of times used in job areas",
                    format="%d"
                ),
                "elevation_uses": st.column_config.NumberColumn(
                    "Elevations", 
                    help="Number of times used in elevations",
                    format="%d"
                ),
                "project_view_uses": st.column_config.NumberColumn(
                    "Project Views", 
                    help="Number of times used in project views",
                    format="%d"
                ),
                "total_uses": st.column_config.NumberColumn(
                    "Total Uses", 
                    help="Total usage across all areas",
                    format="%d"
                ),
                "last_used": st.column_config.DatetimeColumn(
                    "Last Used",
                    format="DD/MM/YYYY HH:mm"
                ),
                "created": st.column_config.DatetimeColumn(
                    "Created",
                    format="DD/MM/YYYY HH:mm"
                ),
                "modified": st.column_config.DatetimeColumn(
                    "Modified", 
                    format="DD/MM/YYYY HH:mm"
                )
            },
)

# if "photo" in df.columns:
#     df["photo"] = df["photo"].map(lambda u: f"[image]({u})" if u else "")

# st.dataframe(df, use_container_width=True, hide_index=True)