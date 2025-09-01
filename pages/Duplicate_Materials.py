import streamlit as st
import pandas as pd
from sqlalchemy import text
from lib.db import get_engine

st.set_page_config(page_title="Duplicate Materials", page_icon="🔁", layout="wide")
st.title("🔁 Duplicate Materials")

engine = get_engine()

# Fetch duplicate rule types
with engine.connect() as conn:
    key_types = [
        r[0]
        for r in conn.execute(
            text("SELECT DISTINCT key_type FROM van_duplicate_materials ORDER BY key_type")
        ).fetchall()
    ]

if not key_types:
    st.info("No duplicates snapshot found. Run **⚙️ Admin / Rebuild Indexes** first.")
    st.stop()

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    key_type = st.selectbox("Duplicate rule", key_types, index=0)
    page_size = st.selectbox("Per page", [1000, 2500, 5000], index=1)

# Count duplicate groups
with engine.connect() as conn:
    total_groups = int(
        conn.execute(
            text(
                """
        SELECT COUNT(*) FROM (
            SELECT group_hash FROM van_duplicate_materials
            WHERE key_type = :kt
            GROUP BY group_hash
        ) x
        """
            ),
            {"kt": key_type},
        ).scalar_one()
    )

page_count = max((total_groups - 1) // page_size + 1, 1)
page = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
st.caption(f"{total_groups} groups • {page_size} per page")
offset = (page - 1) * page_size

# Fetch current page groups
group_rows_sql = text(
    """
    SELECT group_hash, MAX(group_size) AS group_size
    FROM van_duplicate_materials
    WHERE key_type = :kt
    GROUP BY group_hash
    ORDER BY group_size DESC
    LIMIT :lim OFFSET :off
"""
)
with engine.connect() as conn:
    groups = conn.execute(
        group_rows_sql, {"kt": key_type, "lim": page_size, "off": offset}
    ).fetchall()

# Loop through groups
for gh, gsize in groups:
    with st.expander(f"{key_type} — group of {gsize}"):
        details_sql = text(
            """
            SELECT
              dm.material_id,
              m.photo,
              m.title,
              mc.title  AS category,
              mb.title  AS brand,
              mbs.title AS style,
              COALESCE(mus.total_uses, 0) AS total_uses,
              COALESCE(mus.last_used, m.modified) AS last_used,
              m.created,
              m.status
            FROM van_duplicate_materials dm
            JOIN materials m ON m.id = dm.material_id
            LEFT JOIN material_categories mc   ON mc.id  = m.material_category_id
            LEFT JOIN material_brands mb       ON mb.id  = m.material_brand_id
            LEFT JOIN material_brand_styles mbs ON mbs.id = m.material_brand_style_id
            LEFT JOIN van_material_usage_summary mus ON mus.material_id = m.id
            WHERE dm.key_type = :kt AND dm.group_hash = :gh
            ORDER BY m.title
        """
        )
        with engine.connect() as conn:
            df = pd.read_sql(details_sql, conn, params={"kt": key_type, "gh": gh})

        # Convert relative path to full S3 URL
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
