# lib/aggregator.py
from __future__ import annotations
import re
from collections import defaultdict
from datetime import datetime
from typing import Iterable, Tuple, Dict

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ---------------------------
# Utilities
# ---------------------------

def _col_exists(engine: Engine, table: str, column: str) -> bool:
    sql = text("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :t
          AND COLUMN_NAME = :c
    """)
    with engine.connect() as conn:
        return conn.execute(sql, {"t": table, "c": column}).scalar_one() > 0


_csv_strip_re = re.compile(r'[\[\]\s"\']')
_csv_keep_digits_commas = re.compile(r'[^0-9,]')

def _explode_csv(value: str) -> Iterable[int]:
    """
    Robustly parse CSV/JSON-like lists:
      "1,2,3" / "[1,2,3]" / '["1","2","3"]' / " 1 , 2 , 3 "
    Returns unique ints (no empty tokens).
    """
    if not value:
        return []
    cleaned = _csv_strip_re.sub("", str(value))
    cleaned = _csv_keep_digits_commas.sub("", cleaned)
    parts = [p for p in cleaned.split(",") if p.isdigit()]
    # preserve order, remove duplicates
    seen, out = set(), []
    for p in parts:
        i = int(p)
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out


def _loop_ranges(start_id: int, end_id: int, step: int) -> Iterable[Tuple[int, int]]:
    a = start_id
    while a <= end_id:
        b = min(a + step - 1, end_id)
        yield a, b
        a = b + 1


def _ensure_tables(engine: Engine, has_pv: bool):
    """Create all van_* helper tables (if not exist) + indexes."""
    stmts = [
        # exploded lists
        """
        CREATE TABLE IF NOT EXISTS van_tpe_materials_extracted (
          elevation_id INT NOT NULL,
          material_id INT NOT NULL,
          modified DATETIME NULL,
          KEY (material_id),
          KEY (elevation_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS van_jobareas_mat (
          material_id INT NOT NULL PRIMARY KEY,
          cnt INT NOT NULL,
          last_dt DATETIME NULL,
          KEY (last_dt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS van_elev_mat (
          material_id INT NOT NULL PRIMARY KEY,
          cnt INT NOT NULL,
          last_dt DATETIME NULL,
          KEY (last_dt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS van_material_usage_summary (
          material_id INT NOT NULL PRIMARY KEY,
          used_job_areas INT NOT NULL,
          used_elevations INT NOT NULL,
          used_project_views INT NOT NULL,
          total_uses INT NOT NULL,
          last_used DATETIME NULL,
          KEY (total_uses),
          KEY (last_used)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS van_unused_materials (
          material_id INT NOT NULL PRIMARY KEY,
          last_used DATETIME NULL,
          snapshot_at DATETIME NOT NULL,
          reason_all_unused TINYINT(1) NOT NULL DEFAULT 1,
          KEY (snapshot_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS van_duplicate_materials (
          key_type VARCHAR(80) NOT NULL,
          group_hash CHAR(32) NOT NULL,
          group_size INT NOT NULL,
          material_id INT NOT NULL,
          snapshot_at DATETIME NOT NULL,
          KEY (key_type, group_hash),
          KEY (group_hash),
          KEY (material_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
    ]
    if has_pv:
        stmts.insert(1, """
        CREATE TABLE IF NOT EXISTS van_pv_materials_extracted (
          project_view_id INT NOT NULL,
          material_id INT NOT NULL,
          modified DATETIME NULL,
          KEY (material_id),
          KEY (project_view_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        stmts.insert(4, """
        CREATE TABLE IF NOT EXISTS van_pv_mat (
          material_id INT NOT NULL PRIMARY KEY,
          cnt INT NOT NULL,
          last_dt DATETIME NULL,
          KEY (last_dt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))


# ---------------------------
# Extract + aggregate builders
# ---------------------------

def refresh_extracted_tables(engine: Engine, step_rows: int = 5000):
    """Explode CSV/JSON id lists from tmp_project_elevations and project_views into van_* tables."""
    has_pv = _col_exists(engine, "project_views", "existing_material_ids")
    _ensure_tables(engine, has_pv)

    # clear extracted tables
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE van_tpe_materials_extracted"))
        if has_pv:
            conn.execute(text("TRUNCATE TABLE van_pv_materials_extracted"))

    # --- tmp_project_elevations
    with engine.connect() as conn:
        minmax = conn.execute(text("""
            SELECT MIN(id), MAX(id)
            FROM tmp_project_elevations
            WHERE existing_material_ids IS NOT NULL
              AND existing_material_ids <> ''
        """)).first()
        
    if minmax and minmax[0] is not None:
        mi, ma = int(minmax[0]), int(minmax[1])
        for a, b in _loop_ranges(mi, ma, step_rows):
            rows = []
            with engine.connect() as conn:
                cur = conn.execute(text("""
                    SELECT id, modified, existing_material_ids
                    FROM tmp_project_elevations
                    WHERE id BETWEEN :a AND :b
                      AND existing_material_ids IS NOT NULL
                      AND existing_material_ids <> ''
                """), {"a": a, "b": b})
                for r in cur:
                    eid = int(r.id)
                    mod = r.modified
                    for mid in _explode_csv(r.existing_material_ids):
                        rows.append({"elevation_id": eid, "material_id": mid, "modified": mod})
            if rows:
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO van_tpe_materials_extracted (elevation_id, material_id, modified)
                        VALUES (:elevation_id, :material_id, :modified)
                    """), rows)

    # --- project_views (optional)
    if has_pv:
        with engine.connect() as conn:
            minmax = conn.execute(text("""
                SELECT MIN(id), MAX(id)
                FROM project_views
                WHERE existing_material_ids IS NOT NULL
                  AND existing_material_ids <> ''
            """)).first()
        if minmax and minmax[0] is not None:
            mi, ma = int(minmax[0]), int(minmax[1])
            for a, b in _loop_ranges(mi, ma, step_rows):
                rows = []
                with engine.connect() as conn:
                    cur = conn.execute(text("""
                        SELECT id, modified, existing_material_ids
                        FROM project_views
                        WHERE id BETWEEN :a AND :b
                          AND existing_material_ids IS NOT NULL
                          AND existing_material_ids <> ''
                    """), {"a": a, "b": b})
                    for r in cur:
                        pvid = int(r.id)
                        mod = r.modified
                        for mid in _explode_csv(r.existing_material_ids):
                            rows.append({"project_view_id": pvid, "material_id": mid, "modified": mod})
                if rows:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO van_pv_materials_extracted (project_view_id, material_id, modified)
                            VALUES (:project_view_id, :material_id, :modified)
                        """), rows)


def _agg_job_areas(engine: Engine, step_rows: int = 5000):
    """
    Client-side aggregation of job_area_materials -> van_jobareas_mat
    using the mapping job_area_materials.material_option_id -> material_options.id -> material_id.
    """
    # Build dictionary {material_id: (count, last_dt)}
    counts: Dict[int, int] = defaultdict(int)
    last_dt: Dict[int, datetime] = {}

    with engine.connect() as conn:
        mm = conn.execute(text("SELECT MIN(id), MAX(id) FROM job_area_materials")).first()
    if not mm or mm[0] is None:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE van_jobareas_mat"))
        return

    mi, ma = int(mm[0]), int(mm[1])
    for a, b in _loop_ranges(mi, ma, step_rows):
        with engine.connect() as conn:
            cur = conn.execute(text("""
                SELECT jam.id, mo.material_id, jam.updated
                FROM job_area_materials jam
                JOIN material_options mo ON mo.id = jam.material_option_id
                WHERE jam.id BETWEEN :a AND :b
                  AND mo.material_id IS NOT NULL
            """), {"a": a, "b": b})
            for r in cur:
                mid = int(r.material_id)
                counts[mid] += 1
                dt = r.updated
                if dt is not None:
                    prev = last_dt.get(mid)
                    if prev is None or dt > prev:
                        last_dt[mid] = dt

    rows = []
    for mid, cnt in counts.items():
        rows.append({"material_id": mid, "cnt": int(cnt), "last_dt": last_dt.get(mid)})
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE van_jobareas_mat"))
        if rows:
            conn.execute(text("""
                INSERT INTO van_jobareas_mat (material_id, cnt, last_dt)
                VALUES (:material_id, :cnt, :last_dt)
            """), rows)


def rebuild_usage_summary(engine: Engine):
    """Aggregate exploded helpers into van_*_mat tables and produce van_material_usage_summary + van_unused_materials."""
    has_pv = _col_exists(engine, "project_views", "existing_material_ids")
    _ensure_tables(engine, has_pv)

    # 1) job areas (client-side)
    _agg_job_areas(engine, step_rows=5000)

    # 2) elev & (optional) pv aggregates
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE van_elev_mat"))
        conn.execute(text("""
            INSERT INTO van_elev_mat (material_id, cnt, last_dt)
            SELECT material_id, COUNT(*), MAX(modified)
            FROM van_tpe_materials_extracted
            GROUP BY material_id
        """))
        if has_pv:
            conn.execute(text("TRUNCATE TABLE van_pv_mat"))
            conn.execute(text("""
                INSERT INTO van_pv_mat (material_id, cnt, last_dt)
                SELECT material_id, COUNT(*), MAX(modified)
                FROM van_pv_materials_extracted
                GROUP BY material_id
            """))

    # 3) usage summary
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE van_material_usage_summary"))
        conn.execute(text(f"""
            INSERT INTO van_material_usage_summary
                (material_id, used_job_areas, used_elevations, used_project_views, total_uses, last_used)
            SELECT
                m.id,
                COALESCE(j.cnt, 0) AS used_job_areas,
                COALESCE(e.cnt, 0) AS used_elevations,
                COALESCE(p.cnt, 0) AS used_project_views,
                COALESCE(j.cnt,0) + COALESCE(e.cnt,0) + COALESCE(p.cnt,0) AS total_uses,
                GREATEST(
                    COALESCE(j.last_dt, '1970-01-01'),
                    COALESCE(e.last_dt, '1970-01-01'),
                    COALESCE(p.last_dt, '1970-01-01'),
                    CAST(m.modified AS DATETIME)
                ) AS last_used
            FROM materials m
            LEFT JOIN van_jobareas_mat j ON j.material_id = m.id
            LEFT JOIN van_elev_mat     e ON e.material_id = m.id
            LEFT JOIN {"van_pv_mat p ON p.material_id = m.id" if _col_exists(engine, "van_pv_mat", "material_id") else "(SELECT NULL) p ON 1=0"}
        """))

    # 4) unused snapshot
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE van_unused_materials"))
        conn.execute(text("""
            INSERT INTO van_unused_materials (material_id, last_used, snapshot_at, reason_all_unused)
            SELECT mus.material_id, mus.last_used, NOW(), 1
            FROM van_material_usage_summary mus
            WHERE mus.total_uses = 0
        """))


# ---------------------------
# Duplicate groups
# ---------------------------

def _dup_insert(engine: Engine, key_type: str, expr_sql: str):
    """
    Build duplicate groups for a given key expression.
    expr_sql must be an SQL expression that references aliased tables:
        m (materials), mb (brands), mbs (styles), mc (categories)
    Example:
        "LOWER(TRIM(m.title))"
        "CONCAT_WS('|', LOWER(TRIM(m.title)), LOWER(TRIM(COALESCE(mb.title,''))))"
    """
    base_join = """
        FROM materials m
        LEFT JOIN material_brands mb ON mb.id = m.material_brand_id
        LEFT JOIN material_brand_styles mbs ON mbs.id = m.material_brand_style_id
        LEFT JOIN material_categories mc ON mc.id = m.material_category_id
    """

    with engine.begin() as conn:
        # Insert group heads with size > 1
        conn.execute(text(f"""
            INSERT INTO van_duplicate_materials (key_type, group_hash, group_size, material_id, snapshot_at)
            SELECT :kt AS key_type, gh.gh AS group_hash, gh.gs AS group_size, m2.id AS material_id, NOW()
            FROM (
                SELECT MD5({expr_sql}) AS gh, COUNT(*) AS gs
                {base_join}
                GROUP BY gh
                HAVING COUNT(*) > 1
            ) gh
            JOIN (
                SELECT m.id, MD5({expr_sql}) AS gh
                {base_join}
            ) m2 ON m2.gh = gh.gh
        """), {"kt": key_type})


def rebuild_duplicates(engine: Engine):
    """Recompute van_duplicate_materials for multiple grouping strategies."""
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE van_duplicate_materials"))

    # key = title
    _dup_insert(engine, "title", "LOWER(TRIM(m.title))")

    # key = title|brand
    _dup_insert(engine, "title_brand",
                "CONCAT_WS('|', LOWER(TRIM(m.title)), LOWER(TRIM(COALESCE(mb.title,''))))")

    # key = title|brand|style
    _dup_insert(engine, "title_brand_style",
                "CONCAT_WS('|', LOWER(TRIM(m.title)), LOWER(TRIM(COALESCE(mb.title,''))), LOWER(TRIM(COALESCE(mbs.title,''))))")

    # key = title|brand|style|category
    _dup_insert(engine, "title_brand_style_category",
                "CONCAT_WS('|', LOWER(TRIM(m.title)), LOWER(TRIM(COALESCE(mb.title,''))), LOWER(TRIM(COALESCE(mbs.title,''))), LOWER(TRIM(COALESCE(mc.title,''))))")


# ---------------------------
# High level entrypoint
# ---------------------------

def get_material_usage_stats(engine: Engine, material_ids: list = None):
    """
    Get optimized usage statistics for specific materials or all materials.
    Returns a dictionary with material_id as key and usage stats as value.
    """
    where_clause = ""
    params = {}
    
    if material_ids:
        placeholders = ",".join([f":id{i}" for i in range(len(material_ids))])
        where_clause = f"WHERE mus.material_id IN ({placeholders})"
        params = {f"id{i}": mid for i, mid in enumerate(material_ids)}
    
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT 
                mus.material_id,
                mus.used_job_areas,
                mus.used_elevations,
                mus.used_project_views,
                mus.total_uses,
                mus.last_used
            FROM van_material_usage_summary mus
            {where_clause}
        """), params).fetchall()
    
    return {
        row[0]: {
            "job_areas": row[1],
            "elevations": row[2], 
            "project_views": row[3],
            "total": row[4],
            "last_used": row[5]
        }
        for row in result
    }


def rebuild_all(engine: Engine):
    """Full rebuild: explode -> aggregates -> usage summary -> unused -> duplicates."""
    has_pv = _col_exists(engine, "project_views", "existing_material_ids")
    _ensure_tables(engine, has_pv)
    refresh_extracted_tables(engine, step_rows=5000)
    rebuild_usage_summary(engine)
    rebuild_duplicates(engine)