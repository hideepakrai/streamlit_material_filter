import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def _cfg():
    try:
        if "mysql" in st.secrets:
            return dict(st.secrets["mysql"])
    except Exception:
        pass
    import os
    return {
        "host": os.getenv("MYSQL_HOST"),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DATABASE"),
    }

@st.cache_resource
def get_engine() -> Engine:
    c = _cfg()
    for k in ("host","user","password","database"):
        if not c.get(k):
            st.error("Missing DB config. Use .streamlit/secrets.toml or env vars.")
            st.stop()
    dsn = f"mysql+pymysql://{c['user']}:{c['password']}@{c['host']}/{c['database']}?charset=utf8mb4"
    return create_engine(dsn, pool_pre_ping=True, pool_recycle=3600)
