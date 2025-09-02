import streamlit as st
from lib.db import get_engine
from lib.aggregator import rebuild_all

st.set_page_config(page_title="Admin / Rebuild Indexes (Client-side)", page_icon="⚙️", layout="wide")
st.title("⚙️ Admin / Rebuild Indexes (Client-side)")

st.write("This rebuild explodes CSV lists and aggregates usage in pages of **5000** rows to avoid MySQL timeouts.")
if st.button("Rebuild now", type="primary"):
    with st.spinner("Rebuilding (client-side chunking)…"):
        rebuild_all(get_engine())
    st.success("Done! Open the other pages.")