<<<<<<< HEAD
# streamlit_material_filter
Dzinly Streamlit Material Filter
=======
# Dzinly Materials Manager — v4.2 (Client-side aggregation)

This build makes the Admin "Rebuild Indexes" avoid Hostinger `max_statement_time` by scanning
`job_area_materials` in small LIMIT/OFFSET pages and aggregating in Python. Your existing DB tables
are unchanged; only helper tables are created/refreshed.

Run:
```
pip install -r requirements.txt
streamlit run app.py
```
>>>>>>> f844736 (Initial commit)
