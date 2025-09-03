EA Forum Cluster Dashboard
==========================

This folder contains a small pipeline and a Streamlit app to summarize and explore EA Forum clusters.

Contents
- `summarize_cluster_scores.py`: connects to your database and generates CSV summaries.
- `cluster_level_summary.csv`: one row per cluster level (5, 12, 30, 60) with Meta/Proper counts and score stats.
- `cluster_cluster_summary.csv`: one row per cluster per level with Meta/Proper counts and score stats.
- `cluster_dashboard_app.py`: Streamlit dashboard to browse and chart the data.

Prerequisites
- Environment variable `DATABASE_URL` pointing to your Postgres instance.
- Python 3.9+ and the packages in `requirements.txt` (includes `streamlit`).

Generate CSVs
- From repo root: `python cluster_dashboard/summarize_cluster_scores.py`
  - By default, CSVs are written into this folder.
  - Optional: `CLUSTER_LEVELS=5,12` to limit levels; `EA_CLASSIFICATION_FILTER=EA_META` to filter console summary.

Run the Dashboard
- Install dependencies: `pip install -r requirements.txt`
- Start Streamlit: `streamlit run cluster_dashboard/cluster_dashboard_app.py`
  - Optional: override CSV paths via env vars `CLUSTER_LEVEL_CSV` and `CLUSTER_CLUSTER_CSV`.

Notes
- The app shows totals and breakdowns (Meta vs Proper) and provides bar charts and a detailed table with CSV download.
- If you change the CSV generation settings, rerun `summarize_cluster_scores.py` to refresh the data.

