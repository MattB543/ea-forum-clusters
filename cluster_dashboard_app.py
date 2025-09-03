"""
Simple interactive dashboard for EA Forum cluster summaries using Streamlit.

Reads:
- cluster_level_summary.csv (one row per level)
- cluster_cluster_summary.csv (one row per cluster per level)

Usage:
  streamlit run cluster_dashboard_app.py

Optionally set env vars to point to CSVs:
- CLUSTER_LEVEL_CSV
- CLUSTER_CLUSTER_CSV
"""

import os
from pathlib import Path
import pandas as pd
import streamlit as st
import altair as alt
try:
    import psycopg2  # psycopg 2.x
    from psycopg2.extras import RealDictCursor
    HAVE_PSYCOPG2 = True
except Exception:  # pragma: no cover
    psycopg2 = None
    RealDictCursor = None
    HAVE_PSYCOPG2 = False

try:
    import psycopg  # psycopg 3.x
    from psycopg.rows import dict_row
    HAVE_PSYCOPG = True
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None
    HAVE_PSYCOPG = False
from dotenv import load_dotenv, find_dotenv


BASE_DIR = Path(__file__).resolve().parent
_level_env = os.getenv("CLUSTER_LEVEL_CSV", "").strip()
_cluster_env = os.getenv("CLUSTER_CLUSTER_CSV", "").strip()
LEVEL_CSV = Path(_level_env) if _level_env else (BASE_DIR / "cluster_level_summary.csv")
CLUSTER_CSV = Path(_cluster_env) if _cluster_env else (BASE_DIR / "cluster_cluster_summary.csv")


@st.cache_data(show_spinner=False)
def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df


def format_float(x, ndigits=2):
    try:
        return f"{float(x):.{ndigits}f}"
    except Exception:
        return "N/A"


def get_database_url() -> str:
    """Resolve DATABASE_URL in a Streamlit-friendly way.

    Priority order:
    1) st.secrets["DATABASE_URL"]
    2) st.secrets["connections"]["postgres" or "pg" or "default"]["url" or "DATABASE_URL"]
    3) environment variable DATABASE_URL
    4) .env file (loaded via python-dotenv)
    """
    # 1) Direct in Streamlit secrets
    try:
        if "DATABASE_URL" in st.secrets:
            val = st.secrets["DATABASE_URL"]
            if val:
                return str(val)
        # 2) Common nested patterns
        if "connections" in st.secrets:
            conns = st.secrets["connections"]
            for key in ("postgres", "pg", "default"):
                if key in conns:
                    cfg = conns[key]
                    # prefer 'url', fallback to 'DATABASE_URL'
                    if "url" in cfg and cfg["url"]:
                        return str(cfg["url"])
                    if "DATABASE_URL" in cfg and cfg["DATABASE_URL"]:
                        return str(cfg["DATABASE_URL"])
    except Exception:
        pass

    # 3) Env var
    val = os.getenv("DATABASE_URL", "").strip()
    if val:
        return val

    # 4) Load from .env (search upwards) then read env again
    try:
        load_dotenv(find_dotenv(), override=False)
        val = os.getenv("DATABASE_URL", "").strip()
        if val:
            return val
    except Exception:
        pass
    return ""


def connect_db():
    url = get_database_url()
    if not url:
        return None
    try:
        if HAVE_PSYCOPG2:
            return psycopg2.connect(url, cursor_factory=RealDictCursor)
        if HAVE_PSYCOPG:
            return psycopg.connect(url, row_factory=dict_row)
        return None
    except Exception:
        return None


def fetch_cluster_posts(level: int, cluster_id: int, sort_by: str = "score") -> pd.DataFrame:
    """Fetch posts for a given cluster level/id from the DB, sorted by score or date.

    Returns an empty DataFrame if DB is not reachable.
    """
    conn = connect_db()
    if conn is None:
        return pd.DataFrame()
    id_col = f"ea_cluster_{int(level)}"
    order = "score DESC NULLS LAST" if sort_by == "score" else "posted_at DESC NULLS LAST"
    sql = f"""
        SELECT
            post_id,
            title,
            author_display_name,
            posted_at,
            base_score,
            score
        FROM fellowship_mvp
        WHERE {id_col} = %s
        ORDER BY {order}
        LIMIT 500
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (int(cluster_id),))
            rows = cur.fetchall()
        conn.close()
        df = pd.DataFrame(rows)
        if not df.empty:
            # Format types and rounding
            if "posted_at" in df.columns:
                try:
                    df["posted_at"] = pd.to_datetime(df["posted_at"]).dt.date
                except Exception:
                    pass
            for c in ["base_score"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").round(0).astype("Int64")
            for c in ["score"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
        return df
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return pd.DataFrame()


def main():
    st.set_page_config(page_title="EA Forum Cluster Dashboard", layout="wide")
    st.title("EA Forum Cluster Dashboard")
    st.caption("Clean, readable view of clusters and score statistics")

    # Load data
    if not LEVEL_CSV.exists() or not CLUSTER_CSV.exists():
        st.error(
            f"CSV files not found. Expected '{LEVEL_CSV}' and '{CLUSTER_CSV}'.\n"
            "Run summarize_cluster_scores.py first to generate them."
        )
        st.stop()

    level_df = load_csv(LEVEL_CSV)
    cluster_df = load_csv(CLUSTER_CSV)

    # No sidebar / controls — show all levels and clusters by default
    available_levels = sorted(cluster_df["level"].dropna().unique().tolist())

    # Top summary (from any one level row; counts are same across levels)
    if not level_df.empty:
        # Overall sums are identical across levels; take the first row
        r = level_df.iloc[0]
        total_posts = int(r.get("post_count", 0))
        meta_posts = int(r.get("meta_posts", 0))
        proper_posts = int(r.get("proper_posts", 0))
        meta_share = (meta_posts / total_posts * 100.0) if total_posts else 0.0
        proper_share = (proper_posts / total_posts * 100.0) if total_posts else 0.0

        # Summary cards
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Posts", f"{total_posts:,}")
        c2.metric("EA Meta", f"{meta_posts:,}", f"{meta_share:.1f}%")
        c3.metric("EA Proper", f"{proper_posts:,}", f"{proper_share:.1f}%")

        # Simple 2-bar comparison for Meta vs Proper
        mp_df = pd.DataFrame({
            "classification": ["EA Meta", "EA Proper"],
            "posts": [meta_posts, proper_posts],
            "share": [meta_share, proper_share],
        })
        bar = (
            alt.Chart(mp_df)
            .mark_bar()
            .encode(
                x=alt.X("classification:N", sort=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y("posts:Q"),
                color=alt.Color("classification:N", legend=None),
                tooltip=["classification:N", "posts:Q", alt.Tooltip("share:Q", format=".1f", title="share %")],
            )
            .properties(height=200)
        )
        st.altair_chart(bar, width="stretch")

        c1, c2 = st.columns(2)
        c1.metric("Avg Base (±Std)", format_float(r.get("avg_base_score")), f"±{format_float(r.get('stddev_base_score'))}")
        c2.metric("Avg Score (±Std)", format_float(r.get("avg_score")), f"±{format_float(r.get('stddev_score'))}")

    # Helper for readable wrapped labels
    def wrap_label(text: str, width: int = 22) -> str:
        if not isinstance(text, str):
            return str(text)
        words = text.split()
        lines = []
        cur = []
        cur_len = 0
        for w in words:
            if cur_len + len(w) + (1 if cur else 0) > width:
                lines.append(" ".join(cur))
                cur = [w]
                cur_len = len(w)
            else:
                cur.append(w)
                cur_len += len(w) + (1 if cur_len else 0)
        if cur:
            lines.append(" ".join(cur))
        return "\n".join(lines)

    # Show all levels; two-card layout per level (Bar chart + Heatmapped table)
    for i, level in enumerate(available_levels):
        if i > 0:
            st.divider()
        st.subheader(f"{int(level)} Clusters")
        df = cluster_df[cluster_df["level"] == level].copy()

        # Ensure names and add wrapped label for chart readability
        df["cluster_name"] = df["cluster_name"].fillna(df["cluster_id"].apply(lambda x: f"Cluster {x}"))
        df["cluster_name_wrapped"] = df["cluster_name"].apply(lambda s: wrap_label(s, width=26))

        # Charts row: two bar charts side by side (Posts left, Avg Base right)
        col_left, col_right = st.columns([1, 1])
        chart_df = df.sort_values(by="post_count", ascending=False).copy()

        with col_left:
            st.markdown("**Posts by Cluster**")
            left_chart = (
                alt.Chart(chart_df)
                .mark_bar(color="#ff7f0e", opacity=0.85)
                .encode(
                    y=alt.Y(
                        "cluster_name_wrapped:N",
                        sort=alt.SortField(field="post_count", order="descending"),
                        axis=alt.Axis(labelAngle=0, labelLimit=800, labelPadding=4, title=None),
                    ),
                    x=alt.X("post_count:Q", title="Posts", axis=alt.Axis(format=",d")),
                    tooltip=[
                        alt.Tooltip("cluster_name:N", title="Cluster"),
                        alt.Tooltip("post_count:Q", title="Posts", format=",d"),
                    ],
                )
                .properties(height={"step": 24})
            )
            st.altair_chart(left_chart, width="stretch")

        with col_right:
            st.markdown("**Avg Base Score by Cluster**")
            right_chart = (
                alt.Chart(chart_df)
                .mark_bar(color="#1f77b4", opacity=0.85)
                .encode(
                    y=alt.Y(
                        "cluster_name_wrapped:N",
                        sort=alt.SortField(field="post_count", order="descending"),
                        axis=alt.Axis(labelAngle=0, labels=False, ticks=False, title=None),
                    ),
                    x=alt.X("avg_base_score:Q", title="Avg Base Score", axis=alt.Axis(format=".0f")),
                    tooltip=[
                        alt.Tooltip("cluster_name:N", title="Cluster"),
                        alt.Tooltip("avg_base_score:Q", title="Avg Base", format=".0f"),
                        alt.Tooltip("stddev_base_score:Q", title="Std Base", format=".0f"),
                    ],
                )
                .properties(height={"step": 24})
            )
            st.altair_chart(right_chart, width="stretch")

        # Second row: full-width table with rounding and native progress coloring
        st.markdown("**Table**")
        with st.container():
            st.markdown("**Table**")
            display_cols = [
                "cluster_name",
                "post_count",
                "avg_base_score",
                "stddev_base_score",
                "avg_score",
                "stddev_score",
            ]
            display_df = df[display_cols].copy()
            # Coerce and round values sensibly
            display_df["avg_base_score"] = pd.to_numeric(display_df["avg_base_score"], errors="coerce").round(0).astype("Int64")
            display_df["stddev_base_score"] = pd.to_numeric(display_df["stddev_base_score"], errors="coerce").round(0).astype("Int64")
            display_df["avg_score"] = pd.to_numeric(display_df["avg_score"], errors="coerce").round(2)
            display_df["stddev_score"] = pd.to_numeric(display_df["stddev_score"], errors="coerce").round(2)
            display_df["post_count"] = pd.to_numeric(display_df["post_count"], errors="coerce").fillna(0).astype(int)

            # Streamlit-native table with progress bars per column (no matplotlib/pandas Styler)
            max_posts = int(display_df["post_count"].max() or 1)
            max_avg_base = float(pd.to_numeric(display_df["avg_base_score"], errors="coerce").max() or 0.0)
            max_std_base = float(pd.to_numeric(display_df["stddev_base_score"], errors="coerce").max() or 0.0)
            max_avg = float(display_df["avg_score"].max() or 0.0)
            max_std = float(display_df["stddev_score"].max() or 0.0)

            st.dataframe(
                display_df,
                width="stretch",
                height=min(900, 80 + 24 * len(display_df)),
                column_config={
                    "cluster_name": st.column_config.TextColumn("Cluster"),
                    "post_count": st.column_config.ProgressColumn(
                        "Posts", format="%d", min_value=0, max_value=max_posts
                    ),
                    "avg_base_score": st.column_config.ProgressColumn(
                        "Avg Base Score", format="%d", min_value=0.0, max_value=max_avg_base
                    ),
                    "stddev_base_score": st.column_config.ProgressColumn(
                        "Std Base", format="%d", min_value=0.0, max_value=max_std_base
                    ),
                    "avg_score": st.column_config.ProgressColumn(
                        "Avg Score", format="%.2f", min_value=0.0, max_value=max_avg
                    ),
                    "stddev_score": st.column_config.ProgressColumn(
                        "Std Score", format="%.2f", min_value=0.0, max_value=max_std
                    ),
                },
            )

        # Quick details drawer for posts in a selected cluster (DB-backed)
        with st.container():
            st.markdown("")
            st.markdown("**View Posts in Cluster**")
            # Build selector (sorted by posts desc)
            if not df.empty and "post_count" in df.columns:
                picker_df = df.sort_values(by="post_count", ascending=False)[["cluster_id", "cluster_name"]].copy()
                select_options = [
                    f"{int(c_id)} — {name}"
                    for c_id, name in zip(picker_df["cluster_id"], picker_df["cluster_name"])
                ]
            else:
                select_options = []
            sel = st.selectbox(
                "Select a cluster",
                options=select_options,
                index=0 if select_options else None,
                key=f"sel_{int(level)}",
            )
            sort_choice = st.radio(
                "Sort posts by",
                options=("score", "date"),
                format_func=lambda x: "Score (desc)" if x == "score" else "Date (newest)",
                horizontal=True,
                key=f"sort_{int(level)}",
            )
            if sel:
                # Parse selection
                cluster_id = int(sel.split(" — ", 1)[0])
                posts_df = fetch_cluster_posts(int(level), cluster_id, sort_by=sort_choice)
                if posts_df.empty:
                    st.info("No posts found or database unavailable. Ensure DATABASE_URL is set if you want this feature.")
                else:
                    st.dataframe(
                        posts_df[[c for c in ["posted_at", "title", "author_display_name", "base_score", "score"] if c in posts_df.columns]],
                        width="stretch",
                        hide_index=True,
                    )

            # Download table for this level
            csv_bytes = display_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                f"Download CSV (Level {int(level)})",
                data=csv_bytes,
                file_name=f"cluster_details_level_{int(level)}.csv",
                mime="text/csv",
            )


if __name__ == "__main__":
    main()
