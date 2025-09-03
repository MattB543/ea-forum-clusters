"""
Summarize EA Forum cluster statistics by level (5, 12, 30, 60).

Outputs, for each cluster level:
- Cluster name
- Number of posts in cluster
- Average base score
- Std dev base score

Optionally filter or break down by EA classification (e.g., 'EA Proper', 'EA Meta')
if the `ea_classification` column exists in the database.
"""

import os
from typing import Iterable, Optional, Tuple, List, Dict, Any
import csv
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


load_dotenv()


DEFAULT_LEVELS = (5, 12, 30, 60)


def connect_db():
    return psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor)


def column_exists(conn, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (table, column),
        )
        return cur.fetchone() is not None


def classification_values(conn) -> Tuple[bool, Iterable[str]]:
    """Return (exists, values) for ea_classification if present."""
    if not column_exists(conn, "fellowship_mvp", "ea_classification"):
        return False, []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ea_classification
            FROM fellowship_mvp
            WHERE ea_classification IS NOT NULL
            ORDER BY ea_classification
            """
        )
        vals = [row["ea_classification"] for row in cur.fetchall()]
    return True, vals


def print_header(title: str):
    print("=" * 80)
    print(title)
    print("=" * 80)


def summarize_level(
    conn,
    level: int,
    classification_filter: Optional[str] = None,
):
    """Print summary for a given cluster level.

    Args:
        conn: psycopg2 connection
        level: cluster level (e.g., 5, 12, 30, 60)
        classification_filter: optional ea_classification to filter on
    """

    id_col = f"ea_cluster_{level}"
    name_col = f"ea_cluster_{level}_name"

    # Verify columns exist; skip if not present
    if not column_exists(conn, "fellowship_mvp", id_col):
        print(f"[skip] {level}-cluster columns not found (missing {id_col})")
        return

    # name column is optional; we will fallback if missing
    has_name_col = column_exists(conn, "fellowship_mvp", name_col)

    where_clauses = [f"{id_col} IS NOT NULL"]
    params = []

    # Apply classification filter if provided and column exists
    if classification_filter and column_exists(conn, "fellowship_mvp", "ea_classification"):
        where_clauses.append("ea_classification = %s")
        params.append(classification_filter)

    where_sql = " AND ".join(where_clauses)

    # Build query
    # AVG and STDDEV ignore NULLs in Postgres, so this is safe
    select_name = (
        f"COALESCE({name_col}, 'Cluster ' || {id_col}::text) AS cluster_name"
        if has_name_col
        else f"('Cluster ' || {id_col}::text) AS cluster_name"
    )

    query = f"""
        SELECT
            {id_col} AS cluster_id,
            {select_name},
            COUNT(*)::int AS post_count,
            AVG(base_score) AS avg_base_score,
            STDDEV(base_score) AS stddev_base_score
        FROM fellowship_mvp
        WHERE {where_sql}
        GROUP BY {id_col}, cluster_name
        ORDER BY post_count DESC, cluster_id ASC
    """

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    # Output
    subtitle = f"Cluster Level {level}"
    if classification_filter:
        subtitle += f"  |  EA Classification = {classification_filter}"
    print(subtitle)
    print("-" * len(subtitle))
    if not rows:
        print("(no rows)")
        print()
        return

    # Print compact columns
    for row in rows:
        name = row["cluster_name"] or f"Cluster {row['cluster_id']}"
        count = row["post_count"]
        avg = row["avg_base_score"]
        std = row["stddev_base_score"]
        avg_str = f"{float(avg):.2f}" if avg is not None else "N/A"
        std_str = f"{float(std):.2f}" if std is not None else "N/A"
        print(f"- {name}: posts={count}, avg_base={avg_str}, std_base={std_str}")
    print()


def export_level_overview_csv(conn, levels: Iterable[int], out_path: Path):
    """Export per-level summary with Meta/Proper counts and score stats to CSV."""
    rows: List[Dict[str, Any]] = []
    for level in levels:
        id_col = f"ea_cluster_{level}"
        name_col = f"ea_cluster_{level}_name"
        if not column_exists(conn, "fellowship_mvp", id_col):
            continue

        where_sql = f"{id_col} IS NOT NULL"
        query = f"""
            SELECT
                COUNT(*)::int AS post_count,
                SUM(CASE WHEN ea_classification = 'EA_META' THEN 1 ELSE 0 END)::int AS meta_posts,
                SUM(CASE WHEN ea_classification = 'EA_PROPER' THEN 1 ELSE 0 END)::int AS proper_posts,
                AVG(base_score) AS avg_base_score,
                STDDEV(base_score) AS stddev_base_score,
                AVG(score) AS avg_score,
                STDDEV(score) AS stddev_score
            FROM fellowship_mvp
            WHERE {where_sql}
        """
        with conn.cursor() as cur:
            cur.execute(query)
            r = cur.fetchone() or {}
        rows.append({
            "level": level,
            "post_count": r.get("post_count"),
            "meta_posts": r.get("meta_posts"),
            "proper_posts": r.get("proper_posts"),
            "avg_base_score": float(r["avg_base_score"]) if r.get("avg_base_score") is not None else None,
            "stddev_base_score": float(r["stddev_base_score"]) if r.get("stddev_base_score") is not None else None,
            "avg_score": float(r["avg_score"]) if r.get("avg_score") is not None else None,
            "stddev_score": float(r["stddev_score"]) if r.get("stddev_score") is not None else None,
        })

    # Write CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "level", "post_count", "meta_posts", "proper_posts",
        "avg_base_score", "stddev_base_score", "avg_score", "stddev_score"
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def export_cluster_details_csv(conn, levels: Iterable[int], out_path: Path):
    """Export per-cluster summary across levels with Meta/Proper counts and score stats to CSV."""
    all_rows: List[Dict[str, Any]] = []
    for level in levels:
        id_col = f"ea_cluster_{level}"
        name_col = f"ea_cluster_{level}_name"
        if not column_exists(conn, "fellowship_mvp", id_col):
            continue

        has_name_col = column_exists(conn, "fellowship_mvp", name_col)
        select_name = (
            f"COALESCE({name_col}, 'Cluster ' || {id_col}::text) AS cluster_name" if has_name_col
            else f"('Cluster ' || {id_col}::text) AS cluster_name"
        )
        query = f"""
            SELECT
                {id_col} AS cluster_id,
                {select_name},
                COUNT(*)::int AS post_count,
                SUM(CASE WHEN ea_classification = 'EA_META' THEN 1 ELSE 0 END)::int AS meta_posts,
                SUM(CASE WHEN ea_classification = 'EA_PROPER' THEN 1 ELSE 0 END)::int AS proper_posts,
                AVG(base_score) AS avg_base_score,
                STDDEV(base_score) AS stddev_base_score,
                AVG(score) AS avg_score,
                STDDEV(score) AS stddev_score
            FROM fellowship_mvp
            WHERE {id_col} IS NOT NULL
            GROUP BY {id_col}, cluster_name
            ORDER BY post_count DESC, cluster_id ASC
        """
        with conn.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
        for r in res:
            all_rows.append({
                "level": level,
                "cluster_id": r.get("cluster_id"),
                "cluster_name": r.get("cluster_name"),
                "post_count": r.get("post_count"),
                "meta_posts": r.get("meta_posts"),
                "proper_posts": r.get("proper_posts"),
                "avg_base_score": float(r["avg_base_score"]) if r.get("avg_base_score") is not None else None,
                "stddev_base_score": float(r["stddev_base_score"]) if r.get("stddev_base_score") is not None else None,
                "avg_score": float(r["avg_score"]) if r.get("avg_score") is not None else None,
                "stddev_score": float(r["stddev_score"]) if r.get("stddev_score") is not None else None,
            })

    # Write CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "level", "cluster_id", "cluster_name",
        "post_count", "meta_posts", "proper_posts",
        "avg_base_score", "stddev_base_score", "avg_score", "stddev_score"
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in all_rows:
            w.writerow(row)


def main():
    levels_env = os.getenv("CLUSTER_LEVELS", "").strip()
    if levels_env:
        try:
            levels = tuple(int(x) for x in levels_env.split(",") if x.strip())
        except Exception:
            levels = DEFAULT_LEVELS
    else:
        levels = DEFAULT_LEVELS

    classification_env = os.getenv("EA_CLASSIFICATION_FILTER", "").strip()
    classification_filter = classification_env if classification_env else None

    conn = connect_db()

    try:
        # Top-level summary: EA Meta vs Proper
        if column_exists(conn, "fellowship_mvp", "ea_classification"):
            print_header("EA Meta vs Proper Summary (base_score and score)")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        ea_classification,
                        COUNT(*)::int AS post_count,
                        AVG(base_score) AS avg_base_score,
                        STDDEV(base_score) AS stddev_base_score,
                        AVG(score) AS avg_score,
                        STDDEV(score) AS stddev_score
                    FROM fellowship_mvp
                    WHERE ea_classification IN ('EA_META','EA_PROPER')
                    GROUP BY ea_classification
                    ORDER BY ea_classification
                    """
                )
                rows = cur.fetchall()
                # Pretty-print rows
                label_map = {"EA_META": "EA Meta", "EA_PROPER": "EA Proper"}
                for row in rows:
                    label = label_map.get(row["ea_classification"], str(row["ea_classification"]))
                    c = row["post_count"]
                    ab = row["avg_base_score"]
                    sb = row["stddev_base_score"]
                    as_ = row["avg_score"]
                    ss = row["stddev_score"]
                    ab_s = f"{float(ab):.2f}" if ab is not None else "N/A"
                    sb_s = f"{float(sb):.2f}" if sb is not None else "N/A"
                    as_s = f"{float(as_):.2f}" if as_ is not None else "N/A"
                    ss_s = f"{float(ss):.2f}" if ss is not None else "N/A"
                    print(
                        f"- {label}: posts={c}, "
                        f"avg_base={ab_s}, std_base={sb_s}, "
                        f"avg_score={as_s}, std_score={ss_s}"
                    )
                print()

        print_header("EA Forum Cluster Score Summary")

        # Primary summaries (optionally filtered by a single classification)
        for level in levels:
            summarize_level(conn, level, classification_filter)

        # CSV exports (per-level and per-cluster across all levels)
        out_dir_env = os.getenv("CLUSTER_SUMMARY_CSV_DIR", "").strip()
        out_dir = Path(out_dir_env).resolve() if out_dir_env else Path(__file__).resolve().parent
        level_csv = out_dir / "cluster_level_summary.csv"
        cluster_csv = out_dir / "cluster_cluster_summary.csv"
        export_level_overview_csv(conn, levels, level_csv)
        export_cluster_details_csv(conn, levels, cluster_csv)
        print(f"CSV exported: {level_csv}")
        print(f"CSV exported: {cluster_csv}\n")

        # Optional breakdowns per classification, if column is present and no explicit filter was set
        has_class, values = classification_values(conn)
        if has_class and not classification_filter:
            print_header("Breakdown by EA Classification")
            for cls in values:
                for level in levels:
                    summarize_level(conn, level, classification_filter=cls)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
