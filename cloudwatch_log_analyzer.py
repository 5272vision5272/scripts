#!/usr/bin/env python3
"""
CloudWatch Log Group Daily Ingestion Analyzer
==============================================
Collects daily data ingestion (IncomingBytes) for every CloudWatch Log Group
in an AWS account and exports the results to both CSV and Excel files.

Prerequisites:
    pip install boto3 pandas openpyxl

AWS credentials – set as environment variables before running:
    export AWS_ACCESS_KEY_ID="..."
    export AWS_SECRET_ACCESS_KEY="..."
    export AWS_DEFAULT_REGION="ap-south-1"        # change to your region
    # (optional) export AWS_SESSION_TOKEN="..."    # if using temporary creds

Usage:
    python cloudwatch_log_analyzer.py                          # last 7 days, current region
    python cloudwatch_log_analyzer.py --days 30                # last 30 days
    python cloudwatch_log_analyzer.py --days 14 --region us-east-1
    python cloudwatch_log_analyzer.py --days 7 --all-regions   # scan every enabled region
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError


def get_enabled_regions(session: boto3.Session) -> list[str]:
    """Return all regions enabled for this account."""
    ec2 = session.client("ec2", region_name="us-east-1")
    resp = ec2.describe_regions(AllRegions=False)
    return sorted(r["RegionName"] for r in resp["Regions"])


def list_log_groups(logs_client) -> list[dict]:
    """Paginate through all log groups and return their metadata."""
    paginator = logs_client.get_paginator("describe_log_groups")
    groups = []
    for page in paginator.paginate():
        groups.extend(page.get("logGroups", []))
    return groups


def get_daily_ingestion(cw_client, log_group_name: str,
                        start: datetime, end: datetime) -> list[dict]:
    """
    Query the AWS/Logs IncomingBytes metric for a single log group,
    returning one data-point per day (Sum of bytes ingested that day).
    """
    resp = cw_client.get_metric_statistics(
        Namespace="AWS/Logs",
        MetricName="IncomingBytes",
        Dimensions=[{"Name": "LogGroupName", "Value": log_group_name}],
        StartTime=start,
        EndTime=end,
        Period=86400,          # 1 day in seconds
        Statistics=["Sum"],
        Unit="Bytes",
    )
    return resp.get("Datapoints", [])


def bytes_to_human(b: float) -> str:
    """Convert bytes to a concise human-readable string."""
    if b >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    if b >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    if b >= 1 << 10:
        return f"{b / (1 << 10):.2f} KB"
    return f"{b:.0f} B"


def analyze_region(session: boto3.Session, region: str,
                   days: int) -> pd.DataFrame:
    """
    For one region, collect daily IncomingBytes for every log group
    and return a tidy DataFrame.
    """
    logs_client = session.client("logs", region_name=region)
    cw_client = session.client("cloudwatch", region_name=region)

    log_groups = list_log_groups(logs_client)
    if not log_groups:
        print(f"  [{region}] No log groups found.")
        return pd.DataFrame()

    now = datetime.now(timezone.utc)
    end_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=days)

    total = len(log_groups)
    rows = []

    for idx, group in enumerate(log_groups, 1):
        name = group["logGroupName"]
        stored_bytes = group.get("storedBytes", 0)
        retention = group.get("retentionInDays", "Never Expire")

        progress = f"[{idx}/{total}]"
        print(f"  {progress} {name}", end="\r", flush=True)

        datapoints = get_daily_ingestion(cw_client, name, start_time, end_time)

        if not datapoints:
            rows.append({
                "Region": region,
                "LogGroupName": name,
                "Date": None,
                "IncomingBytes": 0,
                "IncomingBytes_MB": 0.0,
                "IncomingBytes_GB": 0.0,
                "IncomingBytes_Human": "0 B",
                "StoredBytes_Total": stored_bytes,
                "StoredBytes_Human": bytes_to_human(stored_bytes),
                "RetentionDays": retention,
            })
            continue

        for dp in datapoints:
            daily_bytes = dp["Sum"]
            rows.append({
                "Region": region,
                "LogGroupName": name,
                "Date": dp["Timestamp"].strftime("%Y-%m-%d"),
                "IncomingBytes": daily_bytes,
                "IncomingBytes_MB": round(daily_bytes / (1 << 20), 4),
                "IncomingBytes_GB": round(daily_bytes / (1 << 30), 6),
                "IncomingBytes_Human": bytes_to_human(daily_bytes),
                "StoredBytes_Total": stored_bytes,
                "StoredBytes_Human": bytes_to_human(stored_bytes),
                "RetentionDays": retention,
            })

    print()  # clear progress line
    return pd.DataFrame(rows)


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a summary: per-log-group average, min, max, and total daily
    ingestion over the analysis window.
    """
    if df.empty or df["Date"].isna().all():
        return pd.DataFrame()

    active = df.dropna(subset=["Date"])
    if active.empty:
        return pd.DataFrame()

    summary = (
        active
        .groupby(["Region", "LogGroupName"], sort=False)
        .agg(
            AvgDaily_MB=("IncomingBytes_MB", "mean"),
            MaxDaily_MB=("IncomingBytes_MB", "max"),
            MinDaily_MB=("IncomingBytes_MB", "min"),
            TotalPeriod_MB=("IncomingBytes_MB", "sum"),
            TotalPeriod_GB=("IncomingBytes_GB", "sum"),
            DaysWithData=("Date", "nunique"),
            StoredBytes_Total=("StoredBytes_Total", "first"),
            StoredBytes_Human=("StoredBytes_Human", "first"),
            RetentionDays=("RetentionDays", "first"),
        )
        .reset_index()
        .sort_values("TotalPeriod_MB", ascending=False)
    )

    summary["AvgDaily_MB"] = summary["AvgDaily_MB"].round(4)
    summary["MaxDaily_MB"] = summary["MaxDaily_MB"].round(4)
    summary["MinDaily_MB"] = summary["MinDaily_MB"].round(4)
    summary["TotalPeriod_MB"] = summary["TotalPeriod_MB"].round(4)
    summary["TotalPeriod_GB"] = summary["TotalPeriod_GB"].round(6)

    return summary


def save_outputs(daily_df: pd.DataFrame, summary_df: pd.DataFrame,
                 output_dir: Path, days: int):
    """Write CSV and Excel outputs."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"cloudwatch_logs_{days}d_{timestamp}"

    csv_daily = output_dir / f"{base}_daily.csv"
    csv_summary = output_dir / f"{base}_summary.csv"
    xlsx_path = output_dir / f"{base}.xlsx"

    daily_sorted = daily_df.sort_values(
        ["Region", "LogGroupName", "Date"]
    ).reset_index(drop=True)
    daily_sorted.to_csv(csv_daily, index=False)

    if not summary_df.empty:
        summary_df.to_csv(csv_summary, index=False)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        daily_sorted.to_excel(writer, sheet_name="Daily Ingestion", index=False)
        if not summary_df.empty:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

        # auto-fit column widths
        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col in ws.columns:
                max_len = max(
                    len(str(cell.value or "")) for cell in col
                )
                header_len = len(str(col[0].value or ""))
                ws.column_dimensions[col[0].column_letter].width = (
                    max(max_len, header_len) + 3
                )

    print(f"\n{'=' * 60}")
    print(f"Output files saved to: {output_dir.resolve()}")
    print(f"  Daily CSV  : {csv_daily.name}")
    if not summary_df.empty:
        print(f"  Summary CSV: {csv_summary.name}")
    print(f"  Excel      : {xlsx_path.name}")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze CloudWatch Log Group daily data ingestion."
    )
    parser.add_argument(
        "--days", type=int, default=7,
        help="Number of past days to analyze (default: 7)."
    )
    parser.add_argument(
        "--region", type=str, default=None,
        help="AWS region (default: from env / boto config)."
    )
    parser.add_argument(
        "--all-regions", action="store_true",
        help="Scan every enabled region in the account."
    )
    parser.add_argument(
        "--output-dir", type=str, default=".",
        help="Directory for output files (default: current directory)."
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        session = boto3.Session(
            region_name=args.region or None
        )
        sts = session.client("sts")
        identity = sts.get_caller_identity()
    except NoCredentialsError:
        print("ERROR: AWS credentials not found.")
        print("Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION.")
        sys.exit(1)
    except ClientError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    account_id = identity["Account"]
    print(f"AWS Account : {account_id}")
    print(f"Analysis    : last {args.days} day(s)")

    if args.all_regions:
        regions = get_enabled_regions(session)
        print(f"Regions     : {len(regions)} enabled regions")
    else:
        region = args.region or session.region_name or "us-east-1"
        regions = [region]
        print(f"Region      : {region}")

    print("-" * 60)

    all_frames = []
    for region in regions:
        print(f"\nScanning region: {region}")
        df = analyze_region(session, region, args.days)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        print("\nNo log groups found in any region. Nothing to export.")
        sys.exit(0)

    daily_df = pd.concat(all_frames, ignore_index=True)
    summary_df = build_summary(daily_df)

    total_groups = daily_df.groupby(
        ["Region", "LogGroupName"]
    ).ngroups
    print(f"\nTotal log groups analyzed: {total_groups}")

    if not summary_df.empty:
        top = summary_df.head(10)
        print(f"\nTop {len(top)} log groups by total ingestion:")
        print("-" * 60)
        for _, row in top.iterrows():
            print(f"  {row['LogGroupName']}")
            print(f"    Region: {row['Region']}  |  "
                  f"Avg: {row['AvgDaily_MB']:.2f} MB/day  |  "
                  f"Total: {row['TotalPeriod_MB']:.2f} MB")

    save_outputs(daily_df, summary_df, output_dir, args.days)


if __name__ == "__main__":
    main()
