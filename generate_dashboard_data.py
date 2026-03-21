#!/usr/bin/env python3
"""
Generate JSON data files for the 小红书 dashboard web app.

Usage:
  python generate_dashboard_data.py                  # Use CC Weekly Metrics as data dir
  python generate_dashboard_data.py /path/to/data    # Specify data dir

Outputs:
  dashboard/data/weekly.json   — Weekly metrics (leads, conversion, CPL, CAA)
  dashboard/data/daily.json    — Daily metrics (leads, spend, CPL by day)

Author: Tony's team / auto-generated
"""

import argparse
import json
import os
import sys
import glob
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

# ============================================================
# Configuration (mirrors xiaohongshu_report.py)
# ============================================================
RAW_DATA_PATTERN = "0元分渠道日报*.xlsx"
JIGUANG_DATA_PATTERN = "【*】叽里呱啦的组织-推广数据导出*.xlsx"

# Column names
COL_PERIOD = "运营期"
COL_PURCHASE_DATE = "英语体验课购买日期"
COL_CHANNEL_L3 = "三级渠道"
COL_CHANNEL_L2_INVEST = "投放二级渠道"
COL_TRAFFIC_TYPE = "流量类型"
COL_LEADS = "Leads数"
COL_CONVERT_W1 = "期内正价课用户数（第一周）"
COL_FRIEND = "运营期内加好友人数"
COL_LESSON1 = "运营期内完课大于等于1节人数"

# 聚光 account mapping
KZ_ACCOUNTS = ["聚才（叽里呱啦-1）"]
YY_ACCOUNTS = ["叽里呱啦-hx"]
BRAND_ACCOUNTS = ["叽里呱啦（1）"]

# CAA formula constants
REBATE = 0.05
SALES_COST = 12
UNIT_PRICE = 2000
MARGIN = 0.74
LTV = UNIT_PRICE * MARGIN  # = ¥1,480

MIN_PERIOD = pd.Timestamp("2026-02-02")


def find_latest_file(data_dir, pattern):
    """Find latest file matching a glob pattern."""
    files = glob.glob(str(data_dir / pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def load_internal_data(file_path):
    """Load internal 0元分渠道日报 data and split by sub-channel."""
    df = pd.read_excel(file_path, sheet_name="Sheet1")
    df[COL_PERIOD] = pd.to_datetime(df[COL_PERIOD], errors="coerce")
    df[COL_PURCHASE_DATE] = pd.to_datetime(df[COL_PURCHASE_DATE], errors="coerce")
    df = df.dropna(subset=[COL_PERIOD])

    # 客资收集: 三级渠道 = "达人投放-小红书"
    df_kz = df[df[COL_CHANNEL_L3].astype(str) == "达人投放-小红书"]

    # 应用推广: 流量类型 = "品牌运营" AND 投放二级渠道 contains "小红书"
    df_yy = df[
        (df[COL_TRAFFIC_TYPE].astype(str) == "品牌运营")
        & (df[COL_CHANNEL_L2_INVEST].astype(str).str.contains("小红书", na=False))
    ]

    df_overall = pd.concat([df_kz, df_yy]).drop_duplicates()
    print(f"  内部数据: {len(df):,} 行 → 小红书 {len(df_overall):,} 行 (客资收集 {len(df_kz)}, 应用推广 {len(df_yy)})")
    return df_overall, df_kz, df_yy


def load_jiguang_data(file_path):
    """Load 聚光 ad platform data."""
    df = pd.read_excel(file_path, sheet_name="聚光")
    df["date"] = pd.to_datetime(df["周期"].astype(str), format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"])
    df = df[df["总消耗"] > 0]
    print(f"  聚光数据: {len(df)} 行, 日期 {df['date'].min().strftime('%m/%d')}-{df['date'].max().strftime('%m/%d')}")
    return df


def period_to_spend_window(period_ts):
    """Convert 运营期 to Fri-Thu spend window."""
    end_thu = period_ts - timedelta(days=4)
    start_fri = end_thu - timedelta(days=6)
    return start_fri, end_thu


def compute_weekly_metrics(df):
    """Compute weekly metrics grouped by 运营期."""
    df = df[df[COL_PERIOD] >= MIN_PERIOD]
    grouped = (
        df.groupby(COL_PERIOD)
        .agg({COL_LEADS: "sum", COL_CONVERT_W1: "sum", COL_FRIEND: "sum", COL_LESSON1: "sum"})
        .sort_index()
    )
    grouped["conversion_rate"] = grouped[COL_CONVERT_W1] / grouped[COL_LEADS]
    grouped["friend_rate"] = grouped[COL_FRIEND] / grouped[COL_LEADS]
    grouped["lesson1_rate"] = grouped[COL_LESSON1] / grouped[COL_LEADS]
    return grouped.replace([np.inf, -np.inf], 0).fillna(0)


def compute_cpl_caa(jg_df, metrics_kz, metrics_yy):
    """Compute CPL and CAA per period per sub-channel."""
    results = {}
    all_periods = sorted(set(metrics_kz.index) | set(metrics_yy.index))

    for period in all_periods:
        start_fri, end_thu = period_to_spend_window(period)
        mask = (jg_df["date"] >= start_fri) & (jg_df["date"] <= end_thu)

        kz_spend = jg_df[mask & jg_df["代理商子账户名称"].isin(KZ_ACCOUNTS)]["总消耗"].sum()
        yy_spend = jg_df[mask & jg_df["代理商子账户名称"].isin(YY_ACCOUNTS)]["总消耗"].sum()
        brand_spend = jg_df[mask & jg_df["代理商子账户名称"].isin(BRAND_ACCOUNTS)]["总消耗"].sum()
        other_spend = jg_df[mask & ~jg_df["代理商子账户名称"].isin(
            KZ_ACCOUNTS + YY_ACCOUNTS + BRAND_ACCOUNTS)]["总消耗"].sum()

        total_direct = kz_spend + yy_spend
        if total_direct > 0:
            kz_share = kz_spend / total_direct
            yy_share = yy_spend / total_direct
        else:
            kz_share = yy_share = 0.5

        # Brand spend excluded — used for other goals, not allocated to channels
        kz_full = kz_spend + other_spend * kz_share
        yy_full = yy_spend + other_spend * yy_share

        period_data = {}

        if period in metrics_kz.index:
            leads = int(metrics_kz.loc[period, COL_LEADS])
            rate = float(metrics_kz.loc[period, "conversion_rate"])
            cpl = kz_full / leads if leads > 0 else None
            if cpl is not None and rate > 0:
                cac = (cpl * (1 - REBATE) + SALES_COST) / rate
                caa = LTV - cac
            else:
                cac = caa = None
            period_data["kz"] = {
                "spend": round(kz_full, 2),
                "cpl": round(cpl, 2) if cpl else None,
                "cac": round(cac, 2) if cac else None,
                "caa": round(caa, 2) if caa else None,
            }

        if period in metrics_yy.index:
            leads = int(metrics_yy.loc[period, COL_LEADS])
            rate = float(metrics_yy.loc[period, "conversion_rate"])
            cpl = yy_full / leads if leads > 0 else None
            if cpl is not None and rate > 0:
                cac = (cpl * (1 - REBATE) + SALES_COST) / rate
                caa = LTV - cac
            else:
                cac = caa = None
            period_data["yy"] = {
                "spend": round(yy_full, 2),
                "cpl": round(cpl, 2) if cpl else None,
                "cac": round(cac, 2) if cac else None,
                "caa": round(caa, 2) if caa else None,
            }

        results[period] = period_data

    return results


def compute_daily_spend(jg_df):
    """Compute daily spend per sub-channel from 聚光 data."""
    daily = {}
    for date, group in jg_df.groupby("date"):
        kz_spend = group[group["代理商子账户名称"].isin(KZ_ACCOUNTS)]["总消耗"].sum()
        yy_spend = group[group["代理商子账户名称"].isin(YY_ACCOUNTS)]["总消耗"].sum()
        brand_spend = group[group["代理商子账户名称"].isin(BRAND_ACCOUNTS)]["总消耗"].sum()
        other_spend = group[~group["代理商子账户名称"].isin(
            KZ_ACCOUNTS + YY_ACCOUNTS + BRAND_ACCOUNTS)]["总消耗"].sum()

        total_direct = kz_spend + yy_spend
        if total_direct > 0:
            kz_share = kz_spend / total_direct
            yy_share = yy_spend / total_direct
        else:
            kz_share = yy_share = 0.5

        # Brand spend excluded — used for other goals, not allocated to channels
        daily[date.strftime("%Y-%m-%d")] = {
            "kz_spend": round(kz_spend + other_spend * kz_share, 2),
            "yy_spend": round(yy_spend + other_spend * yy_share, 2),
            "total_spend": round(kz_spend + yy_spend + other_spend, 2),
        }
    return daily


def compute_daily_leads(df_kz, df_yy):
    """Compute daily leads per sub-channel from purchase date."""
    daily = {}

    df_kz_f = df_kz[df_kz[COL_PURCHASE_DATE] >= MIN_PERIOD]
    df_yy_f = df_yy[df_yy[COL_PURCHASE_DATE] >= MIN_PERIOD]
    kz_daily = df_kz_f.dropna(subset=[COL_PURCHASE_DATE]).groupby(COL_PURCHASE_DATE)[COL_LEADS].sum()
    yy_daily = df_yy_f.dropna(subset=[COL_PURCHASE_DATE]).groupby(COL_PURCHASE_DATE)[COL_LEADS].sum()

    all_dates = sorted(set(kz_daily.index) | set(yy_daily.index))
    for d in all_dates:
        kz_l = int(kz_daily.get(d, 0))
        yy_l = int(yy_daily.get(d, 0))
        daily[d.strftime("%Y-%m-%d")] = {
            "kz_leads": kz_l,
            "yy_leads": yy_l,
            "total_leads": kz_l + yy_l,
        }
    return daily


def build_weekly_json(metrics_kz, metrics_yy, metrics_overall, cpl_caa_data):
    """Build the weekly.json structure."""
    today = pd.Timestamp.now().normalize()
    all_periods = sorted(set(metrics_overall.index))

    weeks = []
    for period in all_periods:
        p_str = period.strftime("%Y-%m-%d")
        p_label = period.strftime("%-m/%-d")
        is_complete = period <= today - timedelta(days=7)

        entry = {
            "period": p_str,
            "label": p_label,
            "is_complete": is_complete,
        }

        # Overall
        if period in metrics_overall.index:
            row = metrics_overall.loc[period]
            entry["overall"] = {
                "leads": int(row[COL_LEADS]),
                "conversions": int(row[COL_CONVERT_W1]),
                "conversion_rate": round(float(row["conversion_rate"]) * 100, 2),
            }

        # 客资收集
        if period in metrics_kz.index:
            row = metrics_kz.loc[period]
            kz = {
                "leads": int(row[COL_LEADS]),
                "conversions": int(row[COL_CONVERT_W1]),
                "conversion_rate": round(float(row["conversion_rate"]) * 100, 2),
                "friend_rate": round(float(row["friend_rate"]) * 100, 2),
                "lesson1_rate": round(float(row["lesson1_rate"]) * 100, 2),
            }
            if cpl_caa_data and period in cpl_caa_data and "kz" in cpl_caa_data[period]:
                kz.update(cpl_caa_data[period]["kz"])
            entry["kz"] = kz

        # 应用推广
        if period in metrics_yy.index:
            row = metrics_yy.loc[period]
            yy = {
                "leads": int(row[COL_LEADS]),
                "conversions": int(row[COL_CONVERT_W1]),
                "conversion_rate": round(float(row["conversion_rate"]) * 100, 2),
                "friend_rate": round(float(row["friend_rate"]) * 100, 2),
                "lesson1_rate": round(float(row["lesson1_rate"]) * 100, 2),
            }
            if cpl_caa_data and period in cpl_caa_data and "yy" in cpl_caa_data[period]:
                yy.update(cpl_caa_data[period]["yy"])
            entry["yy"] = yy

        weeks.append(entry)

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "config": {
            "rebate": REBATE,
            "sales_cost": SALES_COST,
            "unit_price": UNIT_PRICE,
            "margin": MARGIN,
            "ltv": LTV,
        },
        "weeks": weeks,
    }


def build_daily_json(daily_leads, daily_spend):
    """Build the daily.json structure."""
    all_dates = sorted(set(list(daily_leads.keys()) + list(daily_spend.keys())))

    days = []
    for d in all_dates:
        entry = {"date": d}
        if d in daily_leads:
            entry.update(daily_leads[d])
        else:
            entry.update({"kz_leads": 0, "yy_leads": 0, "total_leads": 0})
        if d in daily_spend:
            entry.update(daily_spend[d])
        else:
            entry.update({"kz_spend": 0, "yy_spend": 0, "total_spend": 0})

        # Daily CPL (spend / leads)
        kz_l = entry.get("kz_leads", 0)
        yy_l = entry.get("yy_leads", 0)
        entry["kz_cpl"] = round(entry["kz_spend"] / kz_l, 2) if kz_l > 0 else None
        entry["yy_cpl"] = round(entry["yy_spend"] / yy_l, 2) if yy_l > 0 else None
        total_l = kz_l + yy_l
        total_s = entry["kz_spend"] + entry["yy_spend"]
        entry["total_cpl"] = round(total_s / total_l, 2) if total_l > 0 else None

        days.append(entry)

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "days": days,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate dashboard JSON data")
    parser.add_argument("data_dir", nargs="?", default=None)
    parser.add_argument("-o", "--output-dir", default=None)
    args = parser.parse_args()

    script_dir = Path(__file__).parent.resolve()
    data_dir = Path(args.data_dir).resolve() if args.data_dir else script_dir
    out_dir = Path(args.output_dir).resolve() if args.output_dir else (data_dir / "dashboard" / "data")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 50)
    print("  小红书 Dashboard Data Generator")
    print("=" * 50)

    # Load internal data
    raw_file = find_latest_file(data_dir, RAW_DATA_PATTERN)
    if not raw_file:
        print(f"❌ 未找到内部数据: {data_dir / RAW_DATA_PATTERN}")
        sys.exit(1)
    print(f"\n📂 内部数据: {Path(raw_file).name}")
    df_overall, df_kz, df_yy = load_internal_data(raw_file)

    # Load 聚光 data
    jg_file = find_latest_file(data_dir, JIGUANG_DATA_PATTERN)
    jg_df = None
    if jg_file:
        print(f"📂 聚光数据: {Path(jg_file).name}")
        jg_df = load_jiguang_data(jg_file)
    else:
        print("⚠️  未找到聚光数据，跳过 CPL/CAA 计算")

    # Compute weekly metrics
    print("\n📊 计算周维度数据...")
    metrics_overall = compute_weekly_metrics(df_overall)
    metrics_kz = compute_weekly_metrics(df_kz)
    metrics_yy = compute_weekly_metrics(df_yy)

    cpl_caa = None
    if jg_df is not None:
        cpl_caa = compute_cpl_caa(jg_df, metrics_kz, metrics_yy)

    weekly_json = build_weekly_json(metrics_kz, metrics_yy, metrics_overall, cpl_caa)

    # Compute daily metrics
    print("📊 计算日维度数据...")
    daily_leads = compute_daily_leads(df_kz, df_yy)
    daily_spend = compute_daily_spend(jg_df) if jg_df is not None else {}
    daily_json = build_daily_json(daily_leads, daily_spend)

    # Write JSON files
    weekly_path = out_dir / "weekly.json"
    daily_path = out_dir / "daily.json"

    with open(weekly_path, "w", encoding="utf-8") as f:
        json.dump(weekly_json, f, ensure_ascii=False, indent=2)
    print(f"\n💾 {weekly_path} ({len(weekly_json['weeks'])} weeks)")

    with open(daily_path, "w", encoding="utf-8") as f:
        json.dump(daily_json, f, ensure_ascii=False, indent=2)
    print(f"💾 {daily_path} ({len(daily_json['days'])} days)")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
