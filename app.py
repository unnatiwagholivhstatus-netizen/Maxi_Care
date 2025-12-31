"""
Unnati Motors Maxi Care Dashboard - Render Ready (app.py)

What is added/changed for Render:
1) Reads Excel paths from environment variables (LABOUR_FILE, SPARES_FILE).
   - Default: files in same folder as app.py (Render path: /opt/render/project/src)
2) If Excel not found, app will still run but APIs will return empty results.
3) Keeps your original logic and UI; only path loading is made Render-safe.
4) Export sheets included:
   - Labour export: Complete Details, Month-wise Summary, Division Summary, Labour Description
   - Spares export: Complete Details, Month-wise Summary, Division Summary, Part Desc Wise

Note:
- Keep "Maxi Labour.xlsx" and "Maxi Spares.xlsx" in repo root (same folder as app.py) OR set env vars.
- Python 3.12.10 compatible.
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import sys
import io
import os
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent

# Render-friendly file paths (set in render.yaml)
LABOUR_FILE = Path(os.getenv("LABOUR_FILE", str(BASE_DIR / "Maxi Labour.xlsx")))
SPARES_FILE = Path(os.getenv("SPARES_FILE", str(BASE_DIR / "Maxi Spares.xlsx")))

# Local Windows fallbacks (only if still not found)
if not LABOUR_FILE.exists():
    win_fallback = Path("D:/Maxi Care Dashboard/Maxi Labour.xlsx")
    if win_fallback.exists():
        LABOUR_FILE = win_fallback

if not SPARES_FILE.exists():
    win_fallback = Path("D:/Maxi Care Dashboard/Maxi Spares.xlsx")
    if win_fallback.exists():
        SPARES_FILE = win_fallback

APP_NAME = "Unnati Motors Maxi Care Dashboard"
APP_PORT = int(os.getenv("PORT", "8000"))  # Render provides PORT

MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
}

FINANCIAL_YEAR_ORDER = ["Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"]

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_data():
    try:
        print(f"Loading Labour file: {LABOUR_FILE}")
        if LABOUR_FILE.exists():
            labour_df_local = pd.read_excel(LABOUR_FILE)
            labour_df_local["Bill Date"] = pd.to_datetime(labour_df_local.get("Bill Date"), errors="coerce")
            print(f"   Loaded {len(labour_df_local)} labour records")
        else:
            print("   Labour file not found. Running with empty labour data.")
            labour_df_local = pd.DataFrame()

        print(f"Loading Spares file: {SPARES_FILE}")
        if SPARES_FILE.exists():
            spares_df_local = pd.read_excel(SPARES_FILE)
            spares_df_local["Doc Date"] = pd.to_datetime(spares_df_local.get("Doc Date"), errors="coerce")
            print(f"   Loaded {len(spares_df_local)} spares records")
        else:
            print("   Spares file not found. Running with empty spares data.")
            spares_df_local = pd.DataFrame()

        return labour_df_local, spares_df_local
    except Exception as e:
        print(f"ERROR loading files: {e}")
        return pd.DataFrame(), pd.DataFrame()


labour_df, spares_df = load_data()

DIVISION_MAPPING = {
    "A": "HO", "B": "AMT", "C": "CITY", "D": "YAT", "G": "WAG",
    "L": "SHI", "M": "CHA", "R": "KOL", "U": "CHI"
}


def get_division_from_ro(ro_no):
    try:
        s = str(ro_no)
        if len(s) >= 5:
            letter = s[4].upper()
            return DIVISION_MAPPING.get(letter, "Unknown")
    except Exception:
        pass
    return "Unknown"


def get_month_name(date):
    try:
        if pd.notna(date):
            return MONTH_NAMES.get(date.month, "")
    except Exception:
        pass
    return ""


def get_labour_divisions():
    if labour_df.empty or "RO No." not in labour_df.columns:
        return []
    divisions = set()
    for ro_no in labour_df["RO No."].dropna().unique():
        div = get_division_from_ro(str(ro_no))
        if div != "Unknown":
            divisions.add(div)
    return sorted(list(divisions))


def get_spares_divisions():
    if spares_df.empty or "RO Number" not in spares_df.columns:
        return []
    divisions = set()
    for ro_no in spares_df["RO Number"].dropna().unique():
        div = get_division_from_ro(str(ro_no))
        if div != "Unknown":
            divisions.add(div)
    return sorted(list(divisions))


def get_labour_months_for_division(division):
    if labour_df.empty or "RO No." not in labour_df.columns or "Bill Date" not in labour_df.columns:
        return []
    if not division:
        months = set()
        for d in labour_df["Bill Date"].dropna().unique():
            m = get_month_name(d)
            if m:
                months.add(m)
        return [m for m in FINANCIAL_YEAR_ORDER if m in months]

    filtered = labour_df[labour_df["RO No."].apply(lambda x: get_division_from_ro(str(x)) == division)]
    months = set()
    for date in filtered["Bill Date"].dropna().unique():
        month = get_month_name(date)
        if month:
            months.add(month)
    return [m for m in FINANCIAL_YEAR_ORDER if m in months]


def get_spares_months_for_division(division):
    if spares_df.empty or "RO Number" not in spares_df.columns or "Doc Date" not in spares_df.columns:
        return []
    if not division:
        months = set()
        for d in spares_df["Doc Date"].dropna().unique():
            m = get_month_name(d)
            if m:
                months.add(m)
        return [m for m in FINANCIAL_YEAR_ORDER if m in months]

    filtered = spares_df[spares_df["RO Number"].apply(lambda x: get_division_from_ro(str(x)) == division)]
    months = set()
    for date in filtered["Doc Date"].dropna().unique():
        month = get_month_name(date)
        if month:
            months.add(month)
    return [m for m in FINANCIAL_YEAR_ORDER if m in months]


def get_labour_advisors_any(division=None, month=None):
    if labour_df.empty or "Service Advisor" not in labour_df.columns:
        return []
    filtered = labour_df.copy()

    if division and "RO No." in filtered.columns:
        filtered = filtered[filtered["RO No."].apply(lambda x: get_division_from_ro(str(x)) == division)]
    if month and "Bill Date" in filtered.columns:
        filtered = filtered[filtered["Bill Date"].apply(lambda x: get_month_name(x) == month)]

    advisors = filtered["Service Advisor"].dropna().unique().tolist()
    advisors = [str(a).strip() for a in advisors if str(a).strip()]
    return sorted(list(set(advisors)))


def get_spares_advisors_any(division=None, month=None):
    if spares_df.empty or "Service Advisor" not in spares_df.columns:
        return []
    filtered = spares_df.copy()

    if division and "RO Number" in filtered.columns:
        filtered = filtered[filtered["RO Number"].apply(lambda x: get_division_from_ro(str(x)) == division)]
    if month and "Doc Date" in filtered.columns:
        filtered = filtered[filtered["Doc Date"].apply(lambda x: get_month_name(x) == month)]

    advisors = filtered["Service Advisor"].dropna().unique().tolist()
    advisors = [str(a).strip() for a in advisors if str(a).strip()]
    return sorted(list(set(advisors)))


def get_labour_summary(division=None, month=None, advisor=None):
    if labour_df.empty:
        return {"total_items": 0, "total_dis": 0, "total_amount": 0}

    filtered = labour_df.copy()

    if division and "RO No." in filtered.columns:
        filtered = filtered[filtered["RO No."].apply(lambda x: get_division_from_ro(str(x)) == division)]
    if month and "Bill Date" in filtered.columns:
        filtered = filtered[filtered["Bill Date"].apply(lambda x: get_month_name(x) == month)]
    if advisor and "Service Advisor" in filtered.columns:
        filtered = filtered[filtered["Service Advisor"].apply(lambda x: str(x).strip() == advisor)]

    if filtered.empty:
        return {"total_items": 0, "total_dis": 0, "total_amount": 0}

    dis_col = "Labour Basic Amount-DIS"
    tot_col = "Labour Total Amount"

    total_dis = float(filtered[dis_col].sum()) if dis_col in filtered.columns else 0.0
    total_amount = float(filtered[tot_col].sum()) if tot_col in filtered.columns else 0.0

    return {
        "total_items": int(len(filtered)),
        "total_dis": total_dis,
        "total_amount": total_amount
    }


def get_spares_summary(division=None, month=None, advisor=None):
    if spares_df.empty:
        return {"total_ndp": 0, "total_selling": 0, "total_mrp": 0}

    filtered = spares_df.copy()

    if division and "RO Number" in filtered.columns:
        filtered = filtered[filtered["RO Number"].apply(lambda x: get_division_from_ro(str(x)) == division)]
    if month and "Doc Date" in filtered.columns:
        filtered = filtered[filtered["Doc Date"].apply(lambda x: get_month_name(x) == month)]
    if advisor and "Service Advisor" in filtered.columns:
        filtered = filtered[filtered["Service Advisor"].apply(lambda x: str(x).strip() == advisor)]

    if filtered.empty:
        return {"total_ndp": 0, "total_selling": 0, "total_mrp": 0}

    ndp_col = "NDP PRIC*Qty"
    sell_col = "Selling Price/Landed Cost (Total of Issued Qty)"
    qty_col = "Final Qty"
    mrp_col = "MRP (Per Qty)"

    total_ndp = float(filtered[ndp_col].sum()) if ndp_col in filtered.columns else 0.0
    total_selling = float(filtered[sell_col].sum()) if sell_col in filtered.columns else 0.0

    if qty_col in filtered.columns and mrp_col in filtered.columns:
        total_mrp = float((filtered[qty_col] * filtered[mrp_col]).sum())
    else:
        total_mrp = 0.0

    return {
        "total_ndp": total_ndp,
        "total_selling": total_selling,
        "total_mrp": total_mrp
    }


def get_labour_data(division=None, month=None, advisor=None):
    if labour_df.empty:
        return []

    filtered = labour_df.copy()

    if division and "RO No." in filtered.columns:
        filtered = filtered[filtered["RO No."].apply(lambda x: get_division_from_ro(str(x)) == division)]
    if month and "Bill Date" in filtered.columns:
        filtered = filtered[filtered["Bill Date"].apply(lambda x: get_month_name(x) == month)]
    if advisor and "Service Advisor" in filtered.columns:
        filtered = filtered[filtered["Service Advisor"].apply(lambda x: str(x).strip() == advisor)]

    if filtered.empty:
        return []

    try:
        if "Labour Description" not in filtered.columns:
            return []

        dis_col = "Labour Basic Amount-DIS"
        tot_col = "Labour Total Amount"

        agg_map = {}
        if dis_col in filtered.columns:
            agg_map[dis_col] = "sum"
        if tot_col in filtered.columns:
            agg_map[tot_col] = "sum"
        if "RO No." in filtered.columns:
            agg_map["RO No."] = "count"
        else:
            filtered["_rowcount_"] = 1
            agg_map["_rowcount_"] = "sum"

        grouped = filtered.groupby("Labour Description", as_index=False).agg(agg_map)

        if "RO No." in grouped.columns:
            grouped = grouped.rename(columns={"RO No.": "count"})
        else:
            grouped = grouped.rename(columns={"_rowcount_": "count"})

        if dis_col not in grouped.columns:
            grouped[dis_col] = 0.0
        if tot_col not in grouped.columns:
            grouped[tot_col] = 0.0

        result = []
        for _, row in grouped.iterrows():
            result.append({
                "Labour Description": str(row["Labour Description"]),
                "count": int(row["count"]),
                "Labour Basic Amount-DIS": float(row[dis_col]),
                "Labour Total Amount": float(row[tot_col])
            })
        return result
    except Exception as e:
        print(f"Error processing labour data: {e}")
        return []


def get_spares_data(division=None, month=None, advisor=None):
    if spares_df.empty:
        return []

    filtered = spares_df.copy()

    if division and "RO Number" in filtered.columns:
        filtered = filtered[filtered["RO Number"].apply(lambda x: get_division_from_ro(str(x)) == division)]
    if month and "Doc Date" in filtered.columns:
        filtered = filtered[filtered["Doc Date"].apply(lambda x: get_month_name(x) == month)]
    if advisor and "Service Advisor" in filtered.columns:
        filtered = filtered[filtered["Service Advisor"].apply(lambda x: str(x).strip() == advisor)]

    if filtered.empty:
        return []

    try:
        if "Part Desc" not in filtered.columns:
            return []

        qty_col = "Final Qty"
        ndp_col = "NDP PRIC*Qty"
        sell_col = "Selling Price/Landed Cost (Total of Issued Qty)"
        mrp_col = "MRP (Per Qty)"

        agg_map = {}
        if qty_col in filtered.columns:
            agg_map[qty_col] = "sum"
        if ndp_col in filtered.columns:
            agg_map[ndp_col] = "sum"
        if sell_col in filtered.columns:
            agg_map[sell_col] = "sum"
        if mrp_col in filtered.columns:
            agg_map[mrp_col] = "mean"

        grouped = filtered.groupby("Part Desc", as_index=False).agg(agg_map)

        if qty_col not in grouped.columns:
            grouped[qty_col] = 0.0
        if ndp_col not in grouped.columns:
            grouped[ndp_col] = 0.0
        if sell_col not in grouped.columns:
            grouped[sell_col] = 0.0
        if mrp_col not in grouped.columns:
            grouped[mrp_col] = 0.0

        grouped["MRP_Total"] = grouped[qty_col] * grouped[mrp_col]

        result = []
        for _, row in grouped.iterrows():
            result.append({
                "part_desc": str(row["Part Desc"]),
                "final_qty": float(row[qty_col]),
                "ndp_price_qty": float(row[ndp_col]),
                "selling_price_total": float(row[sell_col]),
                "mrp_total": float(row["MRP_Total"])
            })
        return result
    except Exception as e:
        print(f"Error processing spares data: {e}")
        return []


# -------------------- APIs --------------------

@app.get("/api/labour/divisions")
def api_labour_divisions():
    return {"divisions": get_labour_divisions()}


@app.get("/api/spares/divisions")
def api_spares_divisions():
    return {"divisions": get_spares_divisions()}


@app.get("/api/labour/months")
def api_labour_months_all():
    return {"months": get_labour_months_for_division(None)}


@app.get("/api/labour/months/{division}")
def api_labour_months_div(division: str):
    return {"months": get_labour_months_for_division(division)}


@app.get("/api/spares/months")
def api_spares_months_all():
    return {"months": get_spares_months_for_division(None)}


@app.get("/api/spares/months/{division}")
def api_spares_months_div(division: str):
    return {"months": get_spares_months_for_division(division)}


@app.get("/api/labour/advisors")
def api_labour_advisors(division: str = None, month: str = None):
    return {"advisors": get_labour_advisors_any(division, month)}


@app.get("/api/labour/advisors/{division}/{month}")
def api_labour_advisors_old(division: str, month: str):
    return {"advisors": get_labour_advisors_any(division, month)}


@app.get("/api/spares/advisors")
def api_spares_advisors(division: str = None, month: str = None):
    return {"advisors": get_spares_advisors_any(division, month)}


@app.get("/api/spares/advisors/{division}/{month}")
def api_spares_advisors_old(division: str, month: str):
    return {"advisors": get_spares_advisors_any(division, month)}


@app.get("/api/labour/summary")
def api_labour_summary(division: str = None, month: str = None, advisor: str = None):
    return get_labour_summary(division if division else None, month if month else None, advisor if advisor else None)


@app.get("/api/labour/data")
def api_labour_data(division: str = None, month: str = None, advisor: str = None):
    data = get_labour_data(division if division else None, month if month else None, advisor if advisor else None)
    return {"rows": data}


@app.get("/api/spares/summary")
def api_spares_summary(division: str = None, month: str = None, advisor: str = None):
    return get_spares_summary(division if division else None, month if month else None, advisor if advisor else None)


@app.get("/api/spares/data")
def api_spares_data(division: str = None, month: str = None, advisor: str = None):
    data = get_spares_data(division if division else None, month if month else None, advisor if advisor else None)
    return {"rows": data}


@app.get("/api/labour/export")
def export_labour_data(division: str = None, month: str = None, advisor: str = None):
    try:
        filtered = labour_df.copy()

        if not filtered.empty:
            if division and "RO No." in filtered.columns:
                filtered = filtered[filtered["RO No."].apply(lambda x: get_division_from_ro(str(x)) == division)]
            if month and "Bill Date" in filtered.columns:
                filtered = filtered[filtered["Bill Date"].apply(lambda x: get_month_name(x) == month)]
            if advisor and "Service Advisor" in filtered.columns:
                filtered = filtered[filtered["Service Advisor"].apply(lambda x: str(x).strip() == advisor)]

        if filtered.empty:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                pd.DataFrame().to_excel(writer, sheet_name="Complete Details", index=False)
                pd.DataFrame().to_excel(writer, sheet_name="Month-wise Summary", index=False)
                pd.DataFrame().to_excel(writer, sheet_name="Division Summary", index=False)
                pd.DataFrame().to_excel(writer, sheet_name="Labour Description", index=False)
            output.seek(0)

            filename = f"Labour_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        required_cols = [
            "RO No.", "RO Date", "RO Status", "Registration No", "Chassis No.", "Vehicle Model",
            "Sale Date", "Customer Name", "Kilometer", "Service Advisor", "Service Type",
            "Sublet Code", "Labour in/Out", "Sublet Amount", "Labour Code", "Labour Description",
            "STD Hours", "Techncn Name 1", "Techncn Name 2", "Techncn Name 3",
            "Labour Basic Amount", "Discount Amount", "Labour Basic Amount-DIS",
            "Tax Amount", "Labour Total Amount", "Billable Type", "Bill Date"
        ]
        present_cols = [c for c in required_cols if c in filtered.columns]
        details_df = filtered[present_cols].copy() if present_cols else filtered.copy()

        month_summary = []
        total_count = 0
        total_without_tax = 0.0
        total_with_tax = 0.0

        dis_col = "Labour Basic Amount-DIS"
        tot_col = "Labour Total Amount"

        for m in FINANCIAL_YEAR_ORDER:
            if "Bill Date" not in filtered.columns:
                break
            m_filtered = filtered[filtered["Bill Date"].apply(lambda x: get_month_name(x) == m)]
            if not m_filtered.empty:
                count = int(len(m_filtered))
                without_tax = float(m_filtered[dis_col].sum()) if dis_col in m_filtered.columns else 0.0
                with_tax = float(m_filtered[tot_col].sum()) if tot_col in m_filtered.columns else 0.0

                month_summary.append({"Month": m, "Count": count, "Without Tax": without_tax, "With Tax": with_tax})
                total_count += count
                total_without_tax += without_tax
                total_with_tax += with_tax

        if month_summary:
            month_summary.append({
                "Month": "Grand Total",
                "Count": total_count,
                "Without Tax": total_without_tax,
                "With Tax": total_with_tax
            })

        month_summary_df = pd.DataFrame(month_summary)

        div_summary = []
        total_div_count = 0
        total_div_without_tax = 0.0
        total_div_with_tax = 0.0

        for div in get_labour_divisions():
            df_div = labour_df.copy()
            if not df_div.empty and "RO No." in df_div.columns:
                df_div = df_div[df_div["RO No."].apply(lambda x: get_division_from_ro(str(x)) == div)]

            count = int(len(df_div))
            without_tax = float(df_div[dis_col].sum()) if (not df_div.empty and dis_col in df_div.columns) else 0.0
            with_tax = float(df_div[tot_col].sum()) if (not df_div.empty and tot_col in df_div.columns) else 0.0

            div_summary.append({"Division": div, "Count": count, "Without Tax": without_tax, "With Tax": with_tax})
            total_div_count += count
            total_div_without_tax += without_tax
            total_div_with_tax += with_tax

        if div_summary:
            div_summary.append({
                "Division": "Grand Total",
                "Count": total_div_count,
                "Without Tax": total_div_without_tax,
                "With Tax": total_div_with_tax
            })

        div_summary_df = pd.DataFrame(div_summary)

        # EXTRA SHEET: Labour Description (Count / Without Tax / With Tax) based on current filtered data
        labour_desc_df = pd.DataFrame()
        if "Labour Description" in filtered.columns:
            agg_map = {}

            if "RO No." in filtered.columns:
                agg_map["RO No."] = "count"
            else:
                filtered["_rowcount_"] = 1
                agg_map["_rowcount_"] = "sum"

            if dis_col in filtered.columns:
                agg_map[dis_col] = "sum"
            if tot_col in filtered.columns:
                agg_map[tot_col] = "sum"

            if agg_map:
                g = filtered.groupby("Labour Description", as_index=False).agg(agg_map)

                if "RO No." in g.columns:
                    g = g.rename(columns={"RO No.": "Count"})
                elif "_rowcount_" in g.columns:
                    g = g.rename(columns={"_rowcount_": "Count"})
                else:
                    g["Count"] = 0

                if dis_col in g.columns:
                    g = g.rename(columns={dis_col: "Without Tax"})
                else:
                    g["Without Tax"] = 0.0

                if tot_col in g.columns:
                    g = g.rename(columns={tot_col: "With Tax"})
                else:
                    g["With Tax"] = 0.0

                labour_desc_df = g[["Labour Description", "Count", "Without Tax", "With Tax"]].copy()

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            details_df.to_excel(writer, sheet_name="Complete Details", index=False)
            month_summary_df.to_excel(writer, sheet_name="Month-wise Summary", index=False)
            div_summary_df.to_excel(writer, sheet_name="Division Summary", index=False)
            labour_desc_df.to_excel(writer, sheet_name="Labour Description", index=False)

        output.seek(0)
        filename = f"Labour_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        print(f"Error exporting labour data: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@app.get("/api/spares/export")
def export_spares_data(division: str = None, month: str = None, advisor: str = None):
    try:
        filtered = spares_df.copy()

        if not filtered.empty:
            if division and "RO Number" in filtered.columns:
                filtered = filtered[filtered["RO Number"].apply(lambda x: get_division_from_ro(str(x)) == division)]
            if month and "Doc Date" in filtered.columns:
                filtered = filtered[filtered["Doc Date"].apply(lambda x: get_month_name(x) == month)]
            if advisor and "Service Advisor" in filtered.columns:
                filtered = filtered[filtered["Service Advisor"].apply(lambda x: str(x).strip() == advisor)]

        details_df = filtered.copy()

        month_summary = []
        total_qty = 0.0
        total_ndp = 0.0
        total_selling = 0.0
        total_mrp = 0.0

        qty_col = "Final Qty"
        ndp_col = "NDP PRIC*Qty"
        sell_col = "Selling Price/Landed Cost (Total of Issued Qty)"
        mrp_col = "MRP (Per Qty)"

        for m in FINANCIAL_YEAR_ORDER:
            if "Doc Date" not in filtered.columns:
                break
            m_filtered = filtered[filtered["Doc Date"].apply(lambda x: get_month_name(x) == m)]
            if not m_filtered.empty:
                qty = float(m_filtered[qty_col].sum()) if qty_col in m_filtered.columns else 0.0
                ndp = float(m_filtered[ndp_col].sum()) if ndp_col in m_filtered.columns else 0.0
                selling = float(m_filtered[sell_col].sum()) if sell_col in m_filtered.columns else 0.0
                mrp_val = float((m_filtered[qty_col] * m_filtered[mrp_col]).sum()) if (qty_col in m_filtered.columns and mrp_col in m_filtered.columns) else 0.0

                month_summary.append({"Month": m, "Final Qty": qty, "NDP Value": ndp, "Selling Price": selling, "MRP Value": mrp_val})
                total_qty += qty
                total_ndp += ndp
                total_selling += selling
                total_mrp += mrp_val

        if month_summary:
            month_summary.append({
                "Month": "Grand Total",
                "Final Qty": total_qty,
                "NDP Value": total_ndp,
                "Selling Price": total_selling,
                "MRP Value": total_mrp
            })

        month_summary_df = pd.DataFrame(month_summary)

        div_summary = []
        total_div_qty = 0.0
        total_div_ndp = 0.0
        total_div_selling = 0.0
        total_div_mrp = 0.0

        for div in get_spares_divisions():
            df_div = spares_df.copy()
            if not df_div.empty and "RO Number" in df_div.columns:
                df_div = df_div[df_div["RO Number"].apply(lambda x: get_division_from_ro(str(x)) == div)]

            qty = float(df_div[qty_col].sum()) if (not df_div.empty and qty_col in df_div.columns) else 0.0
            ndp = float(df_div[ndp_col].sum()) if (not df_div.empty and ndp_col in df_div.columns) else 0.0
            selling = float(df_div[sell_col].sum()) if (not df_div.empty and sell_col in df_div.columns) else 0.0
            mrp_val = float((df_div[qty_col] * df_div[mrp_col]).sum()) if (not df_div.empty and qty_col in df_div.columns and mrp_col in df_div.columns) else 0.0

            div_summary.append({"Division": div, "Final Qty": qty, "NDP Value": ndp, "Selling Price": selling, "MRP Value": mrp_val})
            total_div_qty += qty
            total_div_ndp += ndp
            total_div_selling += selling
            total_div_mrp += mrp_val

        if div_summary:
            div_summary.append({
                "Division": "Grand Total",
                "Final Qty": total_div_qty,
                "NDP Value": total_div_ndp,
                "Selling Price": total_div_selling,
                "MRP Value": total_div_mrp
            })

        div_summary_df = pd.DataFrame(div_summary)

        # EXTRA SHEET: Part Desc Wise (Spare Count / NDP / Selling / MRP) based on current filtered data
        part_desc_df = pd.DataFrame()
        if not filtered.empty and "Part Desc" in filtered.columns:
            temp = filtered.copy()

            if qty_col in temp.columns:
                temp[qty_col] = pd.to_numeric(temp[qty_col], errors="coerce").fillna(0.0)
            else:
                temp[qty_col] = 0.0

            if ndp_col in temp.columns:
                temp[ndp_col] = pd.to_numeric(temp[ndp_col], errors="coerce").fillna(0.0)
            else:
                temp[ndp_col] = 0.0

            if sell_col in temp.columns:
                temp[sell_col] = pd.to_numeric(temp[sell_col], errors="coerce").fillna(0.0)
            else:
                temp[sell_col] = 0.0

            if mrp_col in temp.columns:
                temp[mrp_col] = pd.to_numeric(temp[mrp_col], errors="coerce").fillna(0.0)
            else:
                temp[mrp_col] = 0.0

            temp["MRP_Value"] = temp[qty_col] * temp[mrp_col]

            g = temp.groupby("Part Desc", as_index=False).agg({
                qty_col: "sum",
                ndp_col: "sum",
                sell_col: "sum",
                "MRP_Value": "sum"
            })

            g = g.rename(columns={
                qty_col: "Spare Count",
                ndp_col: "NDP Value",
                sell_col: "Selling Price",
                "MRP_Value": "MRP Value"
            })

            part_desc_df = g[["Part Desc", "Spare Count", "NDP Value", "Selling Price", "MRP Value"]].copy()

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            details_df.to_excel(writer, sheet_name="Complete Details", index=False)
            month_summary_df.to_excel(writer, sheet_name="Month-wise Summary", index=False)
            div_summary_df.to_excel(writer, sheet_name="Division Summary", index=False)
            part_desc_df.to_excel(writer, sheet_name="Part Desc Wise", index=False)

        output.seek(0)
        filename = f"Spares_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        print(f"Error exporting spares data: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


# -------------------- HTML / UI --------------------

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Unnati Motors Maxi Care Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            --primary: #5b4fa0;
            --secondary: #7366bd;
            --accent: #f59e0b;
            --bg: #ffffff;
            --text: #1f2937;
            --border: #e5e7eb;
            --hover: #f3f4f6;
        }

        body.dark {
            --bg: #1f2937;
            --text: #f3f4f6;
            --border: #374151;
            --hover: #374151;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #5b4fa0 0%, #7366bd 100%);
            color: var(--text);
            min-height: 100vh;
        }

        .header {
            background: linear-gradient(135deg, #5b4fa0 0%, #7366bd 100%);
            color: white;
            padding: 1rem 1.5rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }

        .header h1 { font-size: 1.5rem; font-weight: 700; }

        .header-buttons {
            display: flex;
            gap: 0.8rem;
            align-items: center;
            flex-wrap: wrap;
        }

        .theme-toggle {
            background: rgba(255,255,255,0.2);
            border: 2px solid rgba(255,255,255,0.3);
            color: white;
            padding: 0.4rem 0.8rem;
            border-radius: 0.5rem;
            cursor: pointer;
            font-weight: 600;
            transition: all 0.3s ease;
            font-size: 0.9rem;
        }

        .theme-toggle:hover { background: rgba(255,255,255,0.3); }

        .export-btn {
            background: #10b981;
            border: none;
            color: white;
            padding: 0.4rem 1rem;
            border-radius: 0.5rem;
            cursor: pointer;
            font-weight: 600;
            transition: all 0.3s ease;
            font-size: 0.9rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .export-btn:hover { background: #059669; transform: translateY(-2px); }

        .export-btn:disabled {
            background: #9ca3af;
            cursor: not-allowed;
            transform: none;
        }

        .container { max-width: 1400px; margin: 0 auto; padding: 1.5rem 1rem; }

        .card-tabs {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }

        .card-tab {
            background: var(--bg);
            padding: 1.5rem;
            border-radius: 0.75rem;
            cursor: pointer;
            transition: all 0.3s ease;
            border: 3px solid transparent;
            opacity: 0.6;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }

        .card-tab:hover { transform: translateY(-2px); }

        .card-tab.active {
            background: linear-gradient(135deg, #5b4fa0 0%, #7366bd 100%);
            color: white;
            border: 3px solid white;
            opacity: 1;
            box-shadow: 0 0 25px 5px rgba(91,79,160,0.8), 0 8px 16px rgba(0,0,0,0.3);
            transform: translateY(-4px);
        }

        .card-tab label { font-size: 0.8rem; opacity: 0.8; display: block; margin-bottom: 0.4rem; }
        .card-tab.active label { opacity: 0.9; }
        .card-tab .title { font-size: 1.5rem; font-weight: 700; }

        .filters-row {
            background: var(--bg);
            padding: 1.25rem;
            border-radius: 0.75rem;
            margin-bottom: 1.5rem;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 1rem;
            align-items: flex-end;
        }

        .filter-group { display: flex; flex-direction: column; }
        .filter-group label { margin-bottom: 0.4rem; font-weight: 600; font-size: 0.85rem; }

        .filter-group select {
            padding: 0.6rem;
            border: 1px solid var(--border);
            border-radius: 0.5rem;
            background: var(--bg);
            color: var(--text);
            font-size: 0.95rem;
            cursor: pointer;
            transition: all 0.3s ease;
        }

        .filter-group select:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 3px rgba(91, 79, 160, 0.1);
        }

        .clear-btn {
            background: #ef4444;
            border: none;
            color: white;
            padding: 0.6rem 1.5rem;
            border-radius: 0.5rem;
            cursor: pointer;
            font-weight: 600;
            transition: all 0.3s ease;
            font-size: 0.95rem;
            width: 100%;
        }

        .clear-btn:hover {
            background: #dc2626;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(239,68,68,0.3);
        }

        .clear-btn:active { transform: translateY(0); }

        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }

        .summary-card {
            background: var(--bg);
            padding: 1.25rem;
            border-radius: 0.75rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }

        .summary-card label { font-size: 0.8rem; opacity: 0.7; display: block; margin-bottom: 0.4rem; }
        .summary-card .value { font-size: 1.3rem; font-weight: 700; color: var(--primary); word-break: break-word; }

        .table-section {
            background: var(--bg);
            padding: 1.25rem;
            border-radius: 0.75rem;
            overflow-x: auto;
        }

        .table-header {
            margin-bottom: 1rem;
            padding-bottom: 0.75rem;
            border-bottom: 2px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }

        .table-header h2 { font-size: 1.2rem; color: var(--primary); }

        table { width: 100%; border-collapse: collapse; font-size: 0.95rem; }
        table thead { background: linear-gradient(135deg, #5b4fa0 0%, #7366bd 100%); color: white; }
        table th { padding: 0.75rem; text-align: left; font-weight: 600; white-space: nowrap; font-size: 0.9rem; }

        table tbody tr { border-bottom: 1px solid var(--border); transition: background-color 0.2s ease; }
        table tbody tr:hover { background: var(--hover); }
        table td { padding: 0.75rem; text-align: left; }

        .number { font-weight: 600; color: #10b981; text-align: center; }
        .currency { color: var(--accent); font-weight: 600; text-align: right; }

        .empty { text-align: center; padding: 2rem; color: #999; }

        .loading { text-align: center; padding: 1.5rem; }
        .spinner {
            border: 4px solid var(--border);
            border-top: 4px solid var(--primary);
            border-radius: 50%;
            width: 40px; height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto;
        }

        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }

        @media (max-width: 768px) {
            .header h1 { font-size: 1.2rem; }
            .header-buttons { width: 100%; justify-content: flex-start; }
            .container { padding: 1rem; }
            .filters-row { grid-template-columns: 1fr; gap: 0.8rem; padding: 1rem; }
            .card-tabs { grid-template-columns: 1fr; gap: 0.8rem; margin-bottom: 1rem; }
            .summary-cards { grid-template-columns: 1fr; gap: 0.8rem; margin-bottom: 1rem; }
            .table-section { padding: 1rem; }
            .table-header { flex-direction: column; align-items: flex-start; }
            table { font-size: 0.85rem; }
            table th, table td { padding: 0.5rem; font-size: 0.8rem; }
            .summary-card { padding: 1rem; }
            .summary-card .value { font-size: 1.1rem; }
            .export-btn { padding: 0.35rem 0.8rem; font-size: 0.8rem; }
        }

        @media (max-width: 480px) {
            .header { padding: 0.8rem 1rem; }
            .header h1 { font-size: 1rem; }
            .theme-toggle { padding: 0.3rem 0.6rem; font-size: 0.8rem; }
            .export-btn { padding: 0.3rem 0.6rem; font-size: 0.75rem; }
            .container { padding: 0.8rem; }
            table th, table td { padding: 0.4rem; font-size: 0.75rem; }
            .summary-card .value { font-size: 1rem; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Unnati Motors Maxi Care Dashboard</h1>
        <div class="header-buttons">
            <button class="export-btn" id="exportBtn" onclick="exportData()">
                Export to Excel
            </button>
            <button class="theme-toggle" id="themeToggle">Dark Mode</button>
        </div>
    </div>

    <div class="container">
        <div class="card-tabs">
            <div class="card-tab active" id="labour-tab" onclick="switchTab('labour')">
                <label>Maxi Care Labour</label>
                <div class="title">Labour</div>
            </div>
            <div class="card-tab" id="spares-tab" onclick="switchTab('spares')">
                <label>Maxi Care Spares</label>
                <div class="title">Spares</div>
            </div>
        </div>

        <div class="filters-row">
            <div class="filter-group">
                <label for="division">Division</label>
                <select id="division">
                    <option value="">All Divisions</option>
                </select>
            </div>
            <div class="filter-group">
                <label for="month">Month</label>
                <select id="month">
                    <option value="">All Months</option>
                </select>
            </div>
            <div class="filter-group">
                <label for="advisor">Service Advisor</label>
                <select id="advisor">
                    <option value="">All Advisors</option>
                </select>
            </div>
            <div class="filter-group" style="align-self: flex-end;">
                <button class="clear-btn" onclick="clearAllFilters()">Clear All</button>
            </div>
        </div>

        <div class="summary-cards" id="summarySection"></div>

        <div class="table-section">
            <div class="table-header">
                <h2 id="tableTitle">Labour Details</h2>
            </div>
            <div id="tableContent">
                <div class="loading"><div class="spinner"></div></div>
            </div>
        </div>
    </div>

    <script>
        let currentTab = 'labour';
        let currentData = [];
        let currentSummary = {};

        function formatIndian(num) {
            if (isNaN(num)) return '0';
            const parts = num.toString().split('.');
            const intPart = parts[0];
            const decimalPart = parts[1] ? '.' + parts[1].substring(0, 2) : '';

            let result = '';
            let count = 0;

            for (let i = intPart.length - 1; i >= 0; i--) {
                if (count === 3 || (count > 3 && (count - 3) % 2 === 0)) {
                    result = ',' + result;
                }
                result = intPart[i] + result;
                count++;
            }

            return result + decimalPart;
        }

        function clearAllFilters() {
            document.getElementById('division').value = '';
            document.getElementById('month').value = '';
            document.getElementById('advisor').value = '';
            refreshDependentDropdowns();
            loadData();
        }

        function exportData() {
            const division = document.getElementById('division').value || '';
            const month = document.getElementById('month').value || '';
            const advisor = document.getElementById('advisor').value || '';

            const exportBtn = document.getElementById('exportBtn');
            exportBtn.disabled = true;
            exportBtn.textContent = 'Exporting...';

            const endpoint = currentTab === 'labour'
                ? `/api/labour/export?division=${encodeURIComponent(division)}&month=${encodeURIComponent(month)}&advisor=${encodeURIComponent(advisor)}`
                : `/api/spares/export?division=${encodeURIComponent(division)}&month=${encodeURIComponent(month)}&advisor=${encodeURIComponent(advisor)}`;

            fetch(endpoint)
                .then(response => response.blob())
                .then(blob => {
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = currentTab === 'labour'
                        ? `Labour_Report_${new Date().getTime()}.xlsx`
                        : `Spares_Report_${new Date().getTime()}.xlsx`;
                    document.body.appendChild(a);
                    a.click();
                    window.URL.revokeObjectURL(url);
                    document.body.removeChild(a);

                    exportBtn.disabled = false;
                    exportBtn.textContent = 'Export to Excel';
                })
                .catch(error => {
                    console.error('Export error:', error);
                    alert('Error exporting data');
                    exportBtn.disabled = false;
                    exportBtn.textContent = 'Export to Excel';
                });
        }

        const themeToggle = document.getElementById('themeToggle');
        const isDark = localStorage.getItem('dark') === 'true';
        if (isDark) {
            document.body.classList.add('dark');
            themeToggle.textContent = 'Light Mode';
        }

        themeToggle.addEventListener('click', () => {
            document.body.classList.toggle('dark');
            const newDark = document.body.classList.contains('dark');
            localStorage.setItem('dark', newDark);
            themeToggle.textContent = newDark ? 'Light Mode' : 'Dark Mode';
        });

        function switchTab(tab) {
            currentTab = tab;

            if (tab === 'labour') {
                document.getElementById('labour-tab').classList.add('active');
                document.getElementById('spares-tab').classList.remove('active');
                document.getElementById('tableTitle').textContent = 'Labour Details';
            } else {
                document.getElementById('spares-tab').classList.add('active');
                document.getElementById('labour-tab').classList.remove('active');
                document.getElementById('tableTitle').textContent = 'Spare Parts Details';
            }

            document.getElementById('division').value = '';
            document.getElementById('month').value = '';
            document.getElementById('advisor').value = '';

            loadDivisions();
            refreshDependentDropdowns();
            loadData();
        }

        async function loadDivisions() {
            try {
                const endpoint = currentTab === 'labour' ? '/api/labour/divisions' : '/api/spares/divisions';
                const response = await fetch(endpoint);
                const data = await response.json();
                const select = document.getElementById('division');

                const current = select.value || '';
                select.innerHTML = '<option value="">All Divisions</option>';
                (data.divisions || []).forEach(div => {
                    const option = document.createElement('option');
                    option.value = div;
                    option.textContent = div;
                    select.appendChild(option);
                });
                select.value = current;
            } catch (error) {
                console.error('Error loading divisions:', error);
            }
        }

        async function loadMonths(division) {
            try {
                const monthSelect = document.getElementById('month');
                const current = monthSelect.value || '';

                monthSelect.innerHTML = '<option value="">All Months</option>';

                const endpoint = currentTab === 'labour'
                    ? (division ? `/api/labour/months/${encodeURIComponent(division)}` : `/api/labour/months`)
                    : (division ? `/api/spares/months/${encodeURIComponent(division)}` : `/api/spares/months`);

                const response = await fetch(endpoint);
                const data = await response.json();

                (data.months || []).forEach(m => {
                    const option = document.createElement('option');
                    option.value = m;
                    option.textContent = m;
                    monthSelect.appendChild(option);
                });

                const exists = [...monthSelect.options].some(o => o.value === current);
                monthSelect.value = exists ? current : '';
            } catch (error) {
                console.error('Error loading months:', error);
            }
        }

        async function loadAdvisors(division, month) {
            try {
                const advisorSelect = document.getElementById('advisor');
                const current = advisorSelect.value || '';

                advisorSelect.innerHTML = '<option value="">All Advisors</option>';

                const params = new URLSearchParams();
                if (division) params.append('division', division);
                if (month) params.append('month', month);

                const endpoint = currentTab === 'labour'
                    ? `/api/labour/advisors?${params.toString()}`
                    : `/api/spares/advisors?${params.toString()}`;

                const response = await fetch(endpoint);
                const data = await response.json();

                (data.advisors || []).forEach(a => {
                    const option = document.createElement('option');
                    option.value = a;
                    option.textContent = a;
                    advisorSelect.appendChild(option);
                });

                const exists = [...advisorSelect.options].some(o => o.value === current);
                advisorSelect.value = exists ? current : '';
            } catch (error) {
                console.error('Error loading advisors:', error);
            }
        }

        async function refreshDependentDropdowns() {
            const division = document.getElementById('division').value || null;
            const month = document.getElementById('month').value || null;

            await loadMonths(division);

            const finalMonth = document.getElementById('month').value || null;
            await loadAdvisors(division, finalMonth);
        }

        async function loadData() {
            const division = document.getElementById('division').value || null;
            const month = document.getElementById('month').value || null;
            const advisor = document.getElementById('advisor').value || null;

            const content = document.getElementById('tableContent');
            const summarySection = document.getElementById('summarySection');

            content.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

            try {
                const params = new URLSearchParams();
                if (division) params.append('division', division);
                if (month) params.append('month', month);
                if (advisor) params.append('advisor', advisor);

                const summaryEndpoint = currentTab === 'labour'
                    ? `/api/labour/summary?${params.toString()}`
                    : `/api/spares/summary?${params.toString()}`;

                const summaryResponse = await fetch(summaryEndpoint);
                const summary = await summaryResponse.json();
                currentSummary = summary;

                const dataEndpoint = currentTab === 'labour'
                    ? `/api/labour/data?${params.toString()}`
                    : `/api/spares/data?${params.toString()}`;

                const dataResponse = await fetch(dataEndpoint);
                const data = await dataResponse.json();
                currentData = data.rows || [];

                content.innerHTML = '';

                if (!data.rows || data.rows.length === 0) {
                    summarySection.innerHTML = '';
                    content.innerHTML = '<div class="empty">No data available</div>';
                    return;
                }

                if (currentTab === 'labour') {
                    summarySection.innerHTML = `
                        <div class="summary-card">
                            <label>Total Items</label>
                            <div class="value">${summary.total_items || 0}</div>
                        </div>
                        <div class="summary-card">
                            <label>Total Without Tax</label>
                            <div class="value">Rs ${formatIndian(summary.total_dis || 0)}</div>
                        </div>
                        <div class="summary-card">
                            <label>Total With Tax</label>
                            <div class="value">Rs ${formatIndian(summary.total_amount || 0)}</div>
                        </div>
                    `;
                } else {
                    summarySection.innerHTML = `
                        <div class="summary-card">
                            <label>Total NDP Value</label>
                            <div class="value">Rs ${formatIndian(summary.total_ndp || 0)}</div>
                        </div>
                        <div class="summary-card">
                            <label>Total Selling Price</label>
                            <div class="value">Rs ${formatIndian(summary.total_selling || 0)}</div>
                        </div>
                        <div class="summary-card">
                            <label>Total MRP Value</label>
                            <div class="value">Rs ${formatIndian(summary.total_mrp || 0)}</div>
                        </div>
                    `;
                }

                const tableHTML = currentTab === 'labour'
                    ? `
                        <table>
                            <thead>
                                <tr>
                                    <th>Labour Description</th>
                                    <th style="text-align: center;">Count</th>
                                    <th style="text-align: right;">Without Tax</th>
                                    <th style="text-align: right;">With Tax</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${data.rows.map(row => `
                                    <tr>
                                        <td>${row['Labour Description']}</td>
                                        <td class="number">${row.count}</td>
                                        <td class="currency">Rs ${formatIndian(row['Labour Basic Amount-DIS'])}</td>
                                        <td class="currency">Rs ${formatIndian(row['Labour Total Amount'])}</td>
                                    </tr>
                                `).join('')}
                                <tr style="background: linear-gradient(135deg, #5b4fa0 0%, #7366bd 100%); color: white; font-weight: bold; border-top: 2px solid #5b4fa0;">
                                    <td style="padding: 1rem;">Grand Total</td>
                                    <td class="number" style="color: white;">
                                        ${data.rows.reduce((sum, row) => sum + (row.count || 0), 0)}
                                    </td>
                                    <td class="currency" style="color: white;">
                                        Rs ${formatIndian(data.rows.reduce((sum, row) => sum + (row['Labour Basic Amount-DIS'] || 0), 0))}
                                    </td>
                                    <td class="currency" style="color: white;">
                                        Rs ${formatIndian(data.rows.reduce((sum, row) => sum + (row['Labour Total Amount'] || 0), 0))}
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    `
                    : `
                        <table>
                            <thead>
                                <tr>
                                    <th>Part Description</th>
                                    <th style="text-align: center;">Final Qty</th>
                                    <th style="text-align: right;">NDP*Qty</th>
                                    <th style="text-align: right;">Selling Price</th>
                                    <th style="text-align: right;">MRP*Qty</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${data.rows.map(row => `
                                    <tr>
                                        <td>${row.part_desc}</td>
                                        <td class="number">${Math.round(row.final_qty || 0)}</td>
                                        <td class="currency">Rs ${formatIndian(row.ndp_price_qty || 0)}</td>
                                        <td class="currency">Rs ${formatIndian(row.selling_price_total || 0)}</td>
                                        <td class="currency">Rs ${formatIndian(row.mrp_total || 0)}</td>
                                    </tr>
                                `).join('')}
                                <tr style="background: linear-gradient(135deg, #5b4fa0 0%, #7366bd 100%); color: white; font-weight: bold; border-top: 2px solid #5b4fa0;">
                                    <td style="padding: 1rem;">Grand Total</td>
                                    <td class="number" style="color: white;">
                                        ${Math.round(data.rows.reduce((sum, row) => sum + (row.final_qty || 0), 0))}
                                    </td>
                                    <td class="currency" style="color: white;">
                                        Rs ${formatIndian(data.rows.reduce((sum, row) => sum + (row.ndp_price_qty || 0), 0))}
                                    </td>
                                    <td class="currency" style="color: white;">
                                        Rs ${formatIndian(data.rows.reduce((sum, row) => sum + (row.selling_price_total || 0), 0))}
                                    </td>
                                    <td class="currency" style="color: white;">
                                        Rs ${formatIndian(data.rows.reduce((sum, row) => sum + (row.mrp_total || 0), 0))}
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    `;

                content.innerHTML = tableHTML;

            } catch (error) {
                console.error('Error loading data:', error);
                summarySection.innerHTML = '';
                content.innerHTML = '<div class="empty">Error loading data</div>';
            }
        }

        document.getElementById('division').addEventListener('change', async () => {
            await refreshDependentDropdowns();
            loadData();
        });

        document.getElementById('month').addEventListener('change', async () => {
            const division = document.getElementById('division').value || null;
            const month = document.getElementById('month').value || null;
            await loadAdvisors(division, month);
            loadData();
        });

        document.getElementById('advisor').addEventListener('change', () => {
            loadData();
        });

        loadDivisions();
        refreshDependentDropdowns();
        loadData();
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    return HTML_CONTENT


if __name__ == "__main__":
    import uvicorn

    print("\n" + "=" * 80)
    print("UNNATI MOTORS MAXI CARE DASHBOARD - RENDER READY")
    print("=" * 80)

    print("\nPaths:")
    print(f"   Labour: {LABOUR_FILE}")
    print(f"   Spares: {SPARES_FILE}")

    print("\nData Loaded:")
    print(f"   Labour: {len(labour_df):,} records" if not labour_df.empty else "   Labour: 0 records")
    print(f"   Spares: {len(spares_df):,} records" if not spares_df.empty else "   Spares: 0 records")

    print("\nDashboard Running:")
    print(f"   http://0.0.0.0:{APP_PORT}")
    print("\nPress Ctrl+C to stop")
    print("=" * 80 + "\n")

    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
