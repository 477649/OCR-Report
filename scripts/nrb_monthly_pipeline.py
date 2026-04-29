import argparse
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

PERIOD_RE = re.compile(r"(?P<bs_year>\d{4})[-_](?P<bs_month>\d{1,2})\s*\((?P<label>[^)]*)\)")
NUMERIC_CLEAN_RE = re.compile(r"[^0-9.\-]")

@dataclass(frozen=True)
class MonthlyFile:
    period_text: str
    bs_year: int
    bs_month: int
    label: str
    xlsx_url: str

    @property
    def period_key(self) -> str:
        return f"{self.bs_year:04d}-{self.bs_month:02d}"

    @property
    def slug(self) -> str:
        label = re.sub(r"[^A-Za-z0-9]+", "_", self.label).strip("_")
        return f"{self.period_key}_{label}"

    @property
    def order(self) -> int:
        return self.bs_year * 12 + self.bs_month


def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 NRB monthly statistics workflow (+https://github.com/)"
    }
    response = requests.get(url, headers=headers, timeout=45)
    response.raise_for_status()
    return response.text


def parse_monthly_files(start_url: str, max_pages: int = 8, months: int = 24) -> list[MonthlyFile]:
    found: dict[str, MonthlyFile] = {}
    next_url: str | None = start_url
    pages = 0
    while next_url and pages < max_pages and len(found) < months:
        pages += 1
        soup = BeautifulSoup(fetch_html(next_url), "html.parser")
        anchors = soup.find_all("a")
        for i, a in enumerate(anchors):
            text = " ".join(a.get_text(" ", strip=True).split())
            match = PERIOD_RE.search(text)
            if not match:
                continue
            xlsx_url = None
            for later in anchors[i + 1 : i + 6]:
                later_text = later.get_text(" ", strip=True).lower()
                href = later.get("href")
                if not href:
                    continue
                if "xlsx" in later_text or href.lower().endswith(".xlsx"):
                    xlsx_url = urljoin(next_url, href)
                    break
            if not xlsx_url:
                continue
            period = MonthlyFile(
                period_text=text,
                bs_year=int(match.group("bs_year")),
                bs_month=int(match.group("bs_month")),
                label=match.group("label"),
                xlsx_url=xlsx_url,
            )
            found[period.period_key] = period
        next_link = None
        for a in anchors:
            if a.get_text(" ", strip=True).lower() == "next":
                next_link = urljoin(next_url, a.get("href"))
                break
        next_url = next_link
    return sorted(found.values(), key=lambda item: item.order, reverse=True)[:months]


def download_file(url: str, out_path: Path) -> bool:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and out_path.stat().st_size > 0:
        return False
    headers = {"User-Agent": "Mozilla/5.0 NRB monthly statistics workflow"}
    response = requests.get(url, headers=headers, timeout=90)
    response.raise_for_status()
    out_path.write_bytes(response.content)
    return True


def fiscal_fields(bs_year: int, bs_month: int) -> dict[str, int | str]:
    if bs_month >= 4:
        fiscal_year_start = bs_year
        fy_month = bs_month - 3
    else:
        fiscal_year_start = bs_year - 1
        fy_month = bs_month + 9
    quarter = int(math.ceil(fy_month / 3))
    return {
        "fiscal_year_start": fiscal_year_start,
        "fiscal_year": f"{fiscal_year_start}/{str(fiscal_year_start + 1)[-2:]}",
        "fiscal_month": fy_month,
        "fiscal_quarter": quarter,
    }


def load_mapping(mapping_path: Path) -> pd.DataFrame:
    mapping = pd.read_csv(mapping_path)
    mapping["bfi_code_norm"] = mapping["bfi_code"].astype(str).str.strip().str.upper()
    return mapping


def clean_metric(value) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"^\d+[.)\-\s]+", "", text).strip()
    return text or None


def to_number(value):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text in {"", "-", "--", "N/A", "NA"}:
        return None
    neg = text.startswith("(") and text.endswith(")")
    text = NUMERIC_CLEAN_RE.sub("", text)
    if text in {"", ".", "-"}:
        return None
    try:
        num = float(text)
        return -num if neg else num
    except ValueError:
        return None


def row_bank_match_count(row_values: list, code_norms: set[str]) -> int:
    count = 0
    for value in row_values:
        text = clean_metric(value)
        if not text:
            continue
        if text.strip().upper() in code_norms:
            count += 1
    return count


def extract_bank_metric_long(xlsx_path: Path, period: MonthlyFile, mapping: pd.DataFrame) -> pd.DataFrame:
    sheets = pd.read_excel(xlsx_path, sheet_name=None, header=None, engine="openpyxl")
    code_norms = set(mapping["bfi_code_norm"])
    all_rows = []
    for sheet_name, df in sheets.items():
        if df.empty:
            continue
        header_row = None
        best_count = 0
        scan_rows = min(30, len(df))
        for r in range(scan_rows):
            count = row_bank_match_count(df.iloc[r].tolist(), code_norms)
            if count > best_count:
                best_count = count
                header_row = r
        if header_row is None or best_count < 5:
            continue
        bank_cols = []
        for c, value in enumerate(df.iloc[header_row].tolist()):
            text = clean_metric(value)
            if text and text.upper() in code_norms:
                original = mapping.loc[mapping["bfi_code_norm"] == text.upper(), "bfi_code"].iloc[0]
                bank_cols.append((c, original))
        if not bank_cols:
            continue
        first_bank_col = min(c for c, _ in bank_cols)
        for r in range(header_row + 1, len(df)):
            label_parts = []
            for c in range(0, first_bank_col):
                part = clean_metric(df.iat[r, c])
                if part:
                    label_parts.append(part)
            metric = " | ".join(label_parts)
            if not metric or len(metric) < 2:
                continue
            if metric.lower().startswith("note"):
                break
            for c, code in bank_cols:
                value = to_number(df.iat[r, c]) if c < df.shape[1] else None
                if value is None:
                    continue
                fields = fiscal_fields(period.bs_year, period.bs_month)
                all_rows.append({
                    "period_key": period.period_key,
                    "period_text": period.period_text,
                    "bs_year": period.bs_year,
                    "bs_month": period.bs_month,
                    "period_order": period.order,
                    "fiscal_year": fields["fiscal_year"],
                    "fiscal_year_start": fields["fiscal_year_start"],
                    "fiscal_month": fields["fiscal_month"],
                    "fiscal_quarter": fields["fiscal_quarter"],
                    "bfi_code": code,
                    "metric": metric,
                    "value": value,
                    "source_sheet": str(sheet_name),
                    "source_file": xlsx_path.name,
                })
    if not all_rows:
        return pd.DataFrame()
    out = pd.DataFrame(all_rows)
    out = out.merge(mapping.drop(columns=["bfi_code_norm"]), on="bfi_code", how="left")
    return out


def metric_bucket(metric: str) -> str:
    m = metric.lower()
    if "deposit" in m and "total" in m:
        return "Total Deposit"
    if ("loan" in m or "credit" in m) and "total" in m:
        return "Total Loan/Credit"
    if "net profit" in m or "profit" in m:
        return "Net Profit"
    if "liquid" in m:
        return "Liquid Assets"
    if "investment" in m:
        return "Investment"
    if "asset" in m and "total" in m:
        return "Total Assets"
    if "capital" in m:
        return "Capital"
    if "borrowing" in m:
        return "Borrowing"
    return "Other"


def safe_pct(current, previous):
    if previous is None or pd.isna(previous) or previous == 0 or current is None or pd.isna(current):
        return None
    return (current / previous) - 1


def compute_growth(long_df: pd.DataFrame) -> pd.DataFrame:
    if long_df.empty:
        return long_df
    df = long_df.copy()
    df["metric_bucket"] = df["metric"].apply(metric_bucket)
    key_cols = ["bfi_code", "metric"]
    df = df.sort_values(["bfi_code", "metric", "period_order"])
    df["mom_growth"] = df.groupby(key_cols)["value"].pct_change(1)
    df["yoy_growth"] = df.groupby(key_cols)["value"].pct_change(12)
    df["previous_value"] = df.groupby(key_cols)["value"].shift(1)
    lookup = df.set_index(["bfi_code", "metric", "period_order"])["value"].to_dict()
    qtd_values = []
    ytd_values = []
    for row in df.itertuples(index=False):
        bs_year = int(row.bs_year)
        bs_month = int(row.bs_month)
        fy_month = int(row.fiscal_month)
        period_order = int(row.period_order)
        q_start_fy_month = ((int(row.fiscal_quarter) - 1) * 3) + 1
        months_back_q = fy_month - q_start_fy_month + 1
        prior_q_end_order = period_order - months_back_q
        months_back_y = fy_month
        prior_y_end_order = period_order - months_back_y
        key_q = (row.bfi_code, row.metric, prior_q_end_order)
        key_y = (row.bfi_code, row.metric, prior_y_end_order)
        qtd_values.append(safe_pct(row.value, lookup.get(key_q)))
        ytd_values.append(safe_pct(row.value, lookup.get(key_y)))
    df["qtd_growth_vs_prior_q_end"] = qtd_values
    df["ytd_growth_vs_prior_fy_end"] = ytd_values
    return df


def latest_rankings(growth_df: pd.DataFrame) -> pd.DataFrame:
    if growth_df.empty:
        return growth_df
    latest_order = growth_df["period_order"].max()
    latest = growth_df[(growth_df["period_order"] == latest_order) & (growth_df["metric_bucket"] != "Other")].copy()
    latest["value_rank_desc"] = latest.groupby("metric")["value"].rank(ascending=False, method="min")
    latest["yoy_rank_desc"] = latest.groupby("metric")["yoy_growth"].rank(ascending=False, method="min")
    latest["mom_rank_desc"] = latest.groupby("metric")["mom_growth"].rank(ascending=False, method="min")
    return latest


def sector_summary(growth_df: pd.DataFrame) -> pd.DataFrame:
    if growth_df.empty:
        return growth_df
    latest_order = growth_df["period_order"].max()
    latest = growth_df[(growth_df["period_order"] == latest_order) & (growth_df["metric_bucket"] != "Other")]
    cols = ["sector", "metric_bucket", "value", "mom_growth", "yoy_growth", "qtd_growth_vs_prior_q_end", "ytd_growth_vs_prior_fy_end"]
    return latest[cols].groupby(["sector", "metric_bucket"], dropna=False).agg(
        bank_count=("value", "count"),
        total_value=("value", "sum"),
        median_value=("value", "median"),
        median_mom_growth=("mom_growth", "median"),
        median_yoy_growth=("yoy_growth", "median"),
        median_qtd_growth=("qtd_growth_vs_prior_q_end", "median"),
        median_ytd_growth=("ytd_growth_vs_prior_fy_end", "median"),
    ).reset_index()


def write_report(growth_df: pd.DataFrame, manifest_df: pd.DataFrame, target_bank: str, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rankings = latest_rankings(growth_df)
    summary = sector_summary(growth_df)
    latest_order = growth_df["period_order"].max() if not growth_df.empty else None
    latest_period = growth_df.loc[growth_df["period_order"] == latest_order, "period_key"].iloc[0] if latest_order else "N/A"
    bank_df = growth_df[growth_df["bfi_code"].str.upper() == target_bank.upper()].copy() if not growth_df.empty else pd.DataFrame()
    latest_bank = bank_df[bank_df["period_order"] == latest_order].copy() if latest_order and not bank_df.empty else pd.DataFrame()
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book
        title_fmt = workbook.add_format({"bold": True, "font_size": 16, "font_color": "#1f4e79"})
        header_fmt = workbook.add_format({"bold": True, "bg_color": "#DDEBF7", "border": 1})
        pct_fmt = workbook.add_format({"num_format": "0.00%"})
        num_fmt = workbook.add_format({"num_format": "#,##0.00"})
        text_fmt = workbook.add_format({"text_wrap": True, "valign": "top"})
        dashboard_rows = [
            ["NRB Monthly BFI Automated Growth Report", ""],
            ["Latest period", latest_period],
            ["Target bank", target_bank],
            ["Files tracked", len(manifest_df)],
            ["Notes", "Growth is computed from available monthly snapshots: MoM, YoY, QTD vs prior quarter-end, and YTD vs prior fiscal-year-end."],
        ]
        dash = pd.DataFrame(dashboard_rows, columns=["Item", "Value"])
        dash.to_excel(writer, sheet_name="Dashboard", index=False, startrow=0)
        ws = writer.sheets["Dashboard"]
        ws.write(0, 0, dashboard_rows[0][0], title_fmt)
        ws.set_column("A:A", 34)
        ws.set_column("B:B", 95, text_fmt)
        if not latest_bank.empty:
            show_cols = ["metric_bucket", "metric", "value", "mom_growth", "yoy_growth", "qtd_growth_vs_prior_q_end", "ytd_growth_vs_prior_fy_end", "sector", "full_name"]
            latest_bank[show_cols].sort_values(["metric_bucket", "metric"]).head(60).to_excel(writer, sheet_name="Dashboard", index=False, startrow=7)
            for col_num, value in enumerate(show_cols):
                ws.write(7, col_num, value, header_fmt)
            ws.set_column("C:C", 16, num_fmt)
            ws.set_column("D:G", 16, pct_fmt)
            ws.set_column("B:B", 45, text_fmt)
        dashboard_source = pd.DataFrame({
            "source": ["NRB Monthly Statistics"],
            "url": ["https://www.nrb.org.np/category/monthly-statistics/?department=bfr"],
        })
        dashboard_source.to_excel(writer, sheet_name="Source", index=False)
        growth_df.to_excel(writer, sheet_name="Bankwise_Growth", index=False)
        rankings.to_excel(writer, sheet_name="Latest_Rankings", index=False)
        summary.to_excel(writer, sheet_name="Sector_Summary", index=False)
        manifest_df.to_excel(writer, sheet_name="Manifest", index=False)
        for sheet in ["Bankwise_Growth", "Latest_Rankings", "Sector_Summary", "Manifest", "Source"]:
            if sheet not in writer.sheets:
                continue
            w = writer.sheets[sheet]
            w.freeze_panes(1, 0)
            w.autofilter(0, 0, 0, 30)
            w.set_row(0, None, header_fmt)
            w.set_column(0, 0, 13)
            w.set_column(1, 8, 15)
            w.set_column(9, 12, 16)
            w.set_column(13, 13, 48, text_fmt)
            w.set_column(14, 16, 18, num_fmt)
            w.set_column(17, 22, 16, pct_fmt)


def run_pipeline(args):
    repo_root = Path(args.repo_root).resolve()
    raw_dir = repo_root / "data" / "raw"
    processed_dir = repo_root / "data" / "processed"
    reports_dir = repo_root / "reports"
    state_dir = repo_root / "data" / "state"
    processed_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    mapping = load_mapping(repo_root / args.mapping)
    periods = parse_monthly_files(args.source_url, max_pages=args.max_pages, months=args.months)
    if not periods:
        raise RuntimeError("No monthly XLSX links found on the NRB page.")
    manifest_records = []
    downloaded_any = False
    for period in periods:
        file_name = f"{period.slug}.xlsx"
        raw_path = raw_dir / file_name
        downloaded = download_file(period.xlsx_url, raw_path)
        downloaded_any = downloaded_any or downloaded
        fields = fiscal_fields(period.bs_year, period.bs_month)
        manifest_records.append({
            "period_key": period.period_key,
            "period_text": period.period_text,
            "bs_year": period.bs_year,
            "bs_month": period.bs_month,
            "fiscal_year": fields["fiscal_year"],
            "fiscal_month": fields["fiscal_month"],
            "fiscal_quarter": fields["fiscal_quarter"],
            "xlsx_url": period.xlsx_url,
            "local_file": str(raw_path.relative_to(repo_root)),
            "downloaded_this_run": downloaded,
        })
    manifest_df = pd.DataFrame(manifest_records).sort_values("period_key")
    manifest_df.to_csv(processed_dir / "nrb_monthly_manifest.csv", index=False)
    previous_state_path = state_dir / "latest.json"
    previous_state = {}
    if previous_state_path.exists():
        try:
            previous_state = json.loads(previous_state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            previous_state = {}
    expected_report = reports_dir / f"NRB_BFI_Growth_Report_{periods[0].period_key}.xlsx"
    if (not args.force and not downloaded_any and previous_state.get("latest_period") == periods[0].period_key and expected_report.exists()):
        print(json.dumps({"status": "no_new_data", "latest_period": periods[0].period_key, "report": str(expected_report.relative_to(repo_root))}, indent=2))
        return
    all_long = []
    period_by_file = {Path(row["local_file"]).name: next(p for p in periods if p.period_key == row["period_key"]) for row in manifest_records}
    for row in manifest_records:
        path = repo_root / row["local_file"]
        period = period_by_file[path.name]
        extracted = extract_bank_metric_long(path, period, mapping)
        if not extracted.empty:
            all_long.append(extracted)
    if not all_long:
        raise RuntimeError("Downloaded files were found, but no bank-wise metric tables could be extracted. Check the workbook layout.")
    long_df = pd.concat(all_long, ignore_index=True)
    long_df.to_csv(processed_dir / "nrb_bankwise_long.csv", index=False)
    growth_df = compute_growth(long_df)
    growth_df.to_csv(processed_dir / "nrb_bankwise_growth.csv", index=False)
    latest_period = periods[0].period_key
    output_path = reports_dir / f"NRB_BFI_Growth_Report_{latest_period}.xlsx"
    write_report(growth_df, manifest_df, args.target_bank, output_path)
    state = {
        "latest_period": periods[0].period_key,
        "latest_url": periods[0].xlsx_url,
        "downloaded_any": downloaded_any,
        "report": str(output_path.relative_to(repo_root)),
        "raw_file_count": len(manifest_records),
    }
    (state_dir / "latest.json").write_text(json.dumps(state, indent=2), encoding="utf-8")
    print(json.dumps(state, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-url", default="https://www.nrb.org.np/category/monthly-statistics/?department=bfr")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--mapping", default="config/bfi_mapping.csv")
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--max-pages", type=int, default=8)
    parser.add_argument("--target-bank", default="Kamana")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    run_pipeline(args)

if __name__ == "__main__":
    main()
