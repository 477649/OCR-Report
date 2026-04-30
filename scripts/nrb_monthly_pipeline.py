#!/usr/bin/env python3
"""
NRB Development Bank Industry Analysis automation.

What this script does:
1. Reads NRB Monthly Statistics page and downloads latest XLSX files.
2. Reads ONLY sheets C8, C9 and C10 from each monthly workbook.
3. Extracts bank-wise values using bank codes in the sheet header row.
4. Builds four report sheets only: Industry Overall, Industry Analysis, Dev_Risk_Flags, and Dev_Scorecard.

Important extraction rules:
- Deposit details are taken from C8 rows under DEPOSITS:
  a. Current, b. Savings, c. Fixed, d. Call Deposits, e. Others.
- Investment in Govt. Sec is taken from C8 row a. Govt.Securities under INVESTMENT IN SECURITIES.
- Investment in Shares and Other is taken from C8 row SHARE & OTHER INVESTMENT.
- C9 is used for P&L items.
- C10 Product Wise section is used for total loan, product-wise loan analysis, product concentration, and scoring.
- Industry Analysis Loan to customers = C10 Total Product wise Loan - C8 Loan to BFIs.
- Industry Overall Loan block uses full C10 Total Product wise Loan without deducting Loan to BFIs.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

PERIOD_RE = re.compile(r"(?P<bs_year>\d{4})[-_](?P<bs_month>\d{1,2})\s*\((?P<label>[^)]*)\)")
NUMERIC_CLEAN_RE = re.compile(r"[^0-9.\-]")

DEFAULT_SOURCE_URL = "https://www.nrb.org.np/category/monthly-statistics/?department=bfr"

# Default mapping is used only when config/bfi_mapping.csv is not present.
# If you maintain config/bfi_mapping.csv, the CSV takes priority.
DEFAULT_MAPPING = [
    ("NBL", "Commercial Bank", "Nepal Bank Limited", 0),
    ("RBB", "Commercial Bank", "Rastriya Banijya Bank", 0),
    ("NABIL", "Commercial Bank", "Nabil Bank Limited", 0),
    ("NIMB", "Commercial Bank", "Nepal Investment Mega Bank Limited", 0),
    ("SCBNL", "Commercial Bank", "Standard Chartered Bank Nepal Limited", 0),
    ("HBL", "Commercial Bank", "Himalayan Bank Limited", 0),
    ("NSBI", "Commercial Bank", "Nepal SBI Bank Limited", 0),
    ("EBL", "Commercial Bank", "Everest Bank Limited", 0),
    ("NIC", "Commercial Bank", "NIC ASIA Bank Limited", 0),
    ("MBL", "Commercial Bank", "Machhapuchchhre Bank Limited", 0),
    ("Kumari", "Commercial Bank", "Kumari Bank Limited", 0),
    ("Laxmi", "Commercial Bank", "Laxmi Sunrise Limited", 0),
    ("SBL", "Commercial Bank", "Siddhartha Bank Limited", 0),
    ("ADBNL", "Commercial Bank", "Agriculture Development Bank", 0),
    ("Global", "Commercial Bank", "Global IME Bank Limited", 0),
    ("Citizen", "Commercial Bank", "Citizens Bank International Limited", 0),
    ("Prime", "Commercial Bank", "Prime Commercial Bank Limited", 0),
    ("NMB", "Commercial Bank", "NMB Bank Limited", 0),
    ("Prabhu", "Commercial Bank", "Prabhu Bank Limited", 0),
    ("Sanima", "Commercial Bank", "Sanima Bank Limited", 0),
    ("Mahalaxmi", "Development Bank", "Mahalaxmi Bikas Bank Limited", 1),
    ("Narayani", "Development Bank", "Narayani Development Bank Limited", 0),
    ("Karnali", "Development Bank", "Karnali Development Bank Limited", 0),
    ("Shangrila", "Development Bank", "Shangrila Development Bank Limited", 1),
    ("Excel", "Development Bank", "Excel Development Bank Limited", 0),
    ("Miteri", "Development Bank", "Miteri Development Bank Limited", 0),
    ("Mukti", "Development Bank", "Muktinath Bikas Bank Limited", 1),
    ("Garima", "Development Bank", "Garima Bikas Bank Limited", 1),
    ("Kamana", "Development Bank", "Kamana Sewa Bikash Bank Limited", 1),
    ("Corporate", "Development Bank", "Corporate Development Bank Limited", 0),
    ("Jyoti", "Development Bank", "Jyoti Bikas Bank Limited", 1),
    ("Shine", "Development Bank", "Shine Resunga Development Bank Limited", 1),
    ("LumbiniDB", "Development Bank", "Lumbini Bikas Bank Limited", 1),
    ("Sindhu", "Finance Company", "Sindhu Bikas Bank Limited", 0),
    ("Salapa", "Finance Company", "Salapa Bikas Bank Limited", 0),
    ("saptakoshi", "Finance Company", "Saptakoshi Development Bank Limited", 0),
    ("GreenDB", "Finance Company", "Green Development Bank Limited", 0),
    ("NFL", "Finance Company", "Nepal Finance Limited", 0),
    ("NSML", "Finance Company", "Nepal Share Markets and Finance Limited", 0),
    ("GURKHAFC", "Finance Company", "Gurkhas Finance Limited", 0),
    ("Goodwill", "Finance Company", "Goodwill Finance Limited", 0),
    ("Shree", "Finance Company", "Shree Investment & Finance Co. Limited", 0),
    ("BestFC", "Finance Company", "Best Finance Limited", 0),
    ("Progressive", "Finance Company", "Progressive Finance Limited", 0),
    ("Janaki", "Finance Company", "Janaki Finance Co. Limited", 0),
    ("Pokhara", "Finance Company", "Pokhara Finance Limited", 0),
    ("Central", "Finance Company", "Central Finance Limited", 0),
    ("Multi", "Finance Company", "Multipurpose Finance Limited", 0),
    ("Samriddhi", "Finance Company", "Samriddhi Finance Company Limited", 0),
    ("CMerchant", "Finance Company", "Capital Merchant Banking & Finance Co. Limited", 0),
    ("GMBFL", "Finance Company", "Guheshwori Merchant Banking & Finance Limited", 0),
    ("ICFC", "Finance Company", "ICFC Finance Limited", 0),
    ("Manju", "Finance Company", "Manjushree Financial Institution Limited", 0),
    ("Reliance", "Finance Company", "Reliance Finance Limited", 0),
]

REPORT_BANK_DEFAULT_ORDER = [
    "Mukti", "Garima", "Jyoti", "Shine", "LumbiniDB", "Kamana", "Mahalaxmi", "Shangrila"
]


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


def norm_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def norm_key(value: Any) -> str:
    text = norm_text(value).upper()
    text = text.replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def to_number(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text in {"", "-", "--", "N/A", "NA", "n/a"}:
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


def fetch_html(url: str) -> str:
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0 NRB monthly statistics workflow (+https://github.com/)"},
        timeout=45,
    )
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
            for later in anchors[i + 1 : i + 8]:
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
            if a.get_text(" ", strip=True).lower() == "next" and a.get("href"):
                next_link = urljoin(next_url, a.get("href"))
                break
        next_url = next_link

    return sorted(found.values(), key=lambda item: item.order, reverse=True)[:months]


def download_file(url: str, out_path: Path) -> bool:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and out_path.stat().st_size > 0:
        return False
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0 NRB monthly statistics workflow"}, timeout=120)
    response.raise_for_status()
    out_path.write_bytes(response.content)
    return True


def fiscal_fields(bs_year: int, bs_month: int) -> dict[str, int | str]:
    # Nepali FY starts at Shrawan. In this NRB numbering, Ashadh is 03, Shrawan is 04.
    if bs_month >= 4:
        fiscal_year_start = bs_year
        fiscal_month = bs_month - 3
    else:
        fiscal_year_start = bs_year - 1
        fiscal_month = bs_month + 9
    return {
        "fiscal_year_start": fiscal_year_start,
        "fiscal_year": f"{fiscal_year_start}/{str(fiscal_year_start + 1)[-2:]}",
        "fiscal_month": fiscal_month,
        "fiscal_quarter": int(math.ceil(fiscal_month / 3)),
    }


def load_mapping(mapping_path: Path) -> pd.DataFrame:
    if mapping_path.exists():
        mapping = pd.read_csv(mapping_path)
    else:
        mapping = pd.DataFrame(DEFAULT_MAPPING, columns=["bfi_code", "sector", "full_name", "include_in_report"])

    required = {"bfi_code", "sector", "full_name"}
    missing = required - set(mapping.columns)
    if missing:
        raise ValueError(f"Mapping file is missing required columns: {sorted(missing)}")

    if "include_in_report" not in mapping.columns:
        mapping["include_in_report"] = mapping["bfi_code"].isin(REPORT_BANK_DEFAULT_ORDER).astype(int)

    mapping["bfi_code"] = mapping["bfi_code"].astype(str).str.strip()
    mapping["bfi_code_norm"] = mapping["bfi_code"].map(norm_key)
    mapping["sector"] = mapping["sector"].astype(str).str.strip()
    mapping["full_name"] = mapping["full_name"].astype(str).str.strip()
    mapping["include_in_report"] = mapping["include_in_report"].fillna(0).astype(int)
    return mapping


def read_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl")
    except ValueError as exc:
        raise RuntimeError(f"Required sheet {sheet_name!r} not found in {path.name}") from exc


def find_bank_header_row(df: pd.DataFrame, mapping: pd.DataFrame, scan_rows: int = 15) -> int:
    code_norms = set(mapping["bfi_code_norm"])
    best_row = -1
    best_count = 0
    for r in range(min(scan_rows, len(df))):
        count = 0
        for value in df.iloc[r].tolist():
            if norm_key(value) in code_norms:
                count += 1
        if count > best_count:
            best_count = count
            best_row = r
    if best_row < 0 or best_count < 5:
        raise RuntimeError("Could not find BFI header row. Expected row containing bank codes like Mukti, Garima, Kamana.")
    return best_row


def get_bank_columns(df: pd.DataFrame, mapping: pd.DataFrame) -> dict[str, int]:
    header_row = find_bank_header_row(df, mapping)
    code_lookup = dict(zip(mapping["bfi_code_norm"], mapping["bfi_code"]))
    cols: dict[str, int] = {}
    for c, value in enumerate(df.iloc[header_row].tolist()):
        key = norm_key(value)
        if key in code_lookup:
            cols[code_lookup[key]] = c
    return cols


def find_label_column(df: pd.DataFrame, anchor_patterns: list[str], max_cols: int = 6) -> int:
    compiled = [re.compile(p, re.I) for p in anchor_patterns]
    best_col = 0
    best_count = -1
    for c in range(min(max_cols, df.shape[1])):
        count = 0
        for value in df.iloc[:, c].tolist():
            text = norm_text(value)
            if any(p.search(text) for p in compiled):
                count += 1
        if count > best_count:
            best_count = count
            best_col = c
    if best_count <= 0:
        raise RuntimeError("Could not identify metric label column in sheet.")
    return best_col


def row_matches(text: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, text, flags=re.I) for pattern in patterns)


def find_row(df: pd.DataFrame, label_col: int, patterns: list[str], start: int = 0, end: int | None = None) -> int | None:
    if end is None:
        end = len(df)
    for r in range(max(0, start), min(end, len(df))):
        text = norm_text(df.iat[r, label_col])
        if text and row_matches(text, patterns):
            return r
    return None


def section_end(df: pd.DataFrame, label_col: int, start_row: int) -> int:
    # Finds next top-level numbered section after a section row.
    for r in range(start_row + 1, len(df)):
        text = norm_text(df.iat[r, label_col])
        if re.match(r"^\s*\d+\s+", text):
            return r
        if text.upper().startswith("TOTAL"):
            return r
    return len(df)


def value_at(df: pd.DataFrame, row: int | None, col: int | None, scale: float = 1.0) -> float | None:
    if row is None or col is None or row >= len(df) or col >= df.shape[1]:
        return None
    num = to_number(df.iat[row, col])
    if num is None:
        return None
    return num / scale


def find_value(
    df: pd.DataFrame,
    label_col: int,
    bank_col: int,
    patterns: list[str],
    start: int = 0,
    end: int | None = None,
    scale: float = 1.0,
) -> float | None:
    return value_at(df, find_row(df, label_col, patterns, start=start, end=end), bank_col, scale=scale)


def extract_one_file(path: Path, period: MonthlyFile, mapping: pd.DataFrame) -> pd.DataFrame:
    c8 = read_sheet(path, "C8")
    c9 = read_sheet(path, "C9")
    c10 = read_sheet(path, "C10")

    c8_label = find_label_column(c8, [r"DEPOSITS", r"LOANS\s*&\s*ADVANCES", r"SHARE\s*&\s*OTHER\s+INVESTMENT"])
    c9_label = find_label_column(c9, [r"Interest Expense", r"Interest Income", r"Net Profit"])
    c10_label = find_label_column(c10, [r"Product Wise", r"Total Product wise Loan", r"Sector Wise"])

    c8_cols = get_bank_columns(c8, mapping)
    c9_cols = get_bank_columns(c9, mapping)
    c10_cols = get_bank_columns(c10, mapping)

    fields = fiscal_fields(period.bs_year, period.bs_month)

    deposits_row = find_row(c8, c8_label, [r"^\s*\d+\s+DEPOSITS\s*$", r"^\s*DEPOSITS\s*$"])
    deposits_end = section_end(c8, c8_label, deposits_row) if deposits_row is not None else None

    inv_sec_row = find_row(c8, c8_label, [r"^\s*\d+\s+INVESTMENT\s+IN\s+SECURITIES\s*$", r"^\s*INVESTMENT\s+IN\s+SECURITIES\s*$"])
    inv_sec_end = section_end(c8, c8_label, inv_sec_row) if inv_sec_row is not None else None

    borrow_row = find_row(c8, c8_label, [r"^\s*\d+\s+BORROWINGS\s*$", r"^\s*BORROWINGS\s*$"])
    borrow_end = section_end(c8, c8_label, borrow_row) if borrow_row is not None else None

    loan_row = find_row(c8, c8_label, [r"^\s*\d+\s+LOANS\s*&\s*ADVANCES", r"LOANS\s*&\s*ADVANCES\s*\(Including Bills Purchased\)"])
    loan_end = section_end(c8, c8_label, loan_row) if loan_row is not None else None

    product_row = find_row(c10, c10_label, [r"^\s*Product\s+Wise\s*$"])
    total_product_row = find_row(c10, c10_label, [r"^\s*Total\s+Product\s+wise\s+Loan\s*$"], start=product_row or 0)
    product_rows: list[tuple[str, int]] = []
    if product_row is not None and total_product_row is not None:
        for r in range(product_row + 1, total_product_row + 1):
            label = norm_text(c10.iat[r, c10_label])
            if label:
                product_rows.append((label, r))

    rows: list[dict[str, Any]] = []
    for item in mapping.itertuples(index=False):
        code = item.bfi_code
        if code not in c8_cols and code not in c9_cols and code not in c10_cols:
            continue

        c8_col = c8_cols.get(code)
        c9_col = c9_cols.get(code)
        c10_col = c10_cols.get(code)

        govt_sec_value = find_value(
            c8,
            c8_label,
            c8_col,
            [r"^\s*a\.\s*Govt\.\s*Securities\b", r"^\s*a\.\s*Govt\s*Securities\b"],
            inv_sec_row or 0,
            inv_sec_end,
            scale=1,
        )
        if govt_sec_value is None:
            govt_sec_value = value_at(c8, inv_sec_row, c8_col, scale=1)

        total_loan_from_c10 = value_at(c10, total_product_row, c10_col, scale=1000)
        loan_to_bfis = find_value(c8, c8_label, c8_col, [r"^\s*b\.\s*Financial\s+Institutions\b"], loan_row or 0, loan_end, scale=1000)
        liquid_funds = find_value(
            c8,
            c8_label,
            c8_col,
            [r"^\s*\d+\s+LIQUID\s+FUNDS\s*$", r"^\s*LIQUID\s+FUNDS\s*$"],
            0,
            None,
            scale=1000,
        )
        loan_to_customers = None
        if total_loan_from_c10 is not None and loan_to_bfis is not None:
            loan_to_customers = total_loan_from_c10 - loan_to_bfis
        elif total_loan_from_c10 is not None:
            loan_to_customers = total_loan_from_c10

        data = {
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
            "sector": item.sector,
            "full_name": item.full_name,
            "include_in_report": int(item.include_in_report),
            "Total Deposit": value_at(c8, deposits_row, c8_col, scale=1000),
            "Current": find_value(c8, c8_label, c8_col, [r"^\s*a\.\s*Current\b"], deposits_row or 0, deposits_end, scale=1000),
            "Savings": find_value(c8, c8_label, c8_col, [r"^\s*b\.\s*Savings\b"], deposits_row or 0, deposits_end, scale=1000),
            "Fixed": find_value(c8, c8_label, c8_col, [r"^\s*c\.\s*Fixed\b"], deposits_row or 0, deposits_end, scale=1000),
            "Call Deposits": find_value(c8, c8_label, c8_col, [r"^\s*d\.\s*Call\s+Deposits\b"], deposits_row or 0, deposits_end, scale=1000),
            "Others": find_value(c8, c8_label, c8_col, [r"^\s*e\.\s*Others\b"], deposits_row or 0, deposits_end, scale=1000),
            "Total loan": total_loan_from_c10,
            "Loan to BFIs": loan_to_bfis,
            "Loan to customers": loan_to_customers,
            "NBA": find_value(c8, c8_label, c8_col, [r"Non\s+Banking\s+Assets"], 0, None, scale=1),
            "Investment in Govt. Sec": govt_sec_value,
            "Investment in Shares and Other": find_value(c8, c8_label, c8_col, [r"^\s*\d+\s+SHARE\s*&\s*OTHER\s+INVESTMENT\s*$", r"^\s*SHARE\s*&\s*OTHER\s+INVESTMENT\s*$"], 0, None, scale=1),
            "Liquid Funds": liquid_funds,
            "Capital": find_value(c8, c8_label, c8_col, [r"^\s*a\.\s*Paid-up\s+Capital\b", r"^\s*a\.\s*Paid\s+up\s+Capital\b"], 0, None, scale=1000),
            "General Reserve": find_value(c8, c8_label, c8_col, [r"^\s*d\.\s*General\s+Reserves\b"], 0, None, scale=1000),
            "LLP fund": find_value(c8, c8_label, c8_col, [r"Loan\s+Loss\s+Provision"], 0, None, scale=1000),
            "Debenture": find_value(c8, c8_label, c8_col, [r"^\s*e\.\s*Bonds\s+and\s+Securities\b"], borrow_row or 0, borrow_end, scale=1000),
        }

        for label, row_idx in product_rows:
            data[f"Product Wise | {label}"] = value_at(c10, row_idx, c10_col, scale=1000)

        interest_income = find_value(c9, c9_label, c9_col, [r"^\s*1\.\s*Interest\s+Income\b"], scale=1)
        interest_expense = find_value(c9, c9_label, c9_col, [r"^\s*1\.\s*Interest\s+Expense\b"], scale=1)
        provision_risk = find_value(c9, c9_label, c9_col, [r"^\s*7\.\s*Provision\s+for\s+Risk\b"], scale=1)
        writeback = find_value(c9, c9_label, c9_col, [r"^\s*6\.\s*Write\s+Back\s+from\s+Provisions\s+for\s+loss\b"], scale=1)

        data.update({
            "Interest Income": interest_income,
            "Interest Expense": interest_expense,
            "NII": None if interest_income is None or interest_expense is None else interest_income - interest_expense,
            "Commission and Discount Income": find_value(c9, c9_label, c9_col, [r"^\s*2\.\s*Commission\s+and\s+Discount\b"], scale=1),
            "LLP Exp": None if provision_risk is None else provision_risk - (writeback or 0),
            "HR Exp (excl. Bonus)": find_value(c9, c9_label, c9_col, [r"^\s*3\.\s*Staff\s+Expense\b"], scale=1),
            "Opex": find_value(c9, c9_label, c9_col, [r"^\s*4\.\s*Office\s+Operating\s+Expenses\b"], scale=1),
            "Loan W/f": find_value(c9, c9_label, c9_col, [r"^\s*8\.\s*Loan\s+Written\s+Off\b"], scale=1),
            "Net Profit": find_value(c9, c9_label, c9_col, [r"^\s*12\.\s*Net\s+Profit\b"], scale=1),
            "Other Operating Income": find_value(c9, c9_label, c9_col, [r"^\s*4\.\s*Other\s+Operating\s+Income\b"], scale=1),
        })

        data["Current Deposit Ratio"] = safe_div(data["Current"], data["Total Deposit"])
        data["Savings Deposit Ratio"] = safe_div(data["Savings"], data["Total Deposit"])
        data["Fixed Deposit Ratio"] = safe_div(data["Fixed"], data["Total Deposit"])
        data["Call Deposit Ratio"] = safe_div(data["Call Deposits"], data["Total Deposit"])
        data["Other Deposit Ratio"] = safe_div(data["Others"], data["Total Deposit"])
        data["CASA Ratio"] = safe_div(optional_sum(data["Current"], data["Savings"]), data["Total Deposit"])
        data["Loan to Deposit Ratio"] = safe_div(data["Loan to customers"], data["Total Deposit"])
        data["Full Loan to Deposit Ratio"] = safe_div(data["Total loan"], data["Total Deposit"])
        data["Liquid Assets"] = optional_sum(data["Liquid Funds"], None if data["Investment in Govt. Sec"] is None else data["Investment in Govt. Sec"] / 1000.0)
        data["Liquidity Ratio"] = safe_div(data["Liquid Assets"], data["Total Deposit"])
        data["Govt Sec to Deposit Ratio"] = safe_div(None if data["Investment in Govt. Sec"] is None else data["Investment in Govt. Sec"] / 1000.0, data["Total Deposit"])
        data["Share Investment to Capital Ratio"] = safe_div(data["Investment in Shares and Other"], None if data["Capital"] is None else data["Capital"] * 1000.0)
        rows.append(data)

    return pd.DataFrame(rows)


def safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b in (None, 0) or pd.isna(a) or pd.isna(b):
        return None
    return a / b


def optional_sum(*values: float | None) -> float | None:
    nums = [float(v) for v in values if v is not None and not pd.isna(v)]
    if not nums:
        return None
    return float(sum(nums))


def value_for(data: pd.DataFrame, code: str, period_order: int | None, metric: str) -> float | None:
    if period_order is None or data.empty:
        return None
    sub = data[(data["bfi_code"].astype(str).str.upper() == code.upper()) & (data["period_order"] == period_order)]
    if sub.empty or metric not in sub.columns:
        return None
    val = sub.iloc[0][metric]
    return None if pd.isna(val) else float(val)


def value_by_period(data: pd.DataFrame, code: str, period_orders: dict[str, int | None], metric: str) -> dict[str, float | None]:
    cur = value_for(data, code, period_orders.get("current"), metric)
    last = value_for(data, code, period_orders.get("last_month"), metric)
    lye = value_for(data, code, period_orders.get("last_year_end"), metric)
    lyc = value_for(data, code, period_orders.get("last_year_corresponding"), metric)
    return {
        "This Month": cur,
        "Last Month": last,
        "Last Year End": lye,
        "MoM Change (Rs.)": None if cur is None or last is None else cur - last,
        "YTD Change (Rs.)": None if cur is None or lye is None else cur - lye,
        "Last Year Corresponding": lyc,
        "YoY Change": None if cur is None or lyc is None else cur - lyc,
    }


def get_period_orders(all_data: pd.DataFrame) -> dict[str, int | None]:
    latest_order = int(all_data["period_order"].max())
    latest = all_data[all_data["period_order"] == latest_order].iloc[0]
    current_bs_year = int(latest["bs_year"])
    current_bs_month = int(latest["bs_month"])
    fiscal_year_start = int(latest["fiscal_year_start"])

    # Last year end means Ashadh end of current FY. With this file naming, Ashadh = month 03.
    last_year_end_order = fiscal_year_start * 12 + 3
    last_month_order = latest_order - 1
    last_year_corresponding_order = latest_order - 12

    available_orders = set(int(x) for x in all_data["period_order"].unique())
    return {
        "current": latest_order,
        "last_month": last_month_order if last_month_order in available_orders else None,
        "last_year_end": last_year_end_order if last_year_end_order in available_orders else None,
        "last_year_corresponding": last_year_corresponding_order if last_year_corresponding_order in available_orders else None,
        "current_bs_year": current_bs_year,
        "current_bs_month": current_bs_month,
    }


def period_display_name(all_data: pd.DataFrame, order: int | None) -> str:
    if order is None:
        return "N/A"
    sub = all_data[all_data["period_order"] == order]
    if sub.empty:
        return "N/A"
    text = str(sub.iloc[0]["period_text"])
    m = PERIOD_RE.search(text)
    return m.group("label") if m else str(sub.iloc[0]["period_key"])


def select_report_banks(all_data: pd.DataFrame, mapping: pd.DataFrame, include_all_dev_banks: bool = False) -> list[str]:
    """Return report bank codes in the exact required order.

    Default report order must stay:
    Mukti, Garima, Jyoti, Shine, LumbiniDB, Kamana, Mahalaxmi, Shangrila.

    If --include-all-dev-banks is used, these eight still come first and the
    remaining Development Banks are appended in mapping-file order.
    """
    dev = mapping[mapping["sector"].str.lower().eq("development bank")].copy()
    dev_codes = dev["bfi_code"].astype(str).tolist()
    existing = set(all_data["bfi_code"].astype(str))

    ordered_core = [code for code in REPORT_BANK_DEFAULT_ORDER if code in dev_codes and code in existing]
    if not include_all_dev_banks:
        return ordered_core

    extras = [code for code in dev_codes if code not in ordered_core and code in existing]
    return ordered_core + extras


def make_rank_map(all_data: pd.DataFrame, codes: list[str], order: int | None, metric: str, descending: bool = True) -> dict[str, int | None]:
    if order is None:
        return {c: None for c in codes}
    vals = []
    for c in codes:
        v = value_for(all_data, c, order, metric)
        if v is not None:
            vals.append((c, v))
    vals.sort(key=lambda x: x[1], reverse=descending)
    ranks: dict[str, int | None] = {c: None for c in codes}
    last_val = None
    last_rank = 0
    for i, (code, val) in enumerate(vals, start=1):
        if last_val is not None and val == last_val:
            rank = last_rank
        else:
            rank = i
        ranks[code] = rank
        last_val = val
        last_rank = rank
    return ranks


def fmt_period_title(all_data: pd.DataFrame, period_orders: dict[str, int | None]) -> str:
    latest_order = period_orders["current"]
    sub = all_data[all_data["period_order"] == latest_order].iloc[0]
    text = str(sub["period_text"])
    m = PERIOD_RE.search(text)
    if m:
        return f"Industry Analysis {m.group('label').split(',')[0].strip()}"
    return f"Industry Analysis {sub['period_key']}"


def write_development_bank_report(
    all_data: pd.DataFrame,
    manifest_df: pd.DataFrame,
    mapping: pd.DataFrame,
    output_path: Path,
    include_all_dev_banks: bool = False,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    period_orders = get_period_orders(all_data)
    banks = select_report_banks(all_data, mapping, include_all_dev_banks=include_all_dev_banks)
    if not banks:
        raise RuntimeError("No Development Bank rows found for report. Check mapping and bank codes.")

    blocks = [
        "This Month", "Last Month", "Last Year End", "MoM Change (Rs.)", "YTD Change (Rs.)", "Last Year Corresponding", "YoY Change"
    ]

    deposits_cols = ["Total Deposit", "Current", "Savings", "Fixed", "Call Deposits", "Others"]
    loan_cols = ["Total loan", "Loan to customers", "Loan to BFIs", "NBA", "Investment in Govt. Sec", "Investment in Shares and Other"]
    pl_cols = ["NII", "Commission and Discount Income", "LLP Exp", "HR Exp (excl. Bonus)", "Opex", "Loan W/f"]
    bs_cols = ["Net Profit", "Other Operating Income", "Capital", "General Reserve", "LLP fund", "Debenture"]
    ratio_cols = ["Current Month", "Last Month", "Ashadh 2082", "Corresponding Year", "Increment % this year"]

    import xlsxwriter

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book

        title_fmt = workbook.add_format({"bold": True, "font_size": 11, "font_name": "Times New Roman"})
        big_title_fmt = workbook.add_format({"bold": True, "font_size": 14, "font_name": "Times New Roman"})
        orange = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#F4B183", "border": 1})
        blue = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#0070C0", "font_color": "#FFFFFF", "border": 1, "text_wrap": True})
        green = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#92D050", "border": 1})
        yellow = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#FFC000", "border": 1})
        peach = workbook.add_format({"bold": True, "bg_color": "#F8CBAD", "border": 1, "text_wrap": True})
        left_hdr = workbook.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#0070C0", "font_color": "#FFFFFF", "border": 1, "text_wrap": True})
        section_green = workbook.add_format({"bold": True, "bg_color": "#A9D18E", "border": 1})
        cell = workbook.add_format({"border": 1})
        cell_bold = workbook.add_format({"border": 1, "bold": True})
        num = workbook.add_format({"border": 1, "num_format": "#,##0.00;(#,##0.00);-"})
        num_bold = workbook.add_format({"border": 1, "bold": True, "num_format": "#,##0.00;(#,##0.00);-"})
        int_fmt = workbook.add_format({"border": 1, "num_format": "0"})
        int_bold = workbook.add_format({"border": 1, "bold": True, "num_format": "0"})
        pct = workbook.add_format({"border": 1, "num_format": "0.00%;(0.00%);-"})
        pct_bold = workbook.add_format({"border": 1, "bold": True, "num_format": "0.00%;(0.00%);-"})
        blank_fmt = workbook.add_format({"border": 1})
        blank_bold = workbook.add_format({"border": 1, "bold": True})

        def write_value(ws, row: int, col: int, val: float | int | None, fmt, blank_format=None) -> None:
            if blank_format is None:
                blank_format = blank_fmt
            if val is None or (isinstance(val, float) and pd.isna(val)):
                ws.write_blank(row, col, None, blank_format)
            else:
                ws.write_number(row, col, float(val), fmt)

        def group_format(block: str):
            if block == "Last Year End":
                return green
            if block in {"MoM Change (Rs.)", "YTD Change (Rs.)", "YoY Change"}:
                return yellow
            return orange

        # Create Industry Overall first so it appears as the first worksheet tab.
        ws2 = workbook.add_worksheet("Industry Overall")
        writer.sheets["Industry Overall"] = ws2

        ws = workbook.add_worksheet("Industry Analysis")
        writer.sheets["Industry Analysis"] = ws
        ws.set_landscape()
        ws.fit_to_pages(1, 0)
        ws.set_zoom(55)
        ws.freeze_panes(5, 2)

        ws.write(0, 0, "Kamana Sewa Bikas Bank Ltd.", title_fmt)
        ws.write(1, 0, "FY 2082-83", title_fmt)
        ws.write(3, 0, fmt_period_title(all_data, period_orders), title_fmt)

        def write_wide_section(start_row: int, section_label: str, rank_label: str, metrics: list[str], rank_metric: str, section_fmt) -> int:
            ws.merge_range(start_row, 0, start_row, 1, "Bank's name", orange)
            col = 2
            for block in blocks:
                ws.merge_range(start_row, col, start_row, col + len(metrics) - 1, block, group_format(block))
                col += len(metrics)

            ws.write(start_row + 1, 0, section_label, left_hdr if section_fmt is None else section_fmt)
            ws.write(start_row + 1, 1, rank_label, blue)
            col = 2
            for _block in blocks:
                for m in metrics:
                    pretty = m
                    if m == "Investment in Govt. Sec":
                        pretty = "Investment\nin Govt. Sec\n(Rs. in Mn)"
                    elif m == "Investment in Shares and Other":
                        pretty = "Investment in\nShares and Other\n(Rs. in Mn)"
                    elif m == "Total loan":
                        pretty = "Total loan\n(Rs. in Bn)"
                    elif m == "Loan to customers":
                        pretty = "Loan to\ncustomers\n(Rs. in Bn)"
                    elif m == "Loan to BFIs":
                        pretty = "Loan to BFIs\n(Rs. in Bn)"
                    ws.write(start_row + 1, col, pretty, blue)
                    col += 1

            ranks = make_rank_map(all_data, banks, period_orders["current"], rank_metric, descending=True)
            for i, code in enumerate(banks):
                r = start_row + 2 + i
                is_kamana = code.strip().upper() == "KAMANA"
                row_cell_fmt = cell_bold if is_kamana else cell
                row_num_fmt = num_bold if is_kamana else num
                row_int_fmt = int_bold if is_kamana else int_fmt
                row_blank_fmt = blank_bold if is_kamana else blank_fmt

                ws.write(r, 0, code, row_cell_fmt)
                write_value(ws, r, 1, ranks.get(code), row_int_fmt, row_blank_fmt)
                col = 2
                for block in blocks:
                    for m in metrics:
                        vals = value_by_period(all_data, code, period_orders, m)
                        write_value(ws, r, col, vals[block], row_num_fmt, row_blank_fmt)
                        col += 1
            return start_row + 2 + len(banks) + 2

        row = 4
        row = write_wide_section(row, "Deposit\n(Rs. in Bn)", "Rank", deposits_cols, "Total Deposit", None)
        row = write_wide_section(row, "Others (Loan and other)", "Rank", loan_cols, "Total loan", peach)
        row = write_wide_section(row, "PL Items\n(Rs. in Mn)", "Rank\n(NII)", pl_cols, "NII", None)
        row = write_wide_section(row, "PL Items\n(Rs. in Mn)/ Balance\nsheet items (Rs. In Bn)", "Rank", bs_cols, "Net Profit", None)

        def write_ratio_section(start_row: int, ratio_name: str, metric: str) -> int:
            ws.write(start_row, 0, ratio_name, title_fmt)
            ws.write(start_row + 1, 0, "Deposit\n(Rs. in Bn)", blue)
            ws.write(start_row + 1, 1, "Rank", blue)
            for i, h in enumerate(ratio_cols, start=2):
                ws.write(start_row + 1, i, h, blue)
            ranks = make_rank_map(all_data, banks, period_orders["current"], metric, descending=True)
            for i, code in enumerate(banks):
                r = start_row + 2 + i
                is_kamana = code.strip().upper() == "KAMANA"
                row_cell_fmt = cell_bold if is_kamana else cell
                row_int_fmt = int_bold if is_kamana else int_fmt
                row_pct_fmt = pct_bold if is_kamana else pct
                row_blank_fmt = blank_bold if is_kamana else blank_fmt

                ws.write(r, 0, code, row_cell_fmt)
                write_value(ws, r, 1, ranks.get(code), row_int_fmt, row_blank_fmt)
                cur = value_for(all_data, code, period_orders.get("current"), metric)
                last = value_for(all_data, code, period_orders.get("last_month"), metric)
                lye = value_for(all_data, code, period_orders.get("last_year_end"), metric)
                lyc = value_for(all_data, code, period_orders.get("last_year_corresponding"), metric)
                inc = None if cur is None or lye is None else cur - lye
                for j, v in enumerate([cur, last, lye, lyc, inc], start=2):
                    write_value(ws, r, j, v, row_pct_fmt, row_blank_fmt)
            return start_row + 2 + len(banks) + 2

        row = write_ratio_section(row, "Ratios (Savings Deposit)", "Savings Deposit Ratio")
        row = write_ratio_section(row, "Ratios (Loan to Deposit Ratio)", "Loan to Deposit Ratio")

        ws.set_column(0, 0, 13)
        ws.set_column(1, 1, 7)
        ws.set_column(2, 2 + 6 * len(blocks), 11)
        for r in range(4, row + 1):
            ws.set_row(r, 20)
        ws.set_row(5, 38)

        def agg_value(order: int | None, metric: str, selector: str) -> float | None:
            if order is None or metric not in all_data.columns:
                return None
            sub = all_data[all_data["period_order"] == order].copy()
            if selector == "industry":
                sub = sub[sub["sector"].isin(["Commercial Bank", "Development Bank", "Finance Company"])]
            elif selector in {"Commercial Bank", "Development Bank", "Finance Company"}:
                sub = sub[sub["sector"].eq(selector)]
            elif selector == "Kamana":
                sub = sub[sub["bfi_code"].astype(str).str.upper().eq("KAMANA")]
            vals = pd.to_numeric(sub[metric], errors="coerce").dropna()
            if vals.empty:
                return None
            return float(vals.sum())

        def agg_period_values(metric: str, selector: str) -> dict[str, float | None]:
            cur = agg_value(period_orders.get("current"), metric, selector)
            last = agg_value(period_orders.get("last_month"), metric, selector)
            lye = agg_value(period_orders.get("last_year_end"), metric, selector)
            lyc = agg_value(period_orders.get("last_year_corresponding"), metric, selector)
            return {
                "This Month": cur,
                "Last Month": last,
                "Last Year End": lye,
                "MoM Change (Rs.)": None if cur is None or last is None else cur - last,
                "YTD Change (Rs.)": None if cur is None or lye is None else cur - lye,
                "Last Year Corresponding": lyc,
                "YoY Change": None if cur is None or lyc is None else cur - lyc,
            }

        def coverage_period_values(metric: str, denominator_selector: str) -> dict[str, float | None]:
            result = {}
            for block, order in [
                ("This Month", period_orders.get("current")),
                ("Last Month", period_orders.get("last_month")),
                ("Last Year End", period_orders.get("last_year_end")),
                ("Last Year Corresponding", period_orders.get("last_year_corresponding")),
            ]:
                num_val = agg_value(order, metric, "Kamana")
                den_val = agg_value(order, metric, denominator_selector)
                result[block] = safe_div(num_val, den_val)
            result["MoM Change (Rs.)"] = None if result["This Month"] is None or result["Last Month"] is None else result["This Month"] - result["Last Month"]
            result["YTD Change (Rs.)"] = None if result["This Month"] is None or result["Last Year End"] is None else result["This Month"] - result["Last Year End"]
            result["YoY Change"] = None if result["This Month"] is None or result["Last Year Corresponding"] is None else result["This Month"] - result["Last Year Corresponding"]
            return result

        ws2.set_zoom(90)
        ws2.freeze_panes(3, 1)
        ws2.merge_range(0, 0, 0, 7, "Incremental Industry Analysis", big_title_fmt)
        for c, h in enumerate(["Particulars"] + blocks):
            ws2.write(1, c, h, blue)
        ws2.set_column(0, 0, 34)
        ws2.set_column(1, 7, 16)

        def write_overall_block(start_row: int, title: str, metric: str, value_fmt) -> int:
            ws2.merge_range(start_row, 0, start_row, 7, title, section_green)
            rows = [
                ("Industry", agg_period_values(metric, "industry"), value_fmt),
                ("Commercial Bank", agg_period_values(metric, "Commercial Bank"), value_fmt),
                ("Development bank", agg_period_values(metric, "Development Bank"), value_fmt),
                ("Finance Company", agg_period_values(metric, "Finance Company"), value_fmt),
                ("Kamana Sewa Bikash Bank", agg_period_values(metric, "Kamana"), value_fmt),
                ("KSBBL coverage in industry %", coverage_period_values(metric, "industry"), pct),
                ("KSBBL coverage in development %", coverage_period_values(metric, "Development Bank"), pct),
            ]
            for i, (label, vals, fmt) in enumerate(rows, start=start_row + 1):
                is_bold = "KSBBL" in label or "Kamana" in label
                ws2.write(i, 0, label, cell_bold if is_bold else cell)
                for j, block in enumerate(blocks, start=1):
                    cell_fmt = pct_bold if is_bold and fmt is pct else num_bold if is_bold else fmt
                    write_value(ws2, i, j, vals.get(block), cell_fmt, blank_bold if is_bold else blank_fmt)
            return start_row + len(rows) + 2

        overall_row = 2
        overall_row = write_overall_block(overall_row, "Deposits", "Total Deposit", num)
        overall_row = write_overall_block(overall_row, "Loan", "Total loan", num)

        # Product-wise columns are still needed for concentration risk scoring,
        # but no separate Dev_Product_Analysis sheet is created in the 4-sheet report.
        product_cols = [c for c in all_data.columns if c.startswith("Product Wise | ") and not c.endswith("Total Product wise Loan")]
        current_order = period_orders.get("current")
        dev_codes = mapping[mapping["sector"].eq("Development Bank")]["bfi_code"].astype(str).tolist()

        def bank_period_change(code: str, metric: str, compare_order: int | None) -> float | None:
            cur_val = value_for(all_data, code, current_order, metric)
            prev_val = value_for(all_data, code, compare_order, metric)
            if cur_val is None or prev_val is None:
                return None
            return cur_val - prev_val

        def bank_period_pct_change(code: str, metric: str, compare_order: int | None) -> float | None:
            cur_val = value_for(all_data, code, current_order, metric)
            prev_val = value_for(all_data, code, compare_order, metric)
            if cur_val is None or prev_val in (None, 0):
                return None
            return (cur_val / prev_val) - 1

        def current_dev_values(metric: str) -> dict[str, float]:
            vals = {}
            for code in banks:
                v = value_for(all_data, code, current_order, metric)
                if v is not None:
                    vals[code] = v
            return vals

        def current_rank(metric: str, descending: bool = True) -> dict[str, int | None]:
            return make_rank_map(all_data, banks, current_order, metric, descending=descending)

        def product_concentration(code: str) -> dict[str, Any]:
            values = []
            for col_name in product_cols:
                product = col_name.split("|", 1)[1].strip()
                if product.lower() == "total product wise loan":
                    continue
                v = value_for(all_data, code, current_order, col_name)
                if v is not None:
                    values.append((product, v))
            values.sort(key=lambda x: x[1], reverse=True)
            total = value_for(all_data, code, current_order, "Total loan")
            top1_name, top1_value = (values[0] if values else (None, None))
            top3_value = sum(v for _, v in values[:3]) if values else None
            return {
                "top1_name": top1_name,
                "top1_value": top1_value,
                "top1_ratio": safe_div(top1_value, total),
                "top3_value": top3_value,
                "top3_ratio": safe_div(top3_value, total),
            }

        def percentile_score(metric: str, code: str, positive: bool = True) -> float | None:
            vals = current_dev_values(metric)
            if code not in vals or len(vals) <= 1:
                return None
            sorted_vals = sorted(vals.items(), key=lambda item: item[1], reverse=positive)
            for idx, (bank_code, _value) in enumerate(sorted_vals):
                if bank_code == code:
                    return 100.0 * (len(sorted_vals) - idx - 1) / (len(sorted_vals) - 1)
            return None

        def score_from_dict(vals: dict[str, float], target: str, positive: bool) -> float | None:
            if target not in vals or len(vals) <= 1:
                return None
            arr = sorted(vals.items(), key=lambda item: item[1], reverse=positive)
            for idx, (bank_code, _value) in enumerate(arr):
                if bank_code == target:
                    return 100.0 * (len(arr) - idx - 1) / (len(arr) - 1)
            return None

        def risk_signal(value: float | None, red_if: bool, amber_if: bool) -> str:
            if value is None:
                return "Data missing"
            if red_if:
                return "High"
            if amber_if:
                return "Medium"
            return "Low"

        # Ranking maps reused by Risk Flags and Scorecard.
        dep_rank = current_rank("Total Deposit", True)
        sav_rank = current_rank("Savings Deposit Ratio", True)
        casa_rank = current_rank("CASA Ratio", True)
        liq_rank = current_rank("Liquidity Ratio", True)
        nii_rank = current_rank("NII", True)
        profit_rank = current_rank("Net Profit", True)

        # ------------------------------------------------------------------
        # Dev_Risk_Flags and Dev_Scorecard
        # ------------------------------------------------------------------
        risk_records: list[dict[str, Any]] = []
        sav_series = pd.Series(current_dev_values("Savings Deposit Ratio"), dtype="float64")
        liq_series = pd.Series(current_dev_values("Liquidity Ratio"), dtype="float64")
        profit_yield_vals: dict[str, float] = {}
        for code in banks:
            npv = value_for(all_data, code, current_order, "Net Profit")
            loanv = value_for(all_data, code, current_order, "Total loan")
            py = safe_div(npv, None if loanv is None else loanv * 1000)
            if py is not None:
                profit_yield_vals[code] = py
        profit_yield_series = pd.Series(profit_yield_vals, dtype="float64")
        sav_q1 = float(sav_series.quantile(0.25)) if not sav_series.empty else None
        liq_q1 = float(liq_series.quantile(0.25)) if not liq_series.empty else None
        prof_q1 = float(profit_yield_series.quantile(0.25)) if not profit_yield_series.empty else None

        for code in banks:
            ldr = value_for(all_data, code, current_order, "Loan to Deposit Ratio")
            liq = value_for(all_data, code, current_order, "Liquidity Ratio")
            savings_ratio = value_for(all_data, code, current_order, "Savings Deposit Ratio")
            fixed_ratio = value_for(all_data, code, current_order, "Fixed Deposit Ratio")
            dep_yoy = bank_period_pct_change(code, "Total Deposit", period_orders.get("last_year_corresponding"))
            loan_yoy = bank_period_pct_change(code, "Total loan", period_orders.get("last_year_corresponding"))
            growth_gap = None if dep_yoy is None or loan_yoy is None else loan_yoy - dep_yoy
            nii = value_for(all_data, code, current_order, "NII")
            llp_exp = value_for(all_data, code, current_order, "LLP Exp")
            hr_exp = value_for(all_data, code, current_order, "HR Exp (excl. Bonus)")
            opex = value_for(all_data, code, current_order, "Opex")
            cost_to_nii = safe_div(optional_sum(hr_exp, opex), nii)
            llp_to_nii = safe_div(llp_exp, nii)
            net_profit = value_for(all_data, code, current_order, "Net Profit")
            loan = value_for(all_data, code, current_order, "Total loan")
            profit_yield = safe_div(net_profit, None if loan is None else loan * 1000)
            share_inv_cap = value_for(all_data, code, current_order, "Share Investment to Capital Ratio")
            conc = product_concentration(code)
            top3 = conc["top3_ratio"]

            flags = {
                "LDR Flag": risk_signal(ldr, ldr is not None and ldr > 0.90, ldr is not None and ldr > 0.87),
                "Liquidity Flag": risk_signal(liq, liq is not None and ((liq_q1 is not None and liq <= liq_q1) or liq < 0.18), liq is not None and liq < 0.25),
                "Savings Mix Flag": risk_signal(savings_ratio, savings_ratio is not None and ((sav_q1 is not None and savings_ratio <= sav_q1) or savings_ratio < 0.35), savings_ratio is not None and savings_ratio < 0.45),
                "Fixed Deposit Flag": risk_signal(fixed_ratio, fixed_ratio is not None and fixed_ratio > 0.55, fixed_ratio is not None and fixed_ratio > 0.45),
                "Deposit Growth Flag": risk_signal(dep_yoy, dep_yoy is not None and dep_yoy < 0, dep_yoy is not None and dep_yoy < 0.03),
                "Loan Growth Gap Flag": risk_signal(growth_gap, growth_gap is not None and growth_gap > 0.06, growth_gap is not None and growth_gap > 0.03),
                "Provision Burden Flag": risk_signal(llp_to_nii, llp_to_nii is not None and llp_to_nii > 0.25, llp_to_nii is not None and llp_to_nii > 0.15),
                "Cost Efficiency Flag": risk_signal(cost_to_nii, cost_to_nii is not None and cost_to_nii > 0.60, cost_to_nii is not None and cost_to_nii > 0.50),
                "Profitability Flag": risk_signal(profit_yield, profit_yield is not None and ((prof_q1 is not None and profit_yield <= prof_q1) or profit_yield < 0.006), profit_yield is not None and profit_yield < 0.010),
                "Share Investment Flag": risk_signal(share_inv_cap, share_inv_cap is not None and share_inv_cap > 0.50, share_inv_cap is not None and share_inv_cap > 0.25),
                "Product Concentration Flag": risk_signal(top3, top3 is not None and top3 > 0.75, top3 is not None and top3 > 0.60),
            }
            risk_score = sum(2 if v == "High" else 1 if v == "Medium" else 0 for v in flags.values())
            risk_level = "High" if risk_score >= 10 else "Medium" if risk_score >= 5 else "Low"
            high_flags = [k.replace(" Flag", "") for k, v in flags.items() if v == "High"]
            med_flags = [k.replace(" Flag", "") for k, v in flags.items() if v == "Medium"]
            summary_parts = []
            if high_flags:
                summary_parts.append("High: " + ", ".join(high_flags))
            if med_flags:
                summary_parts.append("Medium: " + ", ".join(med_flags[:4]))
            if not summary_parts:
                summary_parts.append("No major peer-relative risk flag")
            risk_records.append({
                "Bank": code,
                "Risk Score": risk_score,
                "Risk Level": risk_level,
                "Loan to Deposit Ratio": ldr,
                "Liquidity Ratio": liq,
                "Savings Deposit Ratio": savings_ratio,
                "Fixed Deposit Ratio": fixed_ratio,
                "Deposit YoY %": dep_yoy,
                "Loan YoY %": loan_yoy,
                "Loan Growth Gap vs Deposit": growth_gap,
                "LLP / NII": llp_to_nii,
                "Cost to NII": cost_to_nii,
                "Net Profit / Loan": profit_yield,
                "Share Investment / Capital": share_inv_cap,
                "Top Product": conc["top1_name"],
                "Top Product %": conc["top1_ratio"],
                "Top 3 Product Concentration": top3,
                **flags,
                "Risk Summary": "; ".join(summary_parts),
            })

        risk_df = pd.DataFrame(risk_records)
        ws7 = workbook.add_worksheet("Dev_Risk_Flags")
        writer.sheets["Dev_Risk_Flags"] = ws7
        ws7.write(0, 0, "Development Bank Risk Flags", big_title_fmt)
        risk_headers = list(risk_df.columns)
        for c, h in enumerate(risk_headers):
            ws7.write(2, c, h, blue)
        high_fmt = workbook.add_format({"border": 1, "bg_color": "#F4CCCC"})
        med_fmt = workbook.add_format({"border": 1, "bg_color": "#FFF2CC"})
        low_fmt = workbook.add_format({"border": 1, "bg_color": "#D9EAD3"})
        high_bold_fmt = workbook.add_format({"border": 1, "bold": True, "bg_color": "#F4CCCC"})
        med_bold_fmt = workbook.add_format({"border": 1, "bold": True, "bg_color": "#FFF2CC"})
        low_bold_fmt = workbook.add_format({"border": 1, "bold": True, "bg_color": "#D9EAD3"})
        for r_idx, rec in enumerate(risk_records, start=3):
            is_kamana = str(rec["Bank"]).upper() == "KAMANA"
            for c, h in enumerate(risk_headers):
                v = rec.get(h)
                base_fmt = cell_bold if is_kamana else cell
                if h in {"Risk Score"}:
                    write_value(ws7, r_idx, c, v, int_bold if is_kamana else int_fmt, blank_bold if is_kamana else blank_fmt)
                elif h in {"Bank", "Risk Level", "Top Product", "Risk Summary"} or h.endswith("Flag"):
                    fmt = base_fmt
                    if h.endswith("Flag") or h == "Risk Level":
                        if v == "High":
                            fmt = high_bold_fmt if is_kamana else high_fmt
                        elif v == "Medium":
                            fmt = med_bold_fmt if is_kamana else med_fmt
                        elif v == "Low":
                            fmt = low_bold_fmt if is_kamana else low_fmt
                    ws7.write(r_idx, c, v or "", fmt)
                elif "%" in h or "Ratio" in h or h in {"LLP / NII", "Cost to NII", "Net Profit / Loan", "Share Investment / Capital", "Top Product %", "Top 3 Product Concentration", "Loan Growth Gap vs Deposit"}:
                    write_value(ws7, r_idx, c, v, pct_bold if is_kamana else pct, blank_bold if is_kamana else blank_fmt)
                else:
                    write_value(ws7, r_idx, c, v, num_bold if is_kamana else num, blank_bold if is_kamana else blank_fmt)
        ws7.set_column(0, len(risk_headers) - 1, 16)
        ws7.set_column(risk_headers.index("Risk Summary"), risk_headers.index("Risk Summary"), 55)
        ws7.freeze_panes(3, 1)

        score_records: list[dict[str, Any]] = []
        nii_yield_values: dict[str, float] = {}
        cost_values: dict[str, float] = {}
        risk_values: dict[str, float] = {}

        for rec in risk_records:
            rc = str(rec["Bank"])
            rnii = value_for(all_data, rc, current_order, "NII")
            rloan = value_for(all_data, rc, current_order, "Total loan")
            ryield = safe_div(rnii, None if rloan is None else rloan * 1000)
            if ryield is not None:
                nii_yield_values[rc] = ryield
            rhr = value_for(all_data, rc, current_order, "HR Exp (excl. Bonus)")
            rox = value_for(all_data, rc, current_order, "Opex")
            rcost = safe_div(optional_sum(rhr, rox), rnii)
            if rcost is not None:
                cost_values[rc] = rcost
            risk_values[rc] = rec["Risk Score"]

        def choose_growth_compare_order() -> tuple[int | None, str]:
            if period_orders.get("last_year_corresponding") is not None:
                return period_orders.get("last_year_corresponding"), "YoY"
            if period_orders.get("last_year_end") is not None:
                return period_orders.get("last_year_end"), "YTD"
            if period_orders.get("last_month") is not None:
                return period_orders.get("last_month"), "MoM"
            return None, "N/A"

        growth_compare_order, growth_basis = choose_growth_compare_order()

        def growth_score_components(metric: str) -> tuple[dict[str, float], dict[str, float], dict[str, float], dict[str, float], dict[str, float]]:
            """Return absolute growth, percentage growth, and blended growth scores.

            Blended growth avoids favouring only large banks:
            70% = percentage growth score, 30% = absolute rupee growth score.
            If only one growth measure is available, the available score is used.
            """
            abs_growth: dict[str, float] = {}
            pct_growth: dict[str, float] = {}
            for bc in banks:
                cur_v = value_for(all_data, bc, current_order, metric)
                prev_v = value_for(all_data, bc, growth_compare_order, metric)
                if cur_v is None or prev_v is None:
                    continue
                abs_growth[bc] = cur_v - prev_v
                if prev_v != 0:
                    pct_growth[bc] = (cur_v / prev_v) - 1
            abs_scores = {bc: score_from_dict(abs_growth, bc, True) for bc in banks}
            pct_scores = {bc: score_from_dict(pct_growth, bc, True) for bc in banks}
            blended: dict[str, float] = {}
            for bc in banks:
                ps = pct_scores.get(bc)
                av = abs_scores.get(bc)
                if ps is not None and av is not None:
                    blended[bc] = (ps * 0.70) + (av * 0.30)
                elif ps is not None:
                    blended[bc] = ps
                elif av is not None:
                    blended[bc] = av
            return abs_growth, pct_growth, abs_scores, pct_scores, blended

        dep_abs_g, dep_pct_g, _dep_abs_score, _dep_pct_score, dep_growth_score = growth_score_components("Total Deposit")
        loan_abs_g, loan_pct_g, _loan_abs_score, _loan_pct_score, loan_growth_score = growth_score_components("Total loan")
        profit_abs_g, profit_pct_g, _profit_abs_score, _profit_pct_score, profit_growth_score = growth_score_components("Net Profit")
        nii_abs_g, nii_pct_g, _nii_abs_score, _nii_pct_score, nii_growth_score = growth_score_components("NII")

        def rank_from_dict(vals: dict[str, float], positive: bool = True) -> dict[str, int | None]:
            if not vals:
                return {bc: None for bc in banks}
            arr = sorted(vals.items(), key=lambda item: item[1], reverse=positive)
            ranks: dict[str, int | None] = {bc: None for bc in banks}
            last_val = None
            last_rank = 0
            for idx, (bc, val) in enumerate(arr, start=1):
                if last_val is not None and val == last_val:
                    rank = last_rank
                else:
                    rank = idx
                ranks[bc] = rank
                last_val = val
                last_rank = rank
            return ranks

        dep_growth_rank = rank_from_dict(dep_growth_score, True)
        loan_growth_rank = rank_from_dict(loan_growth_score, True)
        profit_growth_rank = rank_from_dict(profit_growth_score, True)
        nii_growth_rank = rank_from_dict(nii_growth_score, True)
        loan_rank_map = current_rank("Total loan", True)

        for code in banks:
            components = {
                "Deposit Scale Score": percentile_score("Total Deposit", code, True),
                "Loan Scale Score": percentile_score("Total loan", code, True),
                "Deposit Growth Score": dep_growth_score.get(code),
                "Loan Growth Score": loan_growth_score.get(code),
                "Profit Growth Score": profit_growth_score.get(code),
                "NII Growth Score": nii_growth_score.get(code),
                "Savings Mix Score": percentile_score("Savings Deposit Ratio", code, True),
                "CASA Score": percentile_score("CASA Ratio", code, True),
                "Liquidity Score": percentile_score("Liquidity Ratio", code, True),
                "Net Profit Score": percentile_score("Net Profit", code, True),
                "NII Yield Score": score_from_dict(nii_yield_values, code, True),
                "Cost Efficiency Score": score_from_dict(cost_values, code, False),
                "Risk Control Score": score_from_dict(risk_values, code, False),
            }
            weights = {
                "Deposit Scale Score": 0.07,
                "Loan Scale Score": 0.07,
                "Deposit Growth Score": 0.12,
                "Loan Growth Score": 0.12,
                "Profit Growth Score": 0.08,
                "NII Growth Score": 0.06,
                "Savings Mix Score": 0.08,
                "CASA Score": 0.06,
                "Liquidity Score": 0.12,
                "Net Profit Score": 0.07,
                "NII Yield Score": 0.05,
                "Cost Efficiency Score": 0.05,
                "Risk Control Score": 0.05,
            }
            weighted_sum = 0.0
            weight_sum = 0.0
            for k, wgt in weights.items():
                if components.get(k) is not None:
                    weighted_sum += float(components[k]) * wgt
                    weight_sum += wgt
            overall_score = None if weight_sum == 0 else weighted_sum / weight_sum
            risk_rec = next((r for r in risk_records if r["Bank"] == code), {})
            grade = "A" if overall_score is not None and overall_score >= 80 else "B" if overall_score is not None and overall_score >= 65 else "C" if overall_score is not None and overall_score >= 50 else "D"
            score_records.append({
                "Bank": code,
                "Overall Score": overall_score,
                "Grade": grade,
                "Growth Basis": growth_basis,
                "Deposit Rank": dep_rank.get(code),
                "Loan Rank": loan_rank_map.get(code),
                "Deposit Growth Rank": dep_growth_rank.get(code),
                "Loan Growth Rank": loan_growth_rank.get(code),
                "Profit Growth Rank": profit_growth_rank.get(code),
                "NII Growth Rank": nii_growth_rank.get(code),
                "Savings Ratio Rank": sav_rank.get(code),
                "Liquidity Rank": liq_rank.get(code),
                "NII Rank": nii_rank.get(code),
                "Net Profit Rank": profit_rank.get(code),
                "Risk Score": risk_rec.get("Risk Score"),
                "Risk Level": risk_rec.get("Risk Level"),
                "Deposit Growth %": dep_pct_g.get(code),
                "Loan Growth %": loan_pct_g.get(code),
                "Profit Growth %": profit_pct_g.get(code),
                "NII Growth %": nii_pct_g.get(code),
                "Deposit Growth Rs.": dep_abs_g.get(code),
                "Loan Growth Rs.": loan_abs_g.get(code),
                "Profit Growth Rs.": profit_abs_g.get(code),
                "NII Growth Rs.": nii_abs_g.get(code),
                **components,
            })
        score_df = pd.DataFrame(score_records)
        score_df["Overall Rank"] = score_df["Overall Score"].rank(ascending=False, method="min")
        score_df = score_df.sort_values(["Overall Rank", "Bank"])
        ws8 = workbook.add_worksheet("Dev_Scorecard")
        writer.sheets["Dev_Scorecard"] = ws8
        ws8.write(0, 0, "Development Bank Ranking and Peer Scorecard", big_title_fmt)
        score_headers = ["Overall Rank"] + [c for c in score_df.columns if c != "Overall Rank"]
        for c, h in enumerate(score_headers):
            ws8.write(2, c, h, blue)
        score_num_fmt = workbook.add_format({"border": 1, "num_format": "0.0"})
        score_num_bold = workbook.add_format({"border": 1, "bold": True, "num_format": "0.0"})
        for r_idx, rec in enumerate(score_df.to_dict("records"), start=3):
            is_kamana = str(rec["Bank"]).upper() == "KAMANA"
            for c, h in enumerate(score_headers):
                v = rec.get(h)
                if h in {"Bank", "Grade", "Risk Level", "Growth Basis"}:
                    ws8.write(r_idx, c, v or "", cell_bold if is_kamana else cell)
                elif h.endswith("Score") or h == "Overall Score":
                    write_value(ws8, r_idx, c, v, score_num_bold if is_kamana else score_num_fmt, blank_bold if is_kamana else blank_fmt)
                elif "%" in h:
                    write_value(ws8, r_idx, c, v, pct_bold if is_kamana else pct, blank_bold if is_kamana else blank_fmt)
                elif "Rs." in h:
                    write_value(ws8, r_idx, c, v, num_bold if is_kamana else num, blank_bold if is_kamana else blank_fmt)
                else:
                    write_value(ws8, r_idx, c, v, int_bold if is_kamana else int_fmt, blank_bold if is_kamana else blank_fmt)
        ws8.set_column(0, len(score_headers) - 1, 16)
        ws8.freeze_panes(3, 1)



def run_pipeline(args: argparse.Namespace) -> None:
    repo_root = Path(args.repo_root).resolve()
    raw_dir = repo_root / "data" / "raw"
    processed_dir = repo_root / "data" / "processed"
    reports_dir = repo_root / "reports"
    state_dir = repo_root / "data" / "state"
    for d in [raw_dir, processed_dir, reports_dir, state_dir]:
        d.mkdir(parents=True, exist_ok=True)

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

    latest_period = periods[0].period_key
    output_path = reports_dir / f"Development_Bank_Industry_Analysis_{latest_period}.xlsx"
    state_path = state_dir / "latest.json"
    previous_state = {}
    if state_path.exists():
        try:
            previous_state = json.loads(state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            previous_state = {}

    if (
        not args.force
        and not downloaded_any
        and previous_state.get("latest_period") == latest_period
        and output_path.exists()
    ):
        print(json.dumps({"status": "no_new_data", "latest_period": latest_period, "report": str(output_path.relative_to(repo_root))}, indent=2))
        return

    extracted_frames = []
    period_map = {p.period_key: p for p in periods}
    for row in manifest_records:
        period = period_map[row["period_key"]]
        path = repo_root / row["local_file"]
        frame = extract_one_file(path, period, mapping)
        if not frame.empty:
            extracted_frames.append(frame)

    if not extracted_frames:
        raise RuntimeError("No C8/C9/C10 bank-wise data extracted. Check source workbook layout.")

    all_data = pd.concat(extracted_frames, ignore_index=True)
    all_data.to_csv(processed_dir / "nrb_c8_c9_c10_extracted.csv", index=False)

    write_development_bank_report(
        all_data=all_data,
        manifest_df=manifest_df,
        mapping=mapping,
        output_path=output_path,
        include_all_dev_banks=args.include_all_dev_banks,
    )

    state = {
        "latest_period": latest_period,
        "latest_url": periods[0].xlsx_url,
        "downloaded_any": downloaded_any,
        "report": str(output_path.relative_to(repo_root)),
        "raw_file_count": len(manifest_records),
        "extracted_rows": int(len(all_data)),
        "logic": {
            "sheets_read": ["C8", "C9", "C10"],
            "analysis_sheets": ["Industry Overall", "Industry Analysis", "Dev_Risk_Flags", "Dev_Scorecard"],
            "investment_govt_sec": "C8 row a. Govt.Securities under INVESTMENT IN SECURITIES",
            "investment_shares_and_other": "C8 row SHARE & OTHER INVESTMENT",
            "deposit_items": "C8 rows under DEPOSITS: a Current, b Savings, c Fixed, d Call Deposits, e Others",
            "total_loan": "C10 row Total Product wise Loan",
            "industry_analysis_total_loan": "C10 row Total Product wise Loan",
            "industry_analysis_loan_to_customers": "C10 Total Product wise Loan minus C8 b. Financial Institutions",
            "industry_overall_loan": "Full C10 Total Product wise Loan without deducting Loan to BFIs",
            "scorecard_growth_logic": "Scale is reduced; growth scores use 70% percentage growth and 30% absolute growth, using YoY where available, then YTD, then MoM.",
        },
    }
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    print(json.dumps(state, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Development Bank Industry Analysis from NRB C8, C9 and C10 sheets.")
    parser.add_argument("--source-url", default=DEFAULT_SOURCE_URL)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--mapping", default="config/bfi_mapping.csv")
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--max-pages", type=int, default=8)
    parser.add_argument("--include-all-dev-banks", action="store_true", help="Include every Development Bank in mapping instead of only include_in_report=1 banks.")
    parser.add_argument("--target-bank", default=None, help="Backward-compatible argument. The industry-analysis report includes Development Banks; this value is accepted but not used.")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
