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
SOURCE_URL = "https://www.nrb.org.np/category/monthly-statistics/?department=bfr"
DEV_BANK_ORDER = ["Mukti", "Garima", "Shine", "Jyoti", "Kamana", "LumbiniDB", "Shangrila", "Mahalaxmi"]
INVESTMENT_GOVT_SEC_SOURCE_LABEL = r"\bSHARE\s*&\s*OTHER\s+INVESTMENT\b"

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
    headers = {"User-Agent": "Mozilla/5.0 NRB monthly statistics workflow (+https://github.com/)"}
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
                href = a.get("href")
                if href:
                    next_link = urljoin(next_url, href)
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
    required = {"bfi_code", "sector", "full_name"}
    missing = required.difference(mapping.columns)
    if missing:
        raise ValueError(f"Mapping file missing columns: {sorted(missing)}")
    mapping["bfi_code"] = mapping["bfi_code"].astype(str).str.strip()
    mapping["bfi_code_norm"] = mapping["bfi_code"].str.upper()
    return mapping


def clean_text(value) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).replace("\n", " ").replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_label(value) -> str:
    text = clean_text(value)
    text = re.sub(r"^\d+(?:\.\d+)*[.)\-\s]+", "", text).strip()
    return text


def to_number(value):
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


def safe_subtract(a, b):
    if a is None or pd.isna(a):
        return None
    if b is None or pd.isna(b):
        return a
    return a - b


def mn_to_bn(value):
    if value is None or pd.isna(value):
        return None
    return value / 1000.0


def safe_ratio(numerator, denominator):
    if numerator is None or denominator is None or pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return None
    return numerator / denominator


def row_bank_match_count(row_values: list, code_norms: set[str]) -> int:
    count = 0
    for value in row_values:
        text = clean_text(value).upper()
        if text in code_norms:
            count += 1
    return count


def find_bank_header(df: pd.DataFrame, mapping: pd.DataFrame) -> tuple[int, list[tuple[int, str]], int]:
    code_norms = set(mapping["bfi_code_norm"])
    header_row = None
    best_count = 0
    for r in range(min(40, len(df))):
        count = row_bank_match_count(df.iloc[r].tolist(), code_norms)
        if count > best_count:
            best_count = count
            header_row = r
    if header_row is None or best_count < 5:
        raise ValueError("Could not detect BFI code header row. Check C8/C9 sheet format or mapping codes.")
    bank_cols: list[tuple[int, str]] = []
    for c, value in enumerate(df.iloc[header_row].tolist()):
        text = clean_text(value).upper()
        if text in code_norms:
            original = mapping.loc[mapping["bfi_code_norm"] == text, "bfi_code"].iloc[0]
            bank_cols.append((c, original))
    first_bank_col = min(c for c, _ in bank_cols)
    return header_row, bank_cols, first_bank_col


def label_for_row(df: pd.DataFrame, row_idx: int, first_bank_col: int) -> str:
    parts = []
    for c in range(first_bank_col):
        text = normalize_label(df.iat[row_idx, c])
        if text:
            parts.append(text)
    return " | ".join(parts)


def find_value(df: pd.DataFrame, bank_col: int, first_bank_col: int, include_patterns: list[str], exclude_patterns: list[str] | None = None):
    exclude_patterns = exclude_patterns or []
    candidates = []
    for r in range(len(df)):
        label = label_for_row(df, r, first_bank_col)
        if not label:
            continue
        if all(re.search(p, label, flags=re.IGNORECASE) for p in include_patterns):
            if any(re.search(p, label, flags=re.IGNORECASE) for p in exclude_patterns):
                continue
            value = to_number(df.iat[r, bank_col]) if bank_col < df.shape[1] else None
            candidates.append((r, label, value))
    for _, _, value in candidates:
        if value is not None:
            return value
    return None


def get_sheet(workbook_path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(workbook_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    except ValueError as exc:
        raise ValueError(f"Required sheet {sheet_name} not found in {workbook_path.name}") from exc


def extract_monthly_c8_c9(workbook_path: Path, period: MonthlyFile, mapping: pd.DataFrame) -> pd.DataFrame:
    c8 = get_sheet(workbook_path, "C8")
    c9 = get_sheet(workbook_path, "C9")
    _, c8_bank_cols, c8_first = find_bank_header(c8, mapping)
    _, c9_bank_cols, c9_first = find_bank_header(c9, mapping)
    c8_cols = {code.upper(): col for col, code in c8_bank_cols}
    c9_cols = {code.upper(): col for col, code in c9_bank_cols}
    fields = fiscal_fields(period.bs_year, period.bs_month)
    rows = []
    for _, mrow in mapping.iterrows():
        code = mrow["bfi_code"]
        code_norm = str(code).upper()
        if code_norm not in c8_cols:
            continue
        c8_col = c8_cols[code_norm]
        c9_col = c9_cols.get(code_norm)
        deposit_total = find_value(c8, c8_col, c8_first, [r"\bDEPOSITS\b"], [r"Current|Saving|Fixed|Call|Certificate|Others|Domestic|Foreign"])
        current_deposit = find_value(c8, c8_col, c8_first, [r"Current\s+Deposit"])
        saving_deposit = find_value(c8, c8_col, c8_first, [r"Saving\s+Account"])
        fixed_deposit = find_value(c8, c8_col, c8_first, [r"Fixed\s+Account"], [r"Up to|3 to 6|6 months|Above"])
        call_deposit = find_value(c8, c8_col, c8_first, [r"Call\s+Deposit"])
        deposit_others = None
        if deposit_total is not None:
            known = sum(v for v in [current_deposit, saving_deposit, fixed_deposit, call_deposit] if v is not None)
            deposit_others = deposit_total - known
        total_loan = find_value(c8, c8_col, c8_first, [r"LOANS\s*&\s*ADVANCES"], [r"Collected|Against|Private|Financial|Government|Bills|Import|Accrued|Staff"])
        loan_to_bfis = find_value(c8, c8_col, c8_first, [r"Financial\s+Institutions"], [r"Accrued|Other"])
        loan_to_customers = safe_subtract(total_loan, loan_to_bfis)
        nba = find_value(c8, c8_col, c8_first, [r"Non[-\s]*Banking\s+Assets"])
        investment_govt_sec = find_value(c8, c8_col, c8_first, [INVESTMENT_GOVT_SEC_SOURCE_LABEL])
        investment_shares_other = find_value(c8, c8_col, c8_first, [r"SHARE\s*&\s*OTHER\s+INVESTMENT"])
        cash_balance = find_value(c8, c8_col, c8_first, [r"Cash\s+Balance"])
        bank_balance = find_value(c8, c8_col, c8_first, [r"Bank\s+Balance"])
        money_at_call = find_value(c8, c8_col, c8_first, [r"Money\s+at\s+Call"])
        paid_up_capital = find_value(c8, c8_col, c8_first, [r"Paid[-\s]*up\s+Capital"])
        general_reserve = find_value(c8, c8_col, c8_first, [r"General\s+Reserves?"])
        llp_fund = find_value(c8, c8_col, c8_first, [r"Loan\s+Loss\s+Provision"], [r"General|Special|Additional"])
        debenture = find_value(c8, c8_col, c8_first, [r"Bonds\s+and\s+Securities|Debenture"])
        interest_income = interest_expense = commission_income = provision_risk = write_back = staff_exp = office_opex = loan_writeoff = net_profit = other_op_income = None
        if c9_col is not None:
            interest_expense = find_value(c9, c9_col, c9_first, [r"Interest\s+Expense|On\s+Deposit\s+Liabilities"], [r"Income"])
            interest_income = find_value(c9, c9_col, c9_first, [r"Interest\s+Income"], [r"On\s+Loans|On\s+Investment|On\s+Agency|On\s+Call|On\s+Others"])
            commission_income = find_value(c9, c9_col, c9_first, [r"Commission\s+and\s+Discount|Commission\s*&\s*Discount"])
            if commission_income is None:
                bills_discount = find_value(c9, c9_col, c9_first, [r"Bills\s+Purchase\s+and\s+Discount"])
                commission_only = find_value(c9, c9_col, c9_first, [r"Commission"], [r"Expense"])
                commission_income = sum(v for v in [bills_discount, commission_only] if v is not None) if any(v is not None for v in [bills_discount, commission_only]) else None
            provision_risk = find_value(c9, c9_col, c9_first, [r"Provision\s+for\s+Risk"])
            write_back = find_value(c9, c9_col, c9_first, [r"Write\s+Back\s+from\s+Provisions\s+for\s+loss"])
            staff_exp = find_value(c9, c9_col, c9_first, [r"Staff\s+Expense"])
            office_opex = find_value(c9, c9_col, c9_first, [r"Office\s+Operating\s+Expenses"])
            loan_writeoff = find_value(c9, c9_col, c9_first, [r"Loan\s+Written\s+Off"])
            net_profit = find_value(c9, c9_col, c9_first, [r"Net\s+Profit"])
            other_op_income = find_value(c9, c9_col, c9_first, [r"Other\s+Operating\s+Income"])
        nii = None
        if interest_income is not None and interest_expense is not None:
            nii = interest_income - interest_expense
        llp_exp = None
        if provision_risk is not None:
            llp_exp = provision_risk - (write_back or 0)
        liquid_assets_mn = sum(v for v in [cash_balance, bank_balance, money_at_call, investment_govt_sec] if v is not None)
        record = {
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
            "sector": mrow["sector"],
            "full_name": mrow["full_name"],
            "total_deposit": mn_to_bn(deposit_total),
            "current_deposit": mn_to_bn(current_deposit),
            "saving_deposit": mn_to_bn(saving_deposit),
            "fixed_deposit": mn_to_bn(fixed_deposit),
            "call_deposit": mn_to_bn(call_deposit),
            "deposit_others": mn_to_bn(deposit_others),
            "total_loan": mn_to_bn(total_loan),
            "loan_to_customers": mn_to_bn(loan_to_customers),
            "loan_to_bfis": mn_to_bn(loan_to_bfis),
            "nba_mn": nba,
            "investment_govt_sec_mn": investment_govt_sec,
            "investment_shares_other_mn": investment_shares_other,
            "cash_balance_mn": cash_balance,
            "bank_balance_mn": bank_balance,
            "money_at_call_mn": money_at_call,
            "liquid_assets_mn": liquid_assets_mn if liquid_assets_mn != 0 else None,
            "capital": mn_to_bn(paid_up_capital),
            "general_reserve": mn_to_bn(general_reserve),
            "llp_fund": mn_to_bn(llp_fund),
            "debenture": mn_to_bn(debenture),
            "nii_mn": nii,
            "commission_income_mn": commission_income,
            "llp_exp_mn": llp_exp,
            "hr_exp_excl_bonus_mn": staff_exp,
            "opex_mn": office_opex,
            "loan_writeoff_mn": loan_writeoff,
            "net_profit_mn": net_profit,
            "other_operating_income_mn": other_op_income,
            "savings_deposit_ratio": safe_ratio(mn_to_bn(saving_deposit), mn_to_bn(deposit_total)),
            "loan_to_deposit_ratio": safe_ratio(mn_to_bn(total_loan), mn_to_bn(deposit_total)),
            "liquidity_ratio": safe_ratio(liquid_assets_mn, deposit_total),
            "source_file": workbook_path.name,
        }
        rows.append(record)
    return pd.DataFrame(rows)


def select_reference_periods(periods: list[MonthlyFile]) -> dict[str, MonthlyFile | None]:
    ordered = sorted(periods, key=lambda p: p.order, reverse=True)
    current = ordered[0] if ordered else None
    if current is None:
        return {"current": None, "last_month": None, "last_year_end": None, "last_year_corresponding": None}
    by_order = {p.order: p for p in periods}
    last_month = by_order.get(current.order - 1)
    last_year_corresponding = by_order.get(current.order - 12)
    last_year_end = None
    for p in sorted(periods, key=lambda x: x.order, reverse=True):
        if p.order < current.order and p.bs_month == 3:
            last_year_end = p
            break
    return {
        "current": current,
        "last_month": last_month,
        "last_year_end": last_year_end,
        "last_year_corresponding": last_year_corresponding,
    }


def rank_within_dev(df: pd.DataFrame, period_key: str, metric: str, ascending: bool = False) -> dict[str, int]:
    sub = df[(df["period_key"] == period_key) & (df["sector"].str.upper() == "DEVELOPMENT BANK")].copy()
    sub = sub.dropna(subset=[metric])
    if sub.empty:
        return {}
    sub["rank"] = sub[metric].rank(ascending=ascending, method="min").astype(int)
    return dict(zip(sub["bfi_code"], sub["rank"]))


def period_value(df: pd.DataFrame, period: MonthlyFile | None, bank: str, metric: str):
    if period is None:
        return None
    sub = df[(df["period_key"] == period.period_key) & (df["bfi_code"].str.upper() == bank.upper())]
    if sub.empty or metric not in sub.columns:
        return None
    value = sub.iloc[0][metric]
    if pd.isna(value):
        return None
    return value


def change(current, reference):
    if current is None or reference is None or pd.isna(current) or pd.isna(reference):
        return None
    return current - reference


def build_block_rows(df: pd.DataFrame, periods: dict, banks: list[str], metrics: list[tuple[str, str]], rank_metric: str) -> list[list]:
    current = periods["current"]
    last_month = periods["last_month"]
    last_year_end = periods["last_year_end"]
    last_year_corresponding = periods["last_year_corresponding"]
    ranks = rank_within_dev(df, current.period_key, rank_metric) if current else {}
    rows = []
    for bank in banks:
        row = [bank, ranks.get(bank)]
        current_values = [period_value(df, current, bank, metric) for metric, _ in metrics]
        last_month_values = [period_value(df, last_month, bank, metric) for metric, _ in metrics]
        year_end_values = [period_value(df, last_year_end, bank, metric) for metric, _ in metrics]
        last_year_values = [period_value(df, last_year_corresponding, bank, metric) for metric, _ in metrics]
        mom = [change(c, p) for c, p in zip(current_values, last_month_values)]
        ytd = [change(c, p) for c, p in zip(current_values, year_end_values)]
        yoy = [change(c, p) for c, p in zip(current_values, last_year_values)]
        row.extend(current_values)
        row.extend(last_month_values)
        row.extend(year_end_values)
        row.extend(mom)
        row.extend(ytd)
        row.extend(last_year_values)
        row.extend(yoy)
        rows.append(row)
    return rows


def build_ratio_rows(df: pd.DataFrame, periods: dict, banks: list[str], metric: str, rank_metric: str) -> list[list]:
    current = periods["current"]
    last_month = periods["last_month"]
    last_year_end = periods["last_year_end"]
    last_year_corresponding = periods["last_year_corresponding"]
    ranks = rank_within_dev(df, current.period_key, rank_metric, ascending=False) if current else {}
    rows = []
    for bank in banks:
        c = period_value(df, current, bank, metric)
        lm = period_value(df, last_month, bank, metric)
        ye = period_value(df, last_year_end, bank, metric)
        ly = period_value(df, last_year_corresponding, bank, metric)
        rows.append([bank, ranks.get(bank), c, lm, ye, ly, change(c, ye)])
    return rows


def set_section_header(ws, row, label, groups, formats):
    ws.merge_range(row, 0, row, 1, label, formats["section"])
    col = 2
    for title, span, fmt_key in groups:
        ws.merge_range(row, col, row, col + span - 1, title, formats[fmt_key])
        col += span


def write_table(ws, start_row, section_label, unit_label, metrics, data_rows, formats, is_ratio=False):
    groups = [
        ("This Month", len(metrics), "orange"),
        ("Last Month", len(metrics), "blue"),
        ("Last Year End", len(metrics), "green"),
        ("MoM Change (Rs.)", len(metrics), "orange"),
        ("YTD Change (Rs.)", len(metrics), "blue"),
        ("Last Year Corresponding", len(metrics), "blue"),
        ("YoY Change", len(metrics), "blue"),
    ]
    if is_ratio:
        ws.write(start_row, 0, section_label, formats["subsection"])
        start_row += 1
        headers = [unit_label, "Rank", "Current Month", "Last Month", "Ashadh", "Corresponding Year", "Increment % this year"]
        ws.write_row(start_row, 0, headers, formats["header_blue"])
        for r_offset, row_values in enumerate(data_rows, 1):
            ws.write_row(start_row + r_offset, 0, row_values[:2], formats["body"])
            ws.write_row(start_row + r_offset, 2, row_values[2:], formats["pct"])
        return start_row + len(data_rows) + 3
    set_section_header(ws, start_row, section_label, groups, formats)
    headers = [unit_label, "Rank"] + [display for _group in range(7) for _, display in metrics]
    ws.write_row(start_row + 1, 0, headers, formats["header_blue"])
    for r_offset, row_values in enumerate(data_rows, 2):
        ws.write_row(start_row + r_offset, 0, row_values[:2], formats["body"])
        for c, value in enumerate(row_values[2:], 2):
            ws.write_number(start_row + r_offset, c, value, formats["number"]) if isinstance(value, (int, float)) and not pd.isna(value) else ws.write(start_row + r_offset, c, "-", formats["body"])
    return start_row + len(data_rows) + 4


def write_industry_report(df: pd.DataFrame, manifest_df: pd.DataFrame, output_path: Path, target_bank: str):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    periods_available = [MonthlyFile(row.period_text, int(row.bs_year), int(row.bs_month), "", "") for row in manifest_df.itertuples(index=False)]
    periods = select_reference_periods(periods_available)
    current = periods["current"]
    if current is None:
        raise RuntimeError("No current period available for report.")
    dev_df = df[df["sector"].str.upper() == "DEVELOPMENT BANK"].copy()
    banks = [b for b in DEV_BANK_ORDER if b.upper() in set(dev_df["bfi_code"].str.upper())]
    if not banks:
        banks = dev_df[dev_df["period_key"] == current.period_key].sort_values("total_deposit", ascending=False)["bfi_code"].tolist()
    workbook = None
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book
        formats = {
            "title": workbook.add_format({"bold": True, "font_size": 11}),
            "section": workbook.add_format({"bold": True, "bg_color": "#F4B183", "border": 1, "align": "center", "valign": "vcenter"}),
            "subsection": workbook.add_format({"bold": True, "font_size": 10}),
            "orange": workbook.add_format({"bold": True, "bg_color": "#F4B183", "border": 1, "align": "center"}),
            "blue": workbook.add_format({"bold": True, "bg_color": "#0070C0", "font_color": "#FFFFFF", "border": 1, "align": "center", "valign": "vcenter"}),
            "green": workbook.add_format({"bold": True, "bg_color": "#70AD47", "font_color": "#FFFFFF", "border": 1, "align": "center"}),
            "header_blue": workbook.add_format({"bold": True, "bg_color": "#0070C0", "font_color": "#FFFFFF", "border": 1, "align": "center", "valign": "vcenter", "text_wrap": True}),
            "body": workbook.add_format({"border": 1, "font_size": 8}),
            "number": workbook.add_format({"border": 1, "font_size": 8, "num_format": "#,##0.00;(#,##0.00);-"}),
            "pct": workbook.add_format({"border": 1, "font_size": 8, "num_format": "0.00%;(0.00%);-"}),
        }
        ws = workbook.add_worksheet("Industry_Analysis")
        writer.sheets["Industry_Analysis"] = ws
        ws.write(0, 0, "Kamana Sewa Bikas Bank Ltd.", formats["title"])
        ws.write(1, 0, f"FY {current.bs_year}-{str(current.bs_year + 1)[-2:]}", formats["title"])
        ws.write(3, 0, f"Industry Analysis {current.period_text}", formats["title"])
        deposit_metrics = [
            ("total_deposit", "Total Deposit"),
            ("current_deposit", "Current"),
            ("saving_deposit", "Savings"),
            ("fixed_deposit", "Fixed"),
            ("call_deposit", "Call Deposits"),
            ("deposit_others", "Others"),
        ]
        loan_metrics = [
            ("total_loan", "Total loan"),
            ("loan_to_customers", "Loan to customers"),
            ("loan_to_bfis", "Loan to BFIs"),
            ("nba_mn", "NBA"),
            ("investment_govt_sec_mn", "Investment in Govt. Sec"),
            ("investment_shares_other_mn", "Investment in Shares and Other"),
        ]
        pl_metrics = [
            ("nii_mn", "NII"),
            ("commission_income_mn", "Commission and Discount Income"),
            ("llp_exp_mn", "LLP Exp"),
            ("hr_exp_excl_bonus_mn", "HR Exp (excl. Bonus)"),
            ("opex_mn", "Opex"),
            ("loan_writeoff_mn", "Loan W/f"),
        ]
        bs_pl_metrics = [
            ("net_profit_mn", "Net Profit"),
            ("other_operating_income_mn", "Other Operating Income"),
            ("capital", "Capital"),
            ("general_reserve", "General Reserve"),
            ("llp_fund", "LLP fund"),
            ("debenture", "Debenture"),
        ]
        row = 5
        row = write_table(ws, row, "Bank's name", "Deposit (Rs. in Bn)", deposit_metrics, build_block_rows(df, periods, banks, deposit_metrics, "total_deposit"), formats)
        row = write_table(ws, row, "Others (Loan and other)", "Others (Loan and other)", loan_metrics, build_block_rows(df, periods, banks, loan_metrics, "total_loan"), formats)
        row = write_table(ws, row, "PL Items (Rs. in Mn)", "PL Items (Rs. in Mn)", pl_metrics, build_block_rows(df, periods, banks, pl_metrics, "nii_mn"), formats)
        row = write_table(ws, row, "PL Items / Balance sheet items", "PL Items (Rs. in Mn) / Balance sheet items (Rs. in Bn)", bs_pl_metrics, build_block_rows(df, periods, banks, bs_pl_metrics, "net_profit_mn"), formats)
        savings_rows = build_ratio_rows(df, periods, banks, "savings_deposit_ratio", "savings_deposit_ratio")
        row = write_table(ws, row, "Ratios (Savings Deposit)", "Deposit (Rs. in Bn)", [("savings_deposit_ratio", "Current Month")], savings_rows, formats, is_ratio=True)
        ldr_rows = build_ratio_rows(df, periods, banks, "loan_to_deposit_ratio", "loan_to_deposit_ratio")
        row = write_table(ws, row, "Ratios (Loan to Deposit Ratio)", "Deposit (Rs. in Bn)", [("loan_to_deposit_ratio", "Current Month")], ldr_rows, formats, is_ratio=True)
        ws.set_zoom(70)
        ws.freeze_panes(5, 2)
        ws.set_column(0, 0, 14)
        ws.set_column(1, 1, 8)
        ws.set_column(2, 60, 12)
        ws.repeat_rows(0, 4)
        df.to_excel(writer, sheet_name="Extracted_C8_C9", index=False)
        manifest_df.to_excel(writer, sheet_name="Manifest", index=False)
        source_df = pd.DataFrame([
            {"Item": "NRB Monthly Statistics URL", "Value": SOURCE_URL},
            {"Item": "Govt. Sec source override", "Value": "investment_govt_sec_mn is read from C8 row: SHARE & OTHER INVESTMENT"},
            {"Item": "Sheets read", "Value": "C8 and C9 only"},
        ])
        source_df.to_excel(writer, sheet_name="Source_Map", index=False)


def run_pipeline(args):
    repo_root = Path(args.repo_root).resolve()
    raw_dir = repo_root / "data" / "raw"
    processed_dir = repo_root / "data" / "processed"
    reports_dir = repo_root / "reports"
    state_dir = repo_root / "data" / "state"
    processed_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    mapping = load_mapping(repo_root / args.mapping)
    if args.local_xlsx:
        path = Path(args.local_xlsx).resolve()
        period = MonthlyFile(args.local_period_text, args.local_bs_year, args.local_bs_month, args.local_label, "local")
        periods = [period]
        manifest_records = [{
            "period_key": period.period_key,
            "period_text": period.period_text,
            "bs_year": period.bs_year,
            "bs_month": period.bs_month,
            "fiscal_year": fiscal_fields(period.bs_year, period.bs_month)["fiscal_year"],
            "fiscal_month": fiscal_fields(period.bs_year, period.bs_month)["fiscal_month"],
            "fiscal_quarter": fiscal_fields(period.bs_year, period.bs_month)["fiscal_quarter"],
            "xlsx_url": "local",
            "local_file": str(path),
            "downloaded_this_run": False,
        }]
        extracted = extract_monthly_c8_c9(path, period, mapping)
        all_months_df = extracted
        downloaded_any = False
    else:
        periods = parse_monthly_files(args.source_url, max_pages=args.max_pages, months=args.months)
        if not periods:
            raise RuntimeError("No monthly XLSX links found on the NRB page.")
        manifest_records = []
        downloaded_any = False
        all_extracted = []
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
            extracted = extract_monthly_c8_c9(raw_path, period, mapping)
            if not extracted.empty:
                all_extracted.append(extracted)
        if not all_extracted:
            raise RuntimeError("No C8/C9 bank-wise data was extracted from downloaded files.")
        all_months_df = pd.concat(all_extracted, ignore_index=True)
    manifest_df = pd.DataFrame(manifest_records).sort_values("period_key")
    manifest_df.to_csv(processed_dir / "nrb_monthly_manifest.csv", index=False)
    all_months_df.to_csv(processed_dir / "nrb_c8_c9_extracted.csv", index=False)
    latest_period = max(periods, key=lambda p: p.order).period_key
    output_path = reports_dir / f"Development_Bank_Industry_Analysis_{latest_period}.xlsx"
    previous_state_path = state_dir / "latest.json"
    previous_state = {}
    if previous_state_path.exists():
        try:
            previous_state = json.loads(previous_state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            previous_state = {}
    if not args.force and not args.local_xlsx and not downloaded_any and previous_state.get("latest_period") == latest_period and output_path.exists():
        print(json.dumps({"status": "no_new_data", "latest_period": latest_period, "report": str(output_path.relative_to(repo_root))}, indent=2))
        return
    write_industry_report(all_months_df, manifest_df, output_path, args.target_bank)
    state = {
        "latest_period": latest_period,
        "downloaded_any": downloaded_any,
        "report": str(output_path.relative_to(repo_root)),
        "raw_file_count": len(manifest_records),
        "investment_govt_sec_source": "C8 row SHARE & OTHER INVESTMENT",
    }
    previous_state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    print(json.dumps(state, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-url", default=SOURCE_URL)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--mapping", default="config/bfi_mapping.csv")
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--max-pages", type=int, default=8)
    parser.add_argument("--target-bank", default="Kamana")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--local-xlsx", default=None)
    parser.add_argument("--local-bs-year", type=int, default=2082)
    parser.add_argument("--local-bs-month", type=int, default=11)
    parser.add_argument("--local-label", default="Mid Mar, 2026")
    parser.add_argument("--local-period-text", default="2082-11(Mid Mar, 2026)")
    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
