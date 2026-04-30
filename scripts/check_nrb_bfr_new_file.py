import os
from pathlib import Path

import requests
from bs4 import BeautifulSoup

URL_BFR = "https://www.nrb.org.np/category/monthly-statistics/?department=bfr"
STATE_FILE = Path("data/state/last_seen_month.txt")


def get_latest_bfr_title() -> str:
    response = requests.get(URL_BFR, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    for a in soup.find_all("a"):
        text = a.get_text(strip=True)
        if text.startswith("208"):
            return text

    raise RuntimeError("No latest NRB monthly statistics item found.")


def set_github_output(name: str, value: str) -> None:
    output_file = os.getenv("GITHUB_OUTPUT")
    if output_file:
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")


def main() -> None:
    latest = get_latest_bfr_title()

    if STATE_FILE.exists():
        last_seen = STATE_FILE.read_text(encoding="utf-8").strip()
    else:
        last_seen = ""

    print("Latest found:", latest)
    print("Last seen:", repr(last_seen))

    if latest != last_seen:
        print("New NRB monthly file found.")
        set_github_output("new_file", "true")
        set_github_output("latest_title", latest)
    else:
        print("No new NRB monthly file found.")
        set_github_output("new_file", "false")
        set_github_output("latest_title", latest)


if __name__ == "__main__":
    main()
