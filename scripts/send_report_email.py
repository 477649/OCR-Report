import os
import smtplib
from email.message import EmailMessage
from pathlib import Path


def main() -> None:
    from_email = os.getenv("FROM_EMAIL")
    to_email = os.getenv("TO_EMAIL")
    cc_email = os.getenv("CC_EMAIL", "")
    app_password = os.getenv("APP_PASSWORD")
    report_path = os.getenv("REPORT_PATH")
    latest_title = os.getenv("LATEST_TITLE", "NRB Monthly BFI Report")

    if not from_email or not to_email or not app_password or not report_path:
        raise RuntimeError("Missing FROM_EMAIL, TO_EMAIL, APP_PASSWORD, or REPORT_PATH.")

    report = Path(report_path)
    if not report.exists():
        raise RuntimeError(f"Report file not found: {report}")

    to_list = [x.strip() for x in to_email.split(",") if x.strip()]
    cc_list = [x.strip() for x in cc_email.split(",") if x.strip()]
    recipients = to_list + cc_list

    msg = EmailMessage()
    msg["Subject"] = f"NRB Monthly BFI Report Generated - {latest_title}"
    msg["From"] = from_email
    msg["To"] = ", ".join(to_list)

    if cc_list:
        msg["Cc"] = ", ".join(cc_list)

    msg.set_content(
        f"""Dear Team,

A new NRB monthly BFI file was detected.

Latest Entry:
{latest_title}

The latest Industry Analysis report has been generated and attached.

This is an auto-generated email.
"""
    )

    with open(report, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=report.name,
        )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(from_email, app_password)
        server.send_message(msg, from_addr=from_email, to_addrs=recipients)

    print(f"Email sent with attachment: {report}")


if __name__ == "__main__":
    main()
