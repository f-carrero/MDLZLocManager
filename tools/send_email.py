import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def send_email_with_attachment(
    smtp_host,
    smtp_port,
    smtp_username,
    smtp_password,
    sender,
    recipient,
    subject,
    body,
    attachment_data,
    attachment_filename,
):
    """
    Send an email with a CSV attachment via SMTP (TLS).

    :param smtp_host: SMTP server hostname
    :param smtp_port: SMTP server port (typically 587)
    :param smtp_username: SMTP auth username
    :param smtp_password: SMTP auth password
    :param sender: From address
    :param recipient: To address
    :param subject: Email subject line
    :param body: Plain-text email body
    :param attachment_data: Raw string/bytes of the attachment content
    :param attachment_filename: Filename for the attachment
    """
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    part = MIMEBase("application", "octet-stream")
    if isinstance(attachment_data, str):
        attachment_data = attachment_data.encode("utf-8")
    part.set_payload(attachment_data)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename={attachment_filename}")
    msg.attach(part)

    with smtplib.SMTP(smtp_host, int(smtp_port)) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender, recipient, msg.as_string())
