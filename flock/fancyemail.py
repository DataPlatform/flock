import smtplib
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText

def send_email(smtp_host,subject,to,sender,body,attachments=None):

    assert type(to) == list


    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(to)
    msg.attach(MIMEText(body, 'html'))

    if attachments:
        for file in attachments:
            zf = open(file, 'rb')
            mime = MIMEBase('application', 'octet-stream')
            mime.set_payload(zf.read())
            encoders.encode_base64(msg)
            zf.close()
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % file)
            msg.attach(mime)

    s = smtplib.SMTP(smtp_host)
    s.sendmail(me, family, msg.as_string())
    s.quit()
