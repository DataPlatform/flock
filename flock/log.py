import logging
from logging import handlers
import datetime as dt
from .fancyemail import send_email


class BufferedSMTPHandler(handlers.SMTPHandler):

    def __init__(self, *args, **kwargs):
        self.buffer = list()
        self.flushes = 1
        self.last_flush = dt.datetime.now()
        handlers.SMTPHandler.__init__(self, *args, **kwargs)

    def emit(self, item):

        self.buffer.append(item)

        if (dt.datetime.now() - self.last_flush) > dt.timedelta(minutes=60):
            self.flush(final=False)

    def flush(self, final=True):

        subject = self.subject + " part{0}: ".format(self.flushes)

        message = '\r\n'.join((str(self.format(item)).strip().replace('\n', '<br>\n')
                              for item in self.buffer))
        num_errors = message.count('ERROR')
        num_warnings = message.count('WARNING')

        if num_errors or num_warnings:
            subject += ' errors={0} warnings={1}'.format(str(num_errors),
                                                         str(num_warnings))

        if final:
            subject += " (final)"
        else:
            subject += " (ongoing)"

        # send_email('cbpromgt01',subject,self.toaddrs,self.fromaddr,message)
        send_email(self.mailhost, subject,
                   self.toaddrs, self.fromaddr, message)

        self.flushes += 1
        self.last_flush = dt.datetime.now()
        self.buffer = list()

    def close(self):
        # self.flush(final=True)
        super(logging.handlers.SMTPHandler, self).close()


def get_logger(name, log_filename, smtp_args=None, smtp_kwargs=None):

    # Default handler logs to a file
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=log_filename,
                        filemode='a')

    simple_formatter = logging.Formatter(
        '[%(name)s] %(levelname)s, %(asctime)s, %(message)s')
    html_formatter = logging.Formatter(
        '<strong>%(levelname)s</strong>, %(asctime)s, %(message)s<br>', datefmt='%m/%d %H:%M')

    # Second handler writes to stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(simple_formatter)
    logger = logging.getLogger(name)
    logger.addHandler(console)

    # Third handler ships log messages over smtp connection to given
    # distribution list
    if smtp_args:
        if smtp_kwargs == None:
            smtp_kwargs = dict()
        email_handler = BufferedSMTPHandler(*smtp_args, **smtp_kwargs)
        email_handler.setLevel(logging.INFO)
        email_handler.setFormatter(html_formatter)
        logger.addHandler(email_handler)
        console.setFormatter(simple_formatter)

    return logger
