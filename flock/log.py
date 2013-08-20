import logging
from logging import handlers
import datetime as dt
from .fancyemail import send_email

class BufferedSMTPHandler(handlers.SMTPHandler):
	
	#mailhost, fromaddr, toaddrs, subject, credentials=None, secure=None
	def __init__(self,*args,**kwargs):
		self.buffer = list()
		self.flushes = 1
		self.last_flush = dt.datetime.now()
		handlers.SMTPHandler.__init__(self,*args,**kwargs)


	def emit(self,item):

		self.buffer.append(item)

		if (dt.datetime.now() - self.last_flush) > dt.timedelta(minutes=60):
			self.flush()

	def flush(self,final=False):

		subject = self.subject + "[part {0}]".format(self.flushes)
		
		if final:
			subject += " (Final)"
		else:
			subject += " (Ongoing)"

		message = '\r\n'.join((self.format(item) for item in self.buffer))

		send_email(self.mailhost,subject,self.toaddrs,self.fromaddr,message)

		self.flushes += 1
		self.last_flush = dt.datetime.now()
		self.buffer = list()

	def close(self):
		self.flush(final=True)
		super(logging.handlers.SMTPHandler,self).close()


def get_logger(name,log_filename,smtp_args=None,smtp_kwargs=None):

	# Default handler logs to a file
	logging.basicConfig(level=logging.DEBUG,
	                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
	                    datefmt='%m-%d %H:%M',
	                    filename=log_filename,
	                    filemode='a')


	simple_formatter = logging.Formatter('[%(name)s] %(levelname)s, %(asctime)s, %(message)s')

	# Second handler writes to stderr
	console = logging.StreamHandler()
	console.setLevel(logging.DEBUG)
	console.setFormatter(simple_formatter)
	logger = logging.getLogger(name)
	logger.addHandler(console)

	# Third handler ships log messages over smtp connection to given distribution list
	if smtp_args:
		if smtp_kwargs == None:
			smtp_kwargs = dict()
		email_handler = BufferedSMTPHandler(*smtp_args,**smtp_kwargs)
		logger.addHandler(email_handler)
		console.setFormatter(simple_formatter)

	return logger
