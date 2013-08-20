echo "Initializing install with USER" $USER "in directory" `pwd`



#Glob & source private variables
cp ./private ~/.flockrc
cat schemas/*/private >> ~/.flockrc
. ~/.flockrc

#These should exist
mkdir -p $FLOCK_VIRTUALENV_DIR $FLOCK_DATA_DIR $FLOCK_LOG_DIR

#Virtualenv
if [[ ! -e $FLOCK_VIRTUALENV_DIR/bin/activate ]]; then
	virtualenv --no-site-packages $FLOCK_VIRTUALENV_DIR
fi

. $FLOCK_VIRTUALENV_DIR/bin/activate
pip install -r requirements.txt

#Populate crontab
cat schemas/*/crontab >> ~/crontab
crontab ./crontab