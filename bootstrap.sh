cd build

#download and install rpm that adds a yum repo hosted on postgres.org
curl -kO http://yum.postgresql.org/9.3/redhat/rhel-6-x86_64/pgdg-centos93-9.3-1.noarch.rpm
sudo rpm -ivh pgdg-centos93-9.3-1.noarch.rpm
#install postgres 9.3
sudo yum install posgresql93 postgresql93-server -y

#create the database in this directory
sudo mkdir /var/lib/pgsql/9.3
sudo chown -R postgres:postgres /var/lib/pgsql/9.3
sudo service postgresql-9.3 initdb -D /var/lib/pgsql/9.3/data

#start postgres
sudo service postgresql-9.3 start

# add user vagrant as a superuser  (for development env)
sudo -u postgres psql -U postgres -d postgres -c "CREATE ROLE vagrant with SUPERUSER LOGIN  PASSWORD 'password';"

#start postgres to be sure it starts
sudo chkconfig postgresql-9.3 on

#install python setuptools
sudo yum install python-setuptools -y

#install postgres-dev because we need it for psycopg2
sudo yum install postgresql-devel -y

# following packages are required for python package lxml
sudo yum install libxml2-devel libxslt-devel -y

#Install development tools
sudo yum groupinstall "Development tools" -y
sudo yum install zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel -y
sudo yum install python-devel -y
#Download and install Python 2.7.6
wget http://www.python.org/ftp/python/2.7.6/Python-2.7.6.tar.xz
tar xvf Python-2.7.6.tar.xz
cd Python-2.7.6
./configure --prefix=/usr/local
make && sudo make altinstall
cd ..
#Setuptools
curl -O https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py
/usr/local/bin/python2.7 ez_setup.py
/usr/local/bin/easy_install-2.7 pip
/usr/local/bin/pip2.7 install virtualenv


