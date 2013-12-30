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

#Download and install Python 2.7.6
mkdir build
cd build
wget http://www.python.org/ftp/python/2.7.6/Python-2.7.6.tar.xz
tar xvf Python-2.7.6.tar.xz
cd Python-2.7.6
./configure --prefix=/usr/local
make && sudo make altinstall
cd ..
#Setuptools
curl -kO https://pypi.python.org/packages/source/s/setuptools/setuptools-1.1.6.tar.gz
tar xzf setuptools-1.1.6.tar.gz
cd setuptools-1.1.6
sudo /usr/local/bin/python2.7 setup.py install
cd ..
# Set up virtualenv
sudo /usr/local/bin/easy_install-2.7 virtualenv
/usr/local/bin/virtualenv-2.7 --distribute .py/sys27
. .py/sys27/bin/activate


