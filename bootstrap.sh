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
