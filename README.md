Flock
=====

Flock is a framework for scripts that manage data using postgres.

It provides:

 - Common operations for dealing with postgres and getting data in
 - Decodes all your non unicode data
 - Logging & error handling
 - A place to register & call hgh level commands

The goal is for users to be able to describe data lifecycles using code that resembles a makefile. (Acheiving this is still a WIP)

Requirements:

- Python 2.6+ / pip / virtualenv
- Postgres 9.2 

To get started 
```sh 
git clone https://github.com/exafox/flock
cd flock
cp example_private private
. init.sh
python flock.py -h
```

There is an example in schemas/fdpc. To build it:
```sh 
cp schemas/fdps/example_private schemas/fdps/private # Take a moment to peek at / modify this file and make sure the referenced databases exist
. init.sh #Init compiles and sources your private settings
python flock.py fdps bootstrap
```



