#!/bin/bash
# author https://github.com/donhk
#
# Execute this script before start running the install scripts
# or the python install will fail and packt-beam will fail
#

# first let's make sure we have python3
sudo dnf install -y python3
# install virtual env
python3 -m pip install --user virtualenv
# install python 3.7.*
sudo dnf install -y python37
# create env with python3.7
python3.7 -m venv venv
# enable the env
. venv/bin/activate
# now the install will work
