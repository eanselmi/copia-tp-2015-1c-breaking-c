#!/bin/bash

apt-get install mongodb-clients mongodb-server -y
cd /root
wget https://github.com/mongodb/mongo-c-driver/releases/download/1.1.4/mongo-c-driver-1.1.4.tar.gz
tar -xzf mongo-c-driver-1.1.4.tar.gz
cd mongo-c-driver-1.1.4/
./configure
make
make install
echo "export LD_LIBRARY_PATH=/usr/local/lib" >> /etc/profile
