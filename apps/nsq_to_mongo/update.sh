#!/bin/bash

if [ $# -ne 1 ]; then
	echo "./update.sh new_version[eg: 0.9.2]"
	exit
fi

TAG=$1

./build4linux.sh

docker build -t 192.168.2.103:5000/nsq2mongo:$TAG .

docker push 192.168.2.103:5000/nsq2mongo:$TAG

#docker tag 192.168.2.103:5000/nsq2mongo:$TAG 115.28.62.181:5000/nsq2mongo:$TAG

#docker push 115.28.62.181:5000/nsq2mongo:$TAG
