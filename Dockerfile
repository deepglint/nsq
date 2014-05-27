# Set the base image to use to Ubuntu
FROM ubuntu:13.10

# Set the file maintainer (your name - the file's author)
MAINTAINER Weiran Yuan

# Mkdir for nsq data
RUN mkdir -p /data/nsq

# Add files to the image
RUN mkdir -p root/nsq
ADD build/apps/nsqd root/nsq/nsqd
ADD build/apps/nsqlookupd root/nsq/nsqlookupd
ADD build/nsqadmin root/nsq/nsqadmin

#
WORKDIR root/nsq

# Run Command
# sudo  docker run -d -p 4160:4160 -p 4161:4161 --name nsqlookupd $REPO/nsqlive  ./nsqlookupd
# sudo  docker run -d -p 4150:4150 -p 4151:4151 -v $HOME/data/nsq:/data/nsq --name nsqd $REPO/nsqlive ./nsqd --lookupd-tcp-address=$IP:4160 --data-path=/data
# sudo docker run -d -p 4171:4171 --name nsqadmin 115.28.62.181:5000/nsq:0.2.24 ./nsqadmin --lookupd-http-address=115.28.239.226:4161 -template-dir=/root/nsq/templates