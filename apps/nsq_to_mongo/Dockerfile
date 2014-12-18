#Set the base image to use to Ubuntu
FROM 192.168.1.72:5000/ubuntu:trusty

#Set the file maintainer (your name- the file's author)

MAINTAINER Yan Huang


#Add files to the image

RUN mkdir -p root/nsqtomongo

ADD nsq_to_mongo root/nsqtomongo/nsq_to_mongo

RUN chmod +x root/nsqtomongo/nsq_to_mongo

WORKDIR root/nsqtomongo
