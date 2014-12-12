#Set the base image to use to Ubuntu
FROM 192.168.1.72:5000/ubuntu:trusty

#Set the file maintainer (your name- the file's author)

MAINTAINER Yan Huang


#Add files to the image

RUN mkdir -p root/livetest

ADD nsqd root/livetest/livetest

RUN chmod +x root/livetest/livetest

WORKDIR root/livetest
