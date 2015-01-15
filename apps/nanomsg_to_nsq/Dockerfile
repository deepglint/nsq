#Set the base image to use to Ubuntu
FROM 192.168.1.72:5000/ubuntu:trusty

#Set the file maintainer (your name- the file's author)

MAINTAINER Yan Huang


#Add files to the image

RUN mkdir -p root/uploader

ADD uploader root/uploader/uploader

RUN chmod +x root/uploader/uploader

WORKDIR root/uploader
