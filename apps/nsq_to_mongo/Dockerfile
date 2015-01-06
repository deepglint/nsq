FROM 115.28.62.181:5000/ubuntu:trusty

MAINTAINER Yan Huang

RUN mkdir -p /nsq2mongo

ADD nsq2mongo.linux /nsq2mongo/nsq2mongo.linux

RUN chmod +x /nsq2mongo/nsq2mongo.linux

WORKDIR /nsq2mongo

CMD ./nsq2mongo.linux