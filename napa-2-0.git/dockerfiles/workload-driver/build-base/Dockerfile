FROM ubuntu:14.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install \
    bundler \
    curl \
    dnsutils \
    openssh-server \
    supervisor \
    wget &&\
    apt-get clean &&\
    rm -Rf /var/cache/*

RUN mkdir -p /var/run/sshd
ADD sshd.conf /etc/supervisor/conf.d/sshd.conf

#For debug only, easy to login
ADD id_rsa.pub /root/.ssh/id_rsa.pub
RUN cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
RUN sed -i 's/session \+required \+pam_loginuid\.so/session    optional     pam_loginuid.so/' /etc/pam.d/sshd
#Debug end

RUN apt-get update && \
    apt-get install -y openjdk-7-jdk && \
    rm -rf /var/lib/apt/lists/* 

ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

CMD ["/usr/bin/supervisord", "-n"]


