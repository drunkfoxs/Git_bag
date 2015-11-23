FROM build-base

MAINTAINER zhaoxin

RUN cd /opt &&\
    wget https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-2.9.tgz &&\
    tar xvf apache-jmeter-2.9.tgz &&\
    rm apache-jmeter-2.9.tgz
RUN cd /opt/apache-jmeter-2.9/lib/ext/ &&\
    wget https://json-simple.googlecode.com/files/json_simple-1.1.jar

ADD AcmeAir.jmx /root/AcmeAir.jmx
ADD Airports.csv /root/Airports.csv
ADD Airports2.csv /root/Airports2.csv
ADD runjmeter.sh /root/runjmeter.sh
ADD acmeair-driver-0.1.0-SNAPSHOT.jar /opt/apache-jmeter-2.9/lib/ext/acmeair-driver-0.1.0-SNAPSHOT.jar

CMD ["sh", "/root/runjmeter.sh"]
