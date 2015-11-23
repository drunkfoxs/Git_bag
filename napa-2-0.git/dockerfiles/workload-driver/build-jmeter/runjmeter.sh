#!/bin/bash
echo "starting jmeter"
sleep 10
/opt/apache-jmeter-2.9/bin/jmeter -n -t /root/AcmeAir.jmx -j /root/AcmeAir1.log -l /root/AcmeAir1.jtl 
