Transaction Tracking
===============

## Context

The Spark Streaming job that recovers transaction topologies from packets captured by tcpdump.

## Working with Eclipse

Go through the following process to generate ```.project``` and ```.classpath``` for this project

* Make sure you have [sbt already installed](https://github.rtp.raleigh.ibm.com/cleancloudsight/cloudsight/wikis/work-with-spark#setup-spark)
* Include sbt Eclipse plugin by following 
   * For Linux user
    
    ```
    vi /root/.sbt/0.13/plugins/plugins.sbt
    ```
    Include the following content
    
    ```
    addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.5.0")

    addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")
    ```
   * For Windows user
    Open

    ```
    C:\Users\IBM_ADMIN\.sbt\0.13\plugins

    ```
    Include the following content    
    ```
    addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.4.0")

    addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")
    ```    
* Run the following commands under the project directory that contains ```build.sbt```. Once finished, you will be able to find the ```.project``` and ```.classpath``` that can be used to import into Eclipse

   ```
   sbt eclipse
   ```

## Build

   ```
   sbt clean assembly
   ```

## Submit (client mode, processing a local pcap file)
* Upload the assembled jar file to a client host, which collocates with the Spark cluster.
* Run the command:

   ```
   /opt/spark/bin/spark-submit \
     --class spark.pcap.run.PacketPathDiscover \
     --total-executor-cores 14 \
     --executor-memory 4G \
     --conf spark.streaming.blockInterval=400 \
     --master spark://9.12.248.184:7077 \
     TransactionTrackingbyPacket-assembly-1.0.0.jar 1 /root/data/0.4M.pcap
   ```
, in which "1" is the data source mode (file in this case), "/root/data/0.4M.pcap" is the path to the file.

## Submit (client mode, processing data from Kafka)