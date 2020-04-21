# Kafka Streams
A repository to explore the functionalities of kafka and realtime streaming

## Getting Started
This repository written for those who wants to explore kafka consumer, producer and real time streaming with different modules.

### Installing

1. Setup Java Environment
* $ sudo add-apt-repository ppa:webupd8team/java
* $ sudo apt-get install oracle-java8-installer
* $ sudo apt-get install oracle-java8-set-default

2. Setup Kafka server

* $ wget http://www-us.apache.org/dist/kafka/2.4.0/kafka_2.13-2.4.0.tgz
* $ tar xzf kafka_2.13-2.4.0.tgz
* $ vim /etc/systemd/system/zookeeper.service\
 Paste the following--
```
[Unit]
 Description=Apache Zookeeper server
 Documentation=http://zookeeper.apache.org
 Requires=network.target remote-fs.target
 After=network.target remote-fs.target
 
 [Service]
 Type=simple
 ExecStart=/usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties
 ExecStop=/usr/local/kafka/bin/zookeeper-server-stop.sh
 Restart=on-abnormal
 
 [Install]
 WantedBy=multi-user.target
```
 
* $ vim /etc/systemd/system/kafka.service\
 Paste the following--
```
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=zookeeper.service

[Service]
Type=simple
Environment="JAVA_HOME=/usr/lib/jvm/java-8-oracle"
ExecStart=/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties
ExecStop=/usr/local/kafka/bin/kafka-server-stop.sh

[Install]
WantedBy=multi-user.target
```
 
* $ systemctl daemon-reload\
 
 To start the zookeeper
* $ sudo systemctl start zookeeper\

To check the zookeeper status
* $ sudo systemctl status zookeeper\

To start the kafka server
* $ sudo systemctl start kafka\

To check the kafka status
* $ sudo systemctl status kafka\

Send Messages to Kafka Producer
* $ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic mongo-News < news.json\




\
\
\
\
\
References: \
https://tecadmin.net/install-apache-kafka-ubuntu\
https://developer.mongodb.com/how-to/data-streaming-kafka-consumer\
http://ubuntuhandbook.org/index.php/2018/05/install-oracle-java-jdk-8-10-ubuntu-18-04/
