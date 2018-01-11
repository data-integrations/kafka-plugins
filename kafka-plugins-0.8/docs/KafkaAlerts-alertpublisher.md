# Kafka Alert Publisher


Description
-----------
Kafka Alert Publisher that allows you to publish alerts to kafka as json objects.
The plugin internally uses kafka producer apis to publish alerts. 
The plugin allows to specify kafka topic to use for publishing and other additional
kafka producer properties. Please note that this plugin uses kafka 0.8.2 java apis 
so it may not be compatible with higher versions of kafka.


Configuration
-------------
**brokers:** List of Kafka brokers specified in host1:port1,host2:port2 form.

**topic:** The Kafka topic to write to. This topic should already exist in kafka.

**producerProperties** Specifies additional kafka producer properties like acks, client.id as key and value pair.

Example
-------
This example publishes alerts to already existing kafka topic alarm as json objects.
The kafka broker is running at localhost and port 9092. Additional kafka producer properties 
are like acks and client.id are specified as well.


    {
        "name": "Kafka",
        "type": "alertpublisher",
        "properties": {
            "brokers": "localhost:9092",
            "topic": "alarm",
            "producerProperties": "acks:2,client.id:myclient"
        }
    }