# Kafka Alert Publisher


Description
-----------
Kafka Alert Publisher that allows you to publish alerts to kafka as json objects.
The plugin internally uses kafka producer apis to publish alerts. 
The plugin allows to specify kafka topic to use for publishing and other additional
kafka producer properties. This plugin uses kafka 2.6 java apis.


Configuration
-------------
**brokers:** List of Kafka brokers specified in host1:port1,host2:port2 form.

**topic:** The Kafka topic to write to. This topic should already exist in kafka.

**producerProperties** Specifies additional kafka producer properties like acks, client.id as key and value pair.

**Kerberos Principal** The kerberos principal used for the source when kerberos security is enabled for kafka.

**Keytab Location** The keytab location for the kerberos principal when kerberos security is enabled for kafka.

Example
-------
This example publishes alerts to already existing kafka topic alarm as json objects.
The kafka broker is running at localhost and port 9092. Additional kafka producer properties 
are like acks and client.id are specified as well.

```json
{
    "name": "Kafka",
    "type": "alertpublisher",
    "properties": {
        "brokers": "localhost:9092",
        "topic": "alarm",
        "producerProperties": "acks:2,client.id:myclient"
    }
}
```
