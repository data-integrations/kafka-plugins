# Kafka Sink


Description
-----------
Kafka sink that allows you to write events into CSV or JSON to kafka.
Plugin has the capability to push the data to a Kafka topic. It can also be
configured to partition events being written to kafka based on a configurable key. 
The sink can also be configured to operate in sync or async mode and apply different
compression types to events. This plugin uses kafka 0.8.2 java apis.


Configuration
-------------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**brokers:** List of Kafka brokers specified in host1:port1,host2:port2 form.

**topic:** The Kafka topic to write to.

**async:** Specifies whether writing the events to broker is *Asynchronous* or *Synchronous*.

**compressionType** Compression type to be applied on message. It can be none, gzip or snappy. Default value is none

**format:** Specifies the format of the event published to Kafka. It can be csv or json. Defualt value is csv.

**kafkaProperties** Specifies additional kafka producer properties like acks, client.id as key and value pair.

**key:** Specifies the input field that should be used as the key for the event published into Kafka. 
It will use String partitioner to determine kafka event should go to which partition. Key field should be of type string.

Example
-------
This example writes structured record to kafka topic 'alarm' in asynchronous manner 
using compression type 'gzip'. The written events will be written in csv format 
to kafka running at localhost. The Kafka partition will be decided based on the provided key 'ts'.
Additional properties like number of acknowledgements and client id can also be provided.

```json
{
    "name": "Kafka",
    "type": "batchsink",
    "properties": {
        "referenceName": "Kafka",
        "brokers": "localhost:9092",
        "topic": "alarm",
        "async": "FALSE",
        "compressionType": "gzip",
        "format": "CSV",
        "kafkaProperties": "acks:2,client.id:myclient",
        "key": "message"
    }
}
```
