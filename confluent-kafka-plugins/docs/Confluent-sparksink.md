# Confluent Streaming Sink


Description
-----------
This sink writes data to Confluent.
Sends message to specified Kafka topic per received record. It can also be
configured to partition events being written to kafka based on a configurable key. 
The sink can also be configured to operate in sync or async mode and apply different
compression types to events.
Can be used with self-managed Confluent Platform or Confluent Cloud. Supports Schema Registry.


Properties
----------
**Reference Name:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**Kafka Brokers:** List of Kafka brokers specified in host1:port1,host2:port2 form. (Macro-enabled)

**Kafka Topic:** The Kafka topic to read from. (Macro-enabled)

**Async:** Specifies whether an acknowledgment is required from broker that message was received. Default is No.

**Compression Type:** Compression type to be applied on message.

**Time Field:** Optional name of the field containing the read time of the message. 
If this is not set, message will be send with current timestamp. 
If set, this field must be present in the input schema and must be a long.

**Key Field:** Optional name of the field containing the message key.
If this is not set, message will be send without a key.
If set, this field must be present in the schema property and must be of type bytes.

**Partition Field:** Optional name of the field containing the partition the message should be written to.
If this is not set, default partition will be used for all messages.
If set, this field must be present in the schema property and must be an int.

**Message Format:** Optional format a structured record should be converted to.
 Required if used without Schema Registry.

**Additional Kafka Producer Properties:** Additional Kafka producer properties to set.

**Cluster API Key:** The Confluent API Key used for the source.

**Cluster API Secret:** The Confluent API Secret used for the source.

**Schema Registry URL:** The Schema Registry endpoint URL.

**Schema Registry API Key:** The Schema Registry API Key.

**Schema Registry API Secret:** The Schema Registry API Secret.

Example
-------
This example writes structured record to kafka topic 'alarm' in asynchronous manner 
using compression type 'gzip'. The written events will be written in csv format 
to kafka running at localhost. The Kafka partition will be decided based on the provided key 'ts'.
Additional properties like number of acknowledgements and client id can also be provided.

```json
{
    "name": "Confluent",
    "type": "batchsink",
    "properties": {
        "referenceName": "Kafka",
        "brokers": "host1.example.com:9092,host2.example.com:9092",
        "topic": "alarm",
        "async": "true",
        "compressionType": "gzip",
        "format": "CSV",
        "kafkaProperties": "acks:2,client.id:myclient",
        "key": "message",
        "clusterApiKey": "",
        "clusterApiSecret": ""
    }
}
```
