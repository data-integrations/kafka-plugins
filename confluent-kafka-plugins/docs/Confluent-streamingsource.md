# Confluent Streaming Source


Description
-----------
This source reads data from Confluent.
Emits a record per message from specified Kafka topic. 
Can be used with self-managed Confluent Platform or Confluent Cloud. Supports Schema Registry.

Can be configured to parse values from source in following ways:
1. User-defined format. Use **Message Format** field to choose any format supported by CDAP.
1. Schema Registry. Requires credentials for Schema Registry to be specified. 
Uses Avro schemas to deserialize Kafka messages. Use **Get Schema** button to fetch key and value schemas from registry.
1. Binary format. Used in case if no message format or Schema Registry credentials were provided.


Properties
----------
**Reference Name:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**Kafka Brokers:** List of Kafka brokers specified in host1:port1,host2:port2 form. (Macro-enabled)

**Kafka Topic:** The Kafka topic to read from. (Macro-enabled)

**Topic Partitions:** List of topic partitions to read from. If not specified, all partitions will be read. (Macro-enabled)

**Default Initial Offset:** The default initial offset for all topic partitions.
An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1.
Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read.
If you wish to set different initial offsets for different partitions, use the initialPartitionOffsets property. (Macro-enabled)

**Initial Partition Offsets:** The initial offset for each topic partition. If this is not specified,
all partitions will use the same initial offset, which is determined by the defaultInitialOffset property.
Any partitions specified in the partitions property, but not in this property will use the defaultInitialOffset.
An offset of -2 means the smallest offset. An offset of -1 means the latest offset.
Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. (Macro-enabled)

**Time Field:** Optional name of the field containing the read time of the batch.
If this is not set, no time field will be added to output records.
If set, this field must be present in the schema property and must be a long.

**Key Field:** Optional name of the field containing the message key.
If this is not set, no key field will be added to output records.
If set, this field must be present in the schema property and must be bytes.

**Partition Field:** Optional name of the field containing the partition the message was read from.
If this is not set, no partition field will be added to output records.
If set, this field must be present in the schema property and must be an int.

**Offset Field:** Optional name of the field containing the partition offset the message was read from.
If this is not set, no offset field will be added to output records.
If set, this field must be present in the schema property and must be a long.

**Message Format:** Optional format of the Kafka event message. Any format supported by CDAP is supported.
For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values.
If no format is given, Kafka message payloads will be treated as bytes.

**Max Rate Per Partition:** Maximum number of records to read per second per partition. Defaults to 1000.

**Additional Kafka Consumer Properties:** Additional Kafka consumer properties to set.

**Cluster API Key:** The Confluent API Key used for the source.

**Cluster API Secret:** The Confluent API Secret used for the source.

**Schema Registry URL:** The Schema Registry endpoint URL.

**Schema Registry API Key:** The Schema Registry API Key.

**Schema Registry API Secret:** The Schema Registry API Secret.

**Value Field:** The name of the field containing the message value. Required to fetch schema from Schema Registry.

**Schema:** Output schema of the source. If you would like the output records to contain a field with the
Kafka message key, the schema must include a field of type bytes/nullable bytes or string/nullable string, and you must 
set the **Key Field** property to that field's name. Similarly, if you would like the output records to contain a field
with the timestamp of when the record was read, the schema must include a field of type long or nullable long, and you
must set the **Time Field** property to that field's name. Any field that is not the **Time Field** or **Key Field**
will be used in conjunction with the format to parse Kafka message payloads. If used with Schema Registry then should
be fetched using **Get Schema** button.

Example
-------
***Example 1:*** Read from the 'purchases' topic of a Kafka instance running
on brokers host1.example.com:9092 and host2.example.com:9092. The source will add
a time field named 'readTime' that contains a timestamp corresponding to the micro
batch when the record was read. It will also contain a field named 'key' which will have
the message key in it. It parses the Kafka messages using the 'csv' format
with 'user', 'item', 'count', and 'price' as the message schema.

```json
{
    "name": "Confluent",
    "type": "streamingsource",
    "properties": {
        "topics": "purchases",
        "brokers": "host1.example.com:9092,host2.example.com:9092",
        "format": "csv",
        "timeField": "readTime",
        "keyField": "key",
        "clusterApiKey": "",
        "clusterApiSecret": "",
        "defaultInitialOffset": "-2",
        "schema": "{
            \"type\":\"record\",
            \"name\":\"purchase\",
            \"fields\":[
                {\"name\":\"readTime\",\"type\":\"long\"},
                {\"name\":\"key\",\"type\":\"bytes\"},
                {\"name\":\"user\",\"type\":\"string\"},
                {\"name\":\"item\",\"type\":\"string\"},
                {\"name\":\"count\",\"type\":\"int\"},
                {\"name\":\"price\",\"type\":\"double\"}
            ]
        }"
    }
}
```

For each Kafka message read, it will output a record with the schema:

| field name  | type             |
| ----------- | ---------------- |
| readTime    | long             |
| key         | bytes            |
| user        | string           |
| item        | string           |
| count       | int              |
| price       | double           |

Note that the readTime field is not derived from the Kafka message, but from the time that the
message was read.
